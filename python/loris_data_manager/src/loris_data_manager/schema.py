"""Definitions for the logical LORIS resource schema.

This module deliberately contains no SQLAlchemy models.  It describes the logical
objects projected from those models and the relationships between those objects.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Generic, Protocol, TypeAlias, TypeVar, cast

from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from loris_data_manager.resources import (
    DatabaseRowObject,
    LocalPathObject,
    ObjectRef,
    PhysicalObject,
    ResourceObject,
    ResourceRef,
)

ObjectT = TypeVar("ObjectT")
ValueT = TypeVar("ValueT")
FilterT = TypeVar("FilterT")
SourceObjectT = TypeVar("SourceObjectT", contravariant=True)
SourceValueT = TypeVar("SourceValueT", covariant=True)


class PropertyReadContext(Protocol):
    """External services used while reading properties from resolved objects."""

    def local_path_size(self, obj: LocalPathObject) -> int: ...


@dataclass(frozen=True, slots=True)
class OrmLoadRequirement:
    """Mapped ORM state required to materialize a semantic member."""

    attributes: tuple[InstrumentedAttribute[Any], ...] = ()
    whole_entity: bool = False

    def __or__(self, other: "OrmLoadRequirement") -> "OrmLoadRequirement":
        attributes = tuple(dict.fromkeys((*self.attributes, *other.attributes)))
        return OrmLoadRequirement(
            attributes=attributes,
            whole_entity=self.whole_entity or other.whole_entity,
        )


class LoadPolicy(StrEnum):
    """Whether a member participates in an object's default projection."""

    DEFAULT = "default"
    ON_DEMAND = "on-demand"


class ValueSource(Protocol[SourceObjectT, SourceValueT]):
    """Authoritative source for reading and hydrating a scalar value."""

    def get_value(self, obj: SourceObjectT, context: PropertyReadContext) -> SourceValueT: ...

    @property
    def orm_load(self) -> OrmLoadRequirement: ...

    @property
    def query_expression(self) -> ColumnElement[Any] | None: ...


@dataclass(frozen=True, slots=True)
class CallableValueSource(Generic[ObjectT, ValueT]):
    """Value source for non-ORM state or values supplied by external services."""

    read: Callable[[ObjectT, PropertyReadContext], ValueT]

    def get_value(self, obj: ObjectT, context: PropertyReadContext) -> ValueT:
        return self.read(obj, context)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement()

    @property
    def query_expression(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class OrmColumnSource:
    """Value read directly from one mapped attribute on ``obj.orm``."""

    attribute: InstrumentedAttribute[Any]

    def get_value(self, obj: Any, context: PropertyReadContext) -> Any:
        del context
        try:
            orm = getattr(obj, "orm")
        except AttributeError as error:
            raise TypeError("An ORM column source requires an object with an 'orm' attribute") from error
        return getattr(orm, self.attribute.key)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement(attributes=(self.attribute,))

    @property
    def query_expression(self) -> ColumnElement[Any]:
        return cast(ColumnElement[Any], self.attribute)


@dataclass(frozen=True, slots=True)
class PropertyType(Generic[ValueT]):
    """Runtime type metadata and text parser for semantic property values."""

    name: str
    parse_text: Callable[[str], ValueT]

    def parse(self, text: str) -> ValueT:
        try:
            return self.parse_text(text)
        except ValueError as error:
            raise ValueError(f"Invalid {self.name} value {text!r}") from error


def _parse_boolean(text: str) -> bool:
    normalized = text.casefold()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    raise ValueError("expected a boolean")


def _parse_integer_set(text: str) -> frozenset[int]:
    if not text:
        return frozenset()
    return frozenset(int(item.strip()) for item in text.split(","))


def _parse_integer_tuple(text: str) -> tuple[int, ...]:
    if not text:
        return ()
    return tuple(int(item.strip()) for item in text.split(","))


STRING_TYPE = PropertyType[str]("string", lambda text: text)
INTEGER_TYPE = PropertyType[int]("integer", int)
BOOLEAN_TYPE = PropertyType[bool]("boolean", _parse_boolean)
INTEGER_SET_TYPE = PropertyType[frozenset[int]]("comma-separated integers", _parse_integer_set)
INTEGER_TUPLE_TYPE = PropertyType[tuple[int, ...]]("comma-separated integers", _parse_integer_tuple)


@dataclass(frozen=True, slots=True)
class PropertyQuery(Generic[FilterT]):
    """The single query behavior currently supported by one property."""

    operand_type: PropertyType[FilterT]
    apply_comparison: Callable[
        [Select[Any], ColumnElement[Any], FilterT], Select[Any]
    ] = lambda statement, expression, value: statement.where(expression == value)

    def apply(
        self,
        statement: Select[Any],
        source: ValueSource[Any, Any],
        value: FilterT,
    ) -> Select[Any]:
        expression = source.query_expression
        if expression is None:
            raise ValueError("A queryable property requires a SQL expression source")
        return self.apply_comparison(statement, expression, value)


@dataclass(frozen=True, slots=True)
class ValueMember(Generic[ObjectT, ValueT, FilterT]):
    """A scalar-valued member exposed to generic resource-model consumers."""

    name: str
    value_type: PropertyType[ValueT]
    source: ValueSource[ObjectT, ValueT]
    query: PropertyQuery[FilterT] | None = None
    load_policy: LoadPolicy = LoadPolicy.DEFAULT

    def get_value(self, obj: ObjectT, context: PropertyReadContext) -> ValueT:
        return self.source.get_value(obj, context)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return self.source.orm_load

    @property
    def queryable(self) -> bool:
        return self.query is not None

    def predicate(self, value: FilterT) -> "PropertyPredicate[ObjectT]":
        """Bind a typed filter value without applying it to a query yet."""

        if self.query is None:
            raise ValueError(f"Property {self.name!r} is not queryable")
        return PropertyPredicate(property=self, value=value)

    def parse_predicate(self, text: str) -> "PropertyPredicate[ObjectT]":
        if self.query is None:
            raise ValueError(f"Property {self.name!r} is not queryable")
        return self.predicate(self.query.operand_type.parse(text))

    def apply_filter(self, statement: Select[Any], value: FilterT) -> Select[Any]:
        """Apply this property's currently supported filter operation."""

        if self.query is None:
            raise ValueError(f"Property {self.name!r} is not queryable")
        return self.query.apply(statement, self.source, value)


@dataclass(frozen=True, slots=True)
class PropertyPredicate(Generic[ObjectT]):
    """A property filter bound to its value but independent of a provider query."""

    property: ValueMember[ObjectT, Any, Any]
    value: object

    def apply(self, statement: Select[Any]) -> Select[Any]:
        return self.property.apply_filter(statement, self.value)


@dataclass(frozen=True, slots=True)
class SelectionConstraint(Generic[ObjectT]):
    """Provider-private SQL constraint that is not a semantic object property."""

    apply: Callable[[Select[Any]], Select[Any]]


@dataclass(frozen=True, slots=True, order=True)
class MemberRef:
    """Stable qualified name of any semantic object member."""

    object_kind: str
    member_name: str

    def __post_init__(self) -> None:
        if not self.object_kind or not self.member_name or "." in self.member_name:
            raise ValueError("A member reference requires an object kind and an unqualified member name")

    def __str__(self) -> str:
        return f"{self.object_kind}.{self.member_name}"


@dataclass(frozen=True, slots=True, order=True)
class PropertyRef:
    """Stable qualified name of a semantic object property."""

    object_kind: str
    property_name: str

    def __post_init__(self) -> None:
        if not self.object_kind or not self.property_name or "." in self.property_name:
            raise ValueError("A property reference requires an object kind and an unqualified property name")

    @classmethod
    def from_path(cls, path: str) -> "PropertyRef":
        try:
            object_kind, property_name = path.rsplit(".", 1)
        except ValueError as error:
            raise ValueError(f"Property path {path!r} must be qualified as object_kind.property") from error
        return cls(object_kind=object_kind, property_name=property_name)

    def __str__(self) -> str:
        return f"{self.object_kind}.{self.property_name}"


@dataclass(frozen=True, slots=True)
class PropertyPath:
    """A value member reached from a root object through object-valued members."""

    root_kind: str
    link_members: tuple[str, ...]
    property: PropertyRef

    def __str__(self) -> str:
        components = (self.root_kind, *self.link_members, self.property.property_name)
        return ".".join(components)


@dataclass(frozen=True, slots=True)
class ProjectionPath:
    """An object path, optionally followed by one scalar-valued member."""

    root_kind: str
    link_members: tuple[str, ...]
    target_kind: str
    property: PropertyRef | None = None

    def __str__(self) -> str:
        components = (self.root_kind, *self.link_members)
        if self.property is not None:
            components = (*components, self.property.property_name)
        return ".".join(components)


@dataclass(frozen=True, slots=True)
class PropertyCriterion:
    """A qualified property filter whose value has already been typed by its caller."""

    path: PropertyPath
    value: object

    @property
    def property(self) -> PropertyRef:
        """Return the terminal property for predicate construction."""

        return self.path.property


@dataclass(frozen=True, slots=True)
class ObjectSelection(Generic[ObjectT]):
    """Generic selection by stable references and local object properties."""

    refs: frozenset[ObjectRef] | None = None
    predicates: tuple[PropertyPredicate[ObjectT], ...] = ()
    constraints: tuple[SelectionConstraint[ObjectT], ...] = ()

    def keys_for(self, kind: str) -> frozenset[str] | None:
        """Return selected keys after validating their logical-object kind."""

        if self.refs is None:
            return None
        wrong_kinds = {ref.kind for ref in self.refs if ref.kind != kind}
        if wrong_kinds:
            raise ValueError(f"Selection for {kind!r} contains references of kinds {sorted(wrong_kinds)}")
        return frozenset(ref.key for ref in self.refs)

    def apply_filters(
        self,
        statement: Select[Any],
        properties: tuple[ValueMember[Any, Any, Any], ...],
    ) -> Select[Any]:
        """Apply predicates after checking that they belong to the selected kind."""

        allowed_properties = {id(property_definition) for property_definition in properties}
        for predicate in self.predicates:
            if id(predicate.property) not in allowed_properties:
                raise ValueError(f"Property {predicate.property.name!r} does not belong to this object kind")
            statement = predicate.apply(statement)
        for constraint in self.constraints:
            statement = constraint.apply(statement)
        return statement


class LogicalObject(ResourceObject, Protocol):
    """Session-bound semantic object exposed through the registered schema."""

    @property
    def ref(self) -> ObjectRef: ...


class LifecycleSemantics(StrEnum):
    """Lifecycle meaning of an object-valued member."""

    OWNS = "owns"
    REFERENCES = "references"


class RelationshipSemantics(StrEnum):
    """Broad lifecycle meaning of a relationship.

    These values are descriptive.  A future operation policy will decide whether
    and in which direction a particular relationship is traversed for deletion.
    """

    CONTAINS = "contains"
    BELONGS_TO = "belongs-to"
    DERIVED_FROM = "derived-from"
    REFERENCES = "references"


class LinkSource(Protocol[SourceObjectT]):
    """Authoritative source for resolving and hydrating an object-valued member."""

    def targets(
        self,
        obj: SourceObjectT,
        target_kind: str,
    ) -> tuple[ObjectRef | PhysicalObject, ...]: ...

    @property
    def orm_load(self) -> OrmLoadRequirement: ...

    @property
    def queryable(self) -> bool: ...

    def join(
        self,
        statement: Select[Any],
        *,
        forward: bool,
        isouter: bool,
    ) -> Select[Any] | None: ...

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> ObjectSelection[Any] | None: ...


@dataclass(frozen=True, slots=True)
class CallableLinkSource(Generic[ObjectT]):
    """Link source whose targets do not require anchor columns."""

    resolve: Callable[[ObjectT], tuple[ObjectRef | PhysicalObject, ...]]

    def targets(
        self,
        obj: ObjectT,
        target_kind: str,
    ) -> tuple[ObjectRef | PhysicalObject, ...]:
        del target_kind
        return self.resolve(obj)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement()

    @property
    def queryable(self) -> bool:
        return False

    def join(self, statement: Select[Any], *, forward: bool, isouter: bool) -> None:
        del statement, forward, isouter
        return None

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> None:
        del refs, target_kind
        return None


@dataclass(frozen=True, slots=True)
class BatchLinkSource:
    """Object-valued member resolved for many source objects in one SQL query."""

    name: str
    input_name: str
    statement_for_keys: Callable[[Any], Select[Any]]
    input_key: Callable[[ResourceRef], object]
    target_from_row: Callable[[Any], tuple[ResourceRef, ObjectRef | PhysicalObject]]
    batch_size: int = 500

    def targets(
        self,
        obj: Any,
        target_kind: str,
    ) -> tuple[ObjectRef | PhysicalObject, ...]:
        del obj, target_kind
        raise RuntimeError("Batch link targets must be resolved by the projection executor")

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement()

    @property
    def queryable(self) -> bool:
        return False

    def join(self, statement: Select[Any], *, forward: bool, isouter: bool) -> None:
        del statement, forward, isouter
        return None

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> None:
        del refs, target_kind
        return None


@dataclass(frozen=True, slots=True)
class OrmForeignKeySource:
    """Logical-object reference derived from one mapped foreign-key attribute."""

    source_attribute: InstrumentedAttribute[Any]
    target_attribute: InstrumentedAttribute[Any]

    def targets(self, obj: Any, target_kind: str) -> tuple[ObjectRef, ...]:
        value = getattr(getattr(obj, "orm"), self.source_attribute.key)
        return () if value is None else (ObjectRef(target_kind, str(value)),)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement(attributes=(self.source_attribute,))

    @property
    def queryable(self) -> bool:
        return True

    def join(
        self,
        statement: Select[Any],
        *,
        forward: bool,
        isouter: bool,
    ) -> Select[Any]:
        joined_model = (
            self.target_attribute.class_ if forward else self.source_attribute.class_
        )
        return statement.join(
            joined_model,
            self.source_attribute == self.target_attribute,
            isouter=isouter,
        )

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> ObjectSelection[Any]:
        wrong_kinds = {ref.kind for ref in refs if ref.kind != target_kind}
        if wrong_kinds:
            raise ValueError(
                f"Expected {target_kind!r} references, got kinds {sorted(wrong_kinds)}"
            )
        column = self.target_attribute.property.columns[0]
        python_type = column.type.python_type
        try:
            values = tuple(python_type(ref.key) for ref in refs)
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"References to {target_kind!r} are invalid for {column}"
            ) from error
        return ObjectSelection(
            constraints=(
                SelectionConstraint(
                    lambda statement: statement.where(self.source_attribute.in_(values))
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class OrmColumnLinkSource(Generic[ValueT]):
    """Physical targets derived from the value of one mapped attribute."""

    attribute: InstrumentedAttribute[Any]
    resolve_value: Callable[[ValueT], tuple[ObjectRef | PhysicalObject, ...]]

    def targets(
        self,
        obj: Any,
        target_kind: str,
    ) -> tuple[ObjectRef | PhysicalObject, ...]:
        del target_kind
        value = cast(ValueT, getattr(getattr(obj, "orm"), self.attribute.key))
        return self.resolve_value(value)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement(attributes=(self.attribute,))

    @property
    def queryable(self) -> bool:
        return False

    def join(self, statement: Select[Any], *, forward: bool, isouter: bool) -> None:
        del statement, forward, isouter
        return None

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> None:
        del refs, target_kind
        return None


@dataclass(frozen=True, slots=True)
class OrmEntitySource:
    """Physical database-row target backed by the complete mapped entity."""

    def targets(
        self,
        obj: Any,
        target_kind: str,
    ) -> tuple[DatabaseRowObject[Any], ...]:
        del target_kind
        return (DatabaseRowObject(getattr(obj, "orm")),)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return OrmLoadRequirement(whole_entity=True)

    @property
    def queryable(self) -> bool:
        return False

    def join(self, statement: Select[Any], *, forward: bool, isouter: bool) -> None:
        del statement, forward, isouter
        return None

    def select_sources(
        self,
        refs: frozenset[ObjectRef],
        target_kind: str,
    ) -> None:
        del refs, target_kind
        return None


@dataclass(frozen=True, slots=True)
class LinkMember(Generic[ObjectT]):
    """Object-valued member with optional query traversal and lifecycle behavior."""

    name: str
    source_kind: str
    target_kind: str
    source: LinkSource[ObjectT]
    traversal_semantics: RelationshipSemantics | None = None
    lifecycle: LifecycleSemantics | None = None
    target_may_be_shared: bool = False
    load_policy: LoadPolicy = LoadPolicy.DEFAULT

    def targets_for_source(self, obj: ObjectT) -> tuple[ObjectRef | PhysicalObject, ...]:
        return self.source.targets(obj, self.target_kind)

    @property
    def orm_load(self) -> OrmLoadRequirement:
        return self.source.orm_load

    @property
    def queryable(self) -> bool:
        return self.source.queryable

    def apply_join(
        self,
        statement: Select[Any],
        *,
        forward: bool = True,
        isouter: bool = False,
    ) -> Select[Any]:
        joined = self.source.join(statement, forward=forward, isouter=isouter)
        if joined is None:
            raise ValueError(f"Object link {self.source_kind}.{self.name} cannot be used in SQL planning")
        return joined

    def selection_for_targets(
        self,
        refs: frozenset[ObjectRef],
    ) -> ObjectSelection[Any]:
        selection = self.source.select_sources(refs, self.target_kind)
        if selection is None:
            raise ValueError(f"Object link {self.source_kind}.{self.name} is not queryable")
        return selection


ObjectMember: TypeAlias = ValueMember[Any, Any, Any] | LinkMember[Any]


@dataclass(frozen=True, slots=True)
class ObjectKind:
    """Schema definition for an inspectable kind of resource object."""

    name: str
    object_type: type[ResourceObject]
    members: tuple[ObjectMember, ...]


@dataclass(frozen=True, slots=True)
class ObjectLink:
    """A concrete object-valued member in a resource graph."""

    member: MemberRef
    source: ResourceRef
    target: ResourceRef
