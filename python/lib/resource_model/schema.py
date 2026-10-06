"""Definitions for the logical LORIS resource schema.

This module deliberately contains no SQLAlchemy models.  It describes the logical
objects projected from those models and the relationships between those objects.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from sqlalchemy.sql import Select

from lib.resource_model.resources import ObjectRef, PhysicalObject, PhysicalObjectRef, ResourceObject

ObjectT = TypeVar("ObjectT")
ValueT = TypeVar("ValueT")
FilterT = TypeVar("FilterT")


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
    apply: Callable[[Select[Any], FilterT], Select[Any]]


@dataclass(frozen=True, slots=True)
class ObjectProperty(Generic[ObjectT, ValueT, FilterT]):
    """A semantic property exposed to generic resource-model consumers."""

    name: str
    value_type: PropertyType[ValueT]
    get_value: Callable[[ObjectT], ValueT]
    query: PropertyQuery[FilterT] | None = None

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
        return self.query.apply(statement, value)


@dataclass(frozen=True, slots=True)
class PropertyPredicate(Generic[ObjectT]):
    """A property filter bound to its value but independent of a provider query."""

    property: ObjectProperty[ObjectT, Any, Any]
    value: object

    def apply(self, statement: Select[Any]) -> Select[Any]:
        return self.property.apply_filter(statement, self.value)


@dataclass(frozen=True, slots=True)
class SelectionConstraint(Generic[ObjectT]):
    """Provider-private SQL constraint that is not a semantic object property."""

    apply: Callable[[Select[Any]], Select[Any]]


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
    """A property reached from a root object through semantic relationships."""

    root_kind: str
    relationship_members: tuple[str, ...]
    property: PropertyRef

    def __str__(self) -> str:
        components = (self.root_kind, *self.relationship_members, self.property.property_name)
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
        properties: tuple[ObjectProperty[Any, Any, Any], ...],
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

    properties: ClassVar[tuple[ObjectProperty[Any, Any, Any], ...]]

    @property
    def ref(self) -> ObjectRef: ...

    @property
    def bound_resources(self) -> tuple["BoundResource", ...]: ...


class ResourceBindingSemantics(StrEnum):
    """Minimal lifecycle meaning of a logical-to-physical object binding."""

    OWNS = "owns"
    REFERENCES = "references"


@dataclass(frozen=True, slots=True)
class BoundResource:
    """A physical object discovered from a logical object and its lifecycle meaning."""

    object: PhysicalObject
    semantics: ResourceBindingSemantics = ResourceBindingSemantics.OWNS


@dataclass(frozen=True, slots=True)
class ResourceBinding:
    """Concrete graph edge from a logical object to a physical object."""

    source: ObjectRef
    target: PhysicalObjectRef
    semantics: ResourceBindingSemantics


@dataclass(frozen=True, slots=True)
class ObjectKind:
    """Schema definition for a kind of logical object."""

    name: str
    object_type: type[LogicalObject]


class RelationshipSemantics(StrEnum):
    """Broad lifecycle meaning of a relationship.

    These values are descriptive.  A future operation policy will decide whether
    and in which direction a particular relationship is traversed for deletion.
    """

    CONTAINS = "contains"
    BELONGS_TO = "belongs-to"
    DERIVED_FROM = "derived-from"
    REFERENCES = "references"


@dataclass(frozen=True, slots=True)
class RelationshipKind:
    """Schema definition for a directed relationship between object kinds."""

    name: str
    member_name: str
    source_kind: str
    target_kind: str
    semantics: RelationshipSemantics
    target_may_be_shared: bool = False


@dataclass(frozen=True, slots=True)
class RelationshipBinding(Generic[ObjectT]):
    """Forward discovery and reverse-query behavior for a relationship."""

    kind: RelationshipKind
    targets_for_source: Callable[[ObjectT], tuple[ObjectRef, ...]]
    source_selection: Callable[[frozenset[ObjectRef]], ObjectSelection[Any]]


@dataclass(frozen=True, slots=True)
class Relationship:
    """A relationship between two concrete logical-object instances."""

    kind: str
    source: ObjectRef
    target: ObjectRef
