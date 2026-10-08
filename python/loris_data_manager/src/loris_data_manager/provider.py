"""Typed provider interface and schema registry."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, cast

from sqlalchemy import inspect as inspect_orm
from sqlalchemy.orm import Mapper, Session, load_only
from sqlalchemy.sql import Select

from loris_data_manager.graph import GraphFragment
from loris_data_manager.resources import ObjectRef, ResourceObject
from loris_data_manager.schema import (
    LinkMember,
    LogicalObject,
    ObjectKind,
    ObjectLink,
    ObjectSelection,
    OrmForeignKeySource,
    OrmLoadRequirement,
    PropertyCriterion,
    PropertyPath,
    PropertyPredicate,
    PropertyRef,
    RelationshipSemantics,
    ValueMember,
)

ObjectT = TypeVar("ObjectT", bound=LogicalObject)


@dataclass(frozen=True, slots=True)
class ProviderLoadStep:
    """One provider-owned query populated from a preceding set of object IDs."""

    name: str
    input_name: str
    input_kind: str
    statement_for_keys: Callable[[Any], Select[Any]]
    fragment_from_rows: Callable[[tuple[Any, ...]], GraphFragment]
    batch_size: int = 500


@dataclass(frozen=True, slots=True)
class OrmEntityLoad:
    """One ORM entity returned and selectively hydrated by a planned statement."""

    kind: str
    requirement: OrmLoadRequirement


class ResourceProvider(Protocol[ObjectT]):
    """Domain-specific projection from ORM state to logical objects."""

    kind: ObjectKind
    orm_model: type[Any]

    def statement(self, selection: ObjectSelection[ObjectT]) -> Select[Any]:
        """Build, but do not execute, a query for this provider's objects."""
        ...

    def object_from_orm(self, row: Any) -> ObjectT:
        """Wrap one ORM result as this provider's logical object."""
        ...

    @property
    def deferred_links(self) -> frozenset[str]:
        """Links whose objects are loaded through explicit plan steps."""
        ...

    def load_steps(self, links: frozenset[str]) -> tuple[ProviderLoadStep, ...]: ...

    def find(
        self,
        db: Session,
        selection: ObjectSelection[ObjectT],
    ) -> Sequence[ObjectT]:
        """Resolve logical objects matching a generic selection."""
        ...


class ResourceSchema:
    """Registry to which core and external modules contribute schema definitions."""

    def __init__(self) -> None:
        self._object_kinds: dict[str, ObjectKind] = {}
        self._providers: dict[str, ResourceProvider[Any]] = {}

    @property
    def object_kinds(self) -> tuple[ObjectKind, ...]:
        return tuple(self._object_kinds.values())

    def register_object_kind(self, kind: ObjectKind) -> None:
        if kind.name in self._object_kinds:
            raise ValueError(f"Object kind {kind.name!r} is already registered")
        member_names = [member.name for member in kind.members]
        if len(member_names) != len(set(member_names)):
            raise ValueError(f"Object kind {kind.name!r} contains duplicate member names")
        if any(not name or "." in name for name in member_names):
            raise ValueError(f"Object kind {kind.name!r} contains an invalid member name")
        missing_targets = {
            member.target_kind
            for member in kind.members
            if isinstance(member, LinkMember) and member.target_kind not in self._object_kinds
        }
        if missing_targets:
            raise ValueError(
                f"Object kind {kind.name!r} links to unknown object kinds: {sorted(missing_targets)}"
            )
        wrong_sources = {
            member.source_kind
            for member in kind.members
            if isinstance(member, LinkMember) and member.source_kind != kind.name
        }
        if wrong_sources:
            raise ValueError(
                f"Object kind {kind.name!r} contains links for source kinds {sorted(wrong_sources)}"
            )
        self._object_kinds[kind.name] = kind

    def register_provider(self, provider: ResourceProvider[Any]) -> None:
        kind_name = provider.kind.name
        if kind_name not in self._object_kinds:
            raise ValueError(f"Provider kind {kind_name!r} is not registered")
        if kind_name in self._providers:
            raise ValueError(f"Provider for {kind_name!r} is already registered")
        self._providers[kind_name] = provider
        self._validate_mapped_links()

    def register_member(self, kind: str, member: ValueMember[Any, Any, Any] | LinkMember[Any]) -> None:
        definition = self.object_kind(kind)
        if any(existing.name == member.name for existing in definition.members):
            raise ValueError(f"Object kind {kind!r} already has a member named {member.name!r}")
        if isinstance(member, LinkMember):
            if member.source_kind != kind:
                raise ValueError(f"Link member {member.name!r} has the wrong source kind")
            if member.target_kind not in self._object_kinds:
                raise ValueError(f"Link member {member.name!r} uses an unknown target kind")
        self._object_kinds[kind] = ObjectKind(
            name=definition.name,
            object_type=definition.object_type,
            members=(*definition.members, member),
        )
        self._validate_mapped_links()

    def _validate_mapped_links(self) -> None:
        for source_kind, source_provider in self._providers.items():
            for link in self.links(source_kind):
                if not isinstance(link.source, OrmForeignKeySource):
                    continue
                if link.source.source_attribute.class_ is not source_provider.orm_model:
                    raise ValueError(
                        f"Mapped relationship {source_kind}.{link.name} uses a source attribute "
                        f"from {link.source.source_attribute.class_.__name__!r}"
                    )
                target_provider = self._providers.get(link.target_kind)
                if (
                    target_provider is not None
                    and link.source.target_attribute.class_ is not target_provider.orm_model
                ):
                    raise ValueError(
                        f"Mapped relationship {source_kind}.{link.name} uses a target attribute "
                        f"from {link.source.target_attribute.class_.__name__!r}"
                    )

    def provider(self, kind: str) -> ResourceProvider[Any]:
        try:
            return self._providers[kind]
        except KeyError as error:
            raise ValueError(f"No provider is registered for object kind {kind!r}") from error

    def has_provider(self, kind: str) -> bool:
        return kind in self._providers

    def object_kind(self, kind: str) -> ObjectKind:
        try:
            return self._object_kinds[kind]
        except KeyError as error:
            raise ValueError(f"Unknown object kind {kind!r}") from error

    def kind_for_object(self, obj: ResourceObject) -> ObjectKind:
        matches = [kind for kind in self._object_kinds.values() if isinstance(obj, kind.object_type)]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one registered kind for {type(obj).__name__}")
        return matches[0]

    def members(self, kind: str) -> tuple[ValueMember[Any, Any, Any] | LinkMember[Any], ...]:
        try:
            return self._object_kinds[kind].members
        except KeyError as error:
            raise ValueError(f"Unknown object kind {kind!r}") from error

    def properties(self, kind: str) -> tuple[ValueMember[Any, Any, Any], ...]:
        return tuple(
            member for member in self.members(kind) if isinstance(member, ValueMember)
        )

    def queryable_properties(self, kind: str) -> tuple[ValueMember[Any, Any, Any], ...]:
        return tuple(
            member for member in self.properties(kind) if member.queryable
        )

    def links(self, kind: str) -> tuple[LinkMember[Any], ...]:
        return tuple(member for member in self.members(kind) if isinstance(member, LinkMember))

    def link(self, kind: str, member_name: str) -> LinkMember[Any]:
        for member in self.links(kind):
            if member.name == member_name:
                return member
        raise ValueError(f"Unknown object link {kind}.{member_name}")

    def property(self, ref: PropertyRef | str) -> ValueMember[Any, Any, Any]:
        property_ref = PropertyRef.from_path(ref) if isinstance(ref, str) else ref
        for property_definition in self.properties(property_ref.object_kind):
            if property_definition.name == property_ref.property_name:
                return property_definition
        raise ValueError(f"Unknown property {property_ref}")

    def property_path(self, path: str) -> PropertyPath:
        """Resolve object-valued members followed by a scalar-valued member."""

        candidates: list[PropertyPath] = []
        for root_kind in self._object_kinds:
            prefix = f"{root_kind}."
            if not path.startswith(prefix):
                continue
            components = path[len(prefix) :].split(".")
            if not components or any(not component for component in components):
                continue
            current_kind = root_kind
            link_members: list[str] = []
            try:
                for member_name in components[:-1]:
                    link = self.link(current_kind, member_name)
                    link_members.append(member_name)
                    current_kind = link.target_kind
                property_ref = PropertyRef(current_kind, components[-1])
                self.property(property_ref)
            except ValueError:
                continue
            candidates.append(
                PropertyPath(
                    root_kind=root_kind,
                    link_members=tuple(link_members),
                    property=property_ref,
                )
            )
        if not candidates:
            raise ValueError(f"Unknown property path {path!r}")
        if len(candidates) > 1:
            raise ValueError(f"Ambiguous property path {path!r}")
        return candidates[0]

    def path_links(self, path: PropertyPath) -> tuple[LinkMember[Any], ...]:
        """Resolve the ordered object-valued members traversed by a property path."""

        current_kind = path.root_kind
        links: list[LinkMember[Any]] = []
        for member_name in path.link_members:
            link = self.link(current_kind, member_name)
            links.append(link)
            current_kind = link.target_kind
        if current_kind != path.property.object_kind:
            raise ValueError(f"Property path {path} ends at the wrong object kind")
        return tuple(links)

    def predicate(self, criterion: PropertyCriterion) -> PropertyPredicate[Any]:
        return self.property(criterion.property).predicate(criterion.value)

    def criterion(self, path: str, value: object) -> PropertyCriterion:
        """Create a typed criterion from a schema-resolved property path."""

        property_path = self.property_path(path)
        property_definition = self.property(property_path.property)
        if property_definition.query is None:
            raise ValueError(f"Property {property_path} is not queryable")
        return PropertyCriterion(path=property_path, value=value)

    def parse_criterion(self, path: str, text: str) -> PropertyCriterion:
        """Resolve a qualified property and parse a textual query operand."""

        property_path = self.property_path(path)
        property_definition = self.property(property_path.property)
        if property_definition.query is None:
            raise ValueError(f"Property {property_path} is not queryable")
        return PropertyCriterion(
            path=property_path,
            value=property_definition.query.operand_type.parse(text),
        )

    def belongs_to_traversal(
        self,
        source_kind: str,
        target_kind: str,
    ) -> tuple[tuple[LinkMember[Any], bool], ...]:
        """Find a unique monotonic belongs-to path in either direction.

        The boolean on each step is true when traversing from the relationship's
        source to its target and false when traversing in reverse. A path never
        changes direction, which prevents implicit traversal between siblings.
        """

        if source_kind == target_kind:
            return ()
        paths: list[tuple[tuple[LinkMember[Any], bool], ...]] = []

        def visit(
            current: str,
            path: tuple[tuple[LinkMember[Any], bool], ...],
            visited: frozenset[str],
            *,
            forward: bool,
        ) -> None:
            candidates = (
                link
                for candidate_kind in self._object_kinds
                for link in self.links(candidate_kind)
                if link.traversal_semantics is RelationshipSemantics.BELONGS_TO
                and link.queryable
            )
            for link in candidates:
                if forward and link.source_kind == current:
                    neighbor = link.target_kind
                elif not forward and link.target_kind == current:
                    neighbor = link.source_kind
                else:
                    continue
                if neighbor in visited:
                    continue
                next_path = (*path, (link, forward))
                if neighbor == target_kind:
                    paths.append(next_path)
                else:
                    visit(
                        neighbor,
                        next_path,
                        visited | {neighbor},
                        forward=forward,
                    )

        visit(source_kind, (), frozenset({source_kind}), forward=True)
        visit(source_kind, (), frozenset({source_kind}), forward=False)
        if not paths:
            raise ValueError(f"No belongs-to path exists from {source_kind!r} to {target_kind!r}")
        if len(paths) > 1:
            rendered = [" -> ".join(step[0].name for step in path) for path in paths]
            raise ValueError(
                f"Ambiguous belongs-to path from {source_kind!r} to {target_kind!r}: "
                f"{rendered}"
            )
        return paths[0]


class ResourceModel:
    """Resolve provider projections and enforce the registered schema boundary."""

    def __init__(self, schema: ResourceSchema) -> None:
        self.schema = schema

    def resolve(
        self,
        db: Session,
        provider: ResourceProvider[ObjectT],
        selection: ObjectSelection[ObjectT],
    ) -> GraphFragment:
        objects = tuple(provider.find(db, selection))
        return self.fragment(provider, objects)

    def fragment(
        self,
        provider: ResourceProvider[ObjectT],
        objects: tuple[ObjectT, ...],
        link_names: frozenset[str] | None = None,
    ) -> GraphFragment:
        known_kinds = {kind.name: kind for kind in self.schema.object_kinds}
        try:
            registered_kind = known_kinds[provider.kind.name]
        except KeyError as error:
            raise ValueError(f"Provider kind {provider.kind.name!r} is not registered") from error

        for logical_object in objects:
            if logical_object.ref.kind != provider.kind.name:
                raise ValueError(f"Provider returned an object of kind {logical_object.ref.kind!r}")
            if not isinstance(logical_object, registered_kind.object_type):
                raise TypeError(f"Invalid object class for {logical_object.ref}")

        targets = tuple(
            (logical_object.ref, link, target)
            for logical_object in objects
            for link in self.schema.links(provider.kind.name)
            if link_names is None or link.name in link_names
            for target in link.targets_for_source(logical_object)
        )
        for _, link, target in targets:
            target_kind = next(kind for kind in self.schema.object_kinds if kind.name == link.target_kind)
            if isinstance(target, ObjectRef):
                if target.kind != link.target_kind:
                    raise ValueError(f"Invalid target kind for link {link.source_kind}.{link.name}")
            elif not isinstance(target, target_kind.object_type):
                raise TypeError(f"Invalid target object for link {link.source_kind}.{link.name}")

        return GraphFragment(
            logical_objects=objects,
            physical_objects=tuple(
                target for _, _, target in targets if not isinstance(target, ObjectRef)
            ),
            links=tuple(
                ObjectLink(
                    member=PropertyRef(link.source_kind, link.name),
                    source=source,
                    target=target if isinstance(target, ObjectRef) else target.ref,
                )
                for source, link, target in targets
            ),
        )

    def select_objects(
        self,
        db: Session,
        kind: str,
        *,
        refs: frozenset[ObjectRef] | None = None,
        criteria: tuple[PropertyCriterion, ...] = (),
    ) -> GraphFragment:
        """Resolve a kind using local or transitively reachable belongs-to properties."""

        plan = self.plan_selection(kind, refs=refs, criteria=criteria)
        return self.resolve_statement(db, self.schema.provider(kind), plan)

    def plan_selection(
        self,
        kind: str,
        *,
        refs: frozenset[ObjectRef] | None = None,
        criteria: tuple[PropertyCriterion, ...] = (),
        orm_load: OrmLoadRequirement | None = None,
        projection_links: tuple[LinkMember[Any], ...] = (),
        related_entities: tuple[OrmEntityLoad, ...] = (),
    ) -> Select[Any]:
        """Lower a semantic selection to one unexecuted, JOIN-based statement."""

        predicates: list[PropertyPredicate[Any]] = []
        constrained_refs = refs
        joins: list[tuple[LinkMember[Any], bool, bool]] = []

        def add_join(link: LinkMember[Any], *, forward: bool, isouter: bool) -> None:
            for index, (existing, existing_forward, existing_outer) in enumerate(joins):
                if (
                    existing.source_kind == link.source_kind
                    and existing.name == link.name
                    and existing_forward is forward
                ):
                    joins[index] = (existing, forward, existing_outer and isouter)
                    return
            joins.append((link, forward, isouter))

        for criterion in criteria:
            predicate = self.schema.predicate(criterion)
            predicates.append(predicate)
            if criterion.path.root_kind == kind and not criterion.path.link_members:
                continue
            if criterion.path.root_kind == kind:
                path = tuple((link, True) for link in self.schema.path_links(criterion.path))
            else:
                path = (
                    *self.schema.belongs_to_traversal(kind, criterion.path.root_kind),
                    *((link, True) for link in self.schema.path_links(criterion.path)),
                )
            directions = {forward for _, forward in path}
            if len(directions) > 1:
                raise ValueError(
                    f"Property path from {kind!r} to {criterion.path} changes "
                    "belongs-to traversal direction"
                )
            for link, forward in path:
                if not link.queryable:
                    raise ValueError(
                        f"Object link {link.source_kind}.{link.name} cannot be used in SQL planning"
                    )
                add_join(link, forward=forward, isouter=False)
        for link in projection_links:
            if not link.queryable:
                raise ValueError(
                    f"Object link {link.source_kind}.{link.name} cannot be used in SQL planning"
                )
            add_join(link, forward=True, isouter=True)
        provider = self.schema.provider(kind)
        statement = provider.statement(ObjectSelection(refs=constrained_refs))
        for link, forward, isouter in joins:
            statement = link.apply_join(
                statement,
                forward=forward,
                isouter=isouter,
            )
        for predicate in predicates:
            statement = predicate.apply(statement)
        for entity in related_entities:
            statement = statement.add_columns(self.schema.provider(entity.kind).orm_model)
        if joins:
            statement = statement.distinct()
        if orm_load is not None:
            statement = _apply_orm_load(statement, provider, orm_load)
        for entity in related_entities:
            statement = _apply_orm_load(
                statement,
                self.schema.provider(entity.kind),
                entity.requirement,
            )
        return statement

    def resolve_statement(
        self,
        db: Session,
        provider: ResourceProvider[ObjectT],
        statement: Select[Any],
        *,
        link_names: frozenset[str] | None = None,
    ) -> GraphFragment:
        """Resolve a previously planned provider statement."""

        objects = tuple(provider.object_from_orm(row) for row in db.scalars(statement).unique())
        return self.fragment(provider, objects, link_names)

    def traverse_refs(
        self,
        db: Session,
        refs: frozenset[ObjectRef],
        steps: tuple[tuple[LinkMember[Any], bool], ...],
    ) -> frozenset[ObjectRef]:
        """Propagate object references through preplanned relationship steps."""

        current_refs = refs
        for link, forward in steps:
            if forward:
                fragment = self.resolve(
                    db,
                    self.schema.provider(link.source_kind),
                    ObjectSelection(refs=current_refs),
                )
                current_refs = frozenset(
                    target
                    for logical_object in fragment.logical_objects
                    for target in link.targets_for_source(logical_object)
                    if isinstance(target, ObjectRef)
                )
            else:
                fragment = self.resolve(
                    db,
                    self.schema.provider(link.source_kind),
                    link.selection_for_targets(current_refs),
                )
                current_refs = frozenset(
                    logical_object.ref for logical_object in fragment.logical_objects
                )
        return current_refs


def _apply_orm_load(
    statement: Select[Any],
    provider: ResourceProvider[Any],
    requirement: OrmLoadRequirement,
) -> Select[Any]:
    """Restrict one entity-returning provider statement to required mapped attributes."""

    if requirement.whole_entity:
        return statement
    wrong_models = {
        attribute.class_.__name__
        for attribute in requirement.attributes
        if attribute.class_ is not provider.orm_model
    }
    if wrong_models:
        raise ValueError(
            f"Hydration for {provider.kind.name!r} contains attributes from ORM models "
            f"{sorted(wrong_models)}"
        )
    mapper = cast(Mapper[Any], inspect_orm(provider.orm_model))
    identity_attributes = tuple(
        mapper.get_property_by_column(column).class_attribute for column in mapper.primary_key
    )
    attributes = tuple(
        dict.fromkeys((*identity_attributes, *requirement.attributes))
    )
    return statement.options(
        load_only(*attributes, raiseload=True)
    )
