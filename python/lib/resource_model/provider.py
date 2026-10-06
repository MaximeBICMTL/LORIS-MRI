"""Typed provider interface and schema registry."""

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar

from sqlalchemy.orm import Session

from lib.resource_model.graph import GraphFragment
from lib.resource_model.schema import (
    BoundResource,
    LogicalObject,
    ObjectKind,
    ObjectProperty,
    ObjectRef,
    ObjectSelection,
    PropertyCriterion,
    PropertyPath,
    PropertyPredicate,
    PropertyRef,
    Relationship,
    RelationshipBinding,
    RelationshipKind,
    RelationshipSemantics,
    ResourceBinding,
)

ObjectT = TypeVar("ObjectT", bound=LogicalObject)


class ResourceProvider(Protocol[ObjectT]):
    """Domain-specific projection from ORM state to logical objects."""

    kind: ObjectKind

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
        self._relationship_kinds: dict[str, RelationshipKind] = {}
        self._providers: dict[str, ResourceProvider[Any]] = {}
        self._relationship_bindings: dict[str, RelationshipBinding[Any]] = {}

    @property
    def object_kinds(self) -> tuple[ObjectKind, ...]:
        return tuple(self._object_kinds.values())

    @property
    def relationship_kinds(self) -> tuple[RelationshipKind, ...]:
        return tuple(self._relationship_kinds.values())

    def register_object_kind(self, kind: ObjectKind) -> None:
        if kind.name in self._object_kinds:
            raise ValueError(f"Object kind {kind.name!r} is already registered")
        property_names = [property_definition.name for property_definition in kind.object_type.properties]
        if len(property_names) != len(set(property_names)):
            raise ValueError(f"Object kind {kind.name!r} contains duplicate property names")
        self._object_kinds[kind.name] = kind

    def register_relationship_kind(self, kind: RelationshipKind) -> None:
        if kind.name in self._relationship_kinds:
            raise ValueError(f"Relationship kind {kind.name!r} is already registered")
        missing = {kind.source_kind, kind.target_kind} - self._object_kinds.keys()
        if missing:
            raise ValueError(f"Relationship {kind.name!r} uses unknown object kinds: {sorted(missing)}")
        if not kind.member_name or "." in kind.member_name:
            raise ValueError(f"Relationship {kind.name!r} has an invalid member name")
        if any(
            registered.source_kind == kind.source_kind
            and registered.member_name == kind.member_name
            for registered in self._relationship_kinds.values()
        ):
            raise ValueError(
                f"Object kind {kind.source_kind!r} already has a relationship member "
                f"named {kind.member_name!r}"
            )
        self._relationship_kinds[kind.name] = kind

    def register_provider(self, provider: ResourceProvider[Any]) -> None:
        kind_name = provider.kind.name
        if kind_name not in self._object_kinds:
            raise ValueError(f"Provider kind {kind_name!r} is not registered")
        if kind_name in self._providers:
            raise ValueError(f"Provider for {kind_name!r} is already registered")
        self._providers[kind_name] = provider

    def provider(self, kind: str) -> ResourceProvider[Any]:
        try:
            return self._providers[kind]
        except KeyError as error:
            raise ValueError(f"No provider is registered for object kind {kind!r}") from error

    def register_relationship_binding(self, binding: RelationshipBinding[Any]) -> None:
        name = binding.kind.name
        if self._relationship_kinds.get(name) is not binding.kind:
            raise ValueError(f"Relationship kind {name!r} must be registered before its query binding")
        if name in self._relationship_bindings:
            raise ValueError(f"Query binding for relationship {name!r} is already registered")
        self._relationship_bindings[name] = binding

    def relationship_binding(self, name: str) -> RelationshipBinding[Any]:
        try:
            return self._relationship_bindings[name]
        except KeyError as error:
            raise ValueError(f"No query binding is registered for relationship {name!r}") from error

    def properties(self, kind: str) -> tuple[ObjectProperty[Any, Any, Any], ...]:
        try:
            return self._object_kinds[kind].object_type.properties
        except KeyError as error:
            raise ValueError(f"Unknown object kind {kind!r}") from error

    def queryable_properties(self, kind: str) -> tuple[ObjectProperty[Any, Any, Any], ...]:
        return tuple(
            property_definition
            for property_definition in self.properties(kind)
            if property_definition.queryable
        )

    def relationships(self, kind: str) -> tuple[RelationshipKind, ...]:
        """Return the semantic relationship members exposed by an object kind."""

        if kind not in self._object_kinds:
            raise ValueError(f"Unknown object kind {kind!r}")
        return tuple(
            relationship
            for relationship in self._relationship_kinds.values()
            if relationship.source_kind == kind
        )

    def relationship(self, kind: str, member_name: str) -> RelationshipKind:
        """Resolve one relationship through its source-scoped semantic member name."""

        for relationship in self.relationships(kind):
            if relationship.member_name == member_name:
                return relationship
        raise ValueError(f"Unknown relationship {kind}.{member_name}")

    def property(self, ref: PropertyRef | str) -> ObjectProperty[Any, Any, Any]:
        property_ref = PropertyRef.from_path(ref) if isinstance(ref, str) else ref
        for property_definition in self.properties(property_ref.object_kind):
            if property_definition.name == property_ref.property_name:
                return property_definition
        raise ValueError(f"Unknown property {property_ref}")

    def property_path(self, path: str) -> PropertyPath:
        """Resolve a schema-aware relationship path ending in a property."""

        candidates: list[PropertyPath] = []
        for root_kind in self._object_kinds:
            prefix = f"{root_kind}."
            if not path.startswith(prefix):
                continue
            components = path[len(prefix) :].split(".")
            if not components or any(not component for component in components):
                continue
            current_kind = root_kind
            relationship_members: list[str] = []
            try:
                for member_name in components[:-1]:
                    relationship = self.relationship(current_kind, member_name)
                    relationship_members.append(member_name)
                    current_kind = relationship.target_kind
                property_ref = PropertyRef(current_kind, components[-1])
                self.property(property_ref)
            except ValueError:
                continue
            candidates.append(
                PropertyPath(
                    root_kind=root_kind,
                    relationship_members=tuple(relationship_members),
                    property=property_ref,
                )
            )
        if not candidates:
            raise ValueError(f"Unknown property path {path!r}")
        if len(candidates) > 1:
            raise ValueError(f"Ambiguous property path {path!r}")
        return candidates[0]

    def path_bindings(self, path: PropertyPath) -> tuple[RelationshipBinding[Any], ...]:
        """Resolve the ordered relationship bindings traversed by a property path."""

        current_kind = path.root_kind
        bindings: list[RelationshipBinding[Any]] = []
        for member_name in path.relationship_members:
            relationship = self.relationship(current_kind, member_name)
            bindings.append(self.relationship_binding(relationship.name))
            current_kind = relationship.target_kind
        if current_kind != path.property.object_kind:
            raise ValueError(f"Property path {path} ends at the wrong object kind")
        return tuple(bindings)

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

    def outgoing_bindings(self, source_kind: str) -> tuple[RelationshipBinding[Any], ...]:
        return tuple(
            binding
            for binding in self._relationship_bindings.values()
            if binding.kind.source_kind == source_kind
        )

    def belongs_to_path(self, source_kind: str, target_kind: str) -> tuple[RelationshipBinding[Any], ...]:
        """Find the unique transitive belongs-to path between two object kinds."""

        if source_kind == target_kind:
            return ()
        paths: list[tuple[RelationshipKind, ...]] = []

        def visit(current: str, path: tuple[RelationshipKind, ...], visited: frozenset[str]) -> None:
            for relationship in self._relationship_kinds.values():
                if (
                    relationship.source_kind != current
                    or relationship.semantics is not RelationshipSemantics.BELONGS_TO
                    or relationship.target_kind in visited
                ):
                    continue
                next_path = (*path, relationship)
                if relationship.target_kind == target_kind:
                    paths.append(next_path)
                else:
                    visit(relationship.target_kind, next_path, visited | {relationship.target_kind})

        visit(source_kind, (), frozenset({source_kind}))
        if not paths:
            raise ValueError(f"No belongs-to path exists from {source_kind!r} to {target_kind!r}")
        if len(paths) > 1:
            rendered = [" -> ".join(relationship.name for relationship in path) for path in paths]
            raise ValueError(
                f"Ambiguous belongs-to path from {source_kind!r} to {target_kind!r}: {rendered}"
            )
        return tuple(self.relationship_binding(relationship.name) for relationship in paths[0])

    def belongs_to_connection(
        self,
        source_kind: str,
        target_kind: str,
    ) -> tuple[tuple[RelationshipBinding[Any], bool], ...]:
        """Find a unique belongs-to path, allowing traversal in either direction.

        The boolean on each step is true when traversing from the relationship's
        source to its target and false when traversing in reverse.
        """

        if source_kind == target_kind:
            return ()
        paths: list[tuple[tuple[RelationshipKind, bool], ...]] = []

        def visit(
            current: str,
            path: tuple[tuple[RelationshipKind, bool], ...],
            visited: frozenset[str],
        ) -> None:
            for relationship in self._relationship_kinds.values():
                if relationship.semantics is not RelationshipSemantics.BELONGS_TO:
                    continue
                if relationship.source_kind == current:
                    neighbor = relationship.target_kind
                    forward = True
                elif relationship.target_kind == current:
                    neighbor = relationship.source_kind
                    forward = False
                else:
                    continue
                if neighbor in visited:
                    continue
                next_path = (*path, (relationship, forward))
                if neighbor == target_kind:
                    paths.append(next_path)
                else:
                    visit(neighbor, next_path, visited | {neighbor})

        visit(source_kind, (), frozenset({source_kind}))
        if not paths:
            raise ValueError(f"No belongs-to connection exists from {source_kind!r} to {target_kind!r}")
        if len(paths) > 1:
            rendered = [" -> ".join(step[0].name for step in path) for path in paths]
            raise ValueError(
                f"Ambiguous belongs-to connection from {source_kind!r} to {target_kind!r}: "
                f"{rendered}"
            )
        return tuple(
            (self.relationship_binding(relationship.name), forward)
            for relationship, forward in paths[0]
        )

    def validate_relationship(self, relationship: Relationship) -> None:
        try:
            definition = self._relationship_kinds[relationship.kind]
        except KeyError as error:
            raise ValueError(f"Unknown relationship kind {relationship.kind!r}") from error
        if relationship.source.kind != definition.source_kind:
            raise ValueError(f"Invalid source kind for relationship {relationship.kind!r}")
        if relationship.target.kind != definition.target_kind:
            raise ValueError(f"Invalid target kind for relationship {relationship.kind!r}")


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

        relationships = tuple(
            Relationship(kind=binding.kind.name, source=obj.ref, target=target)
            for binding in self.schema.outgoing_bindings(provider.kind.name)
            for obj in objects
            for target in binding.targets_for_source(obj)
        )
        for relationship in relationships:
            self.schema.validate_relationship(relationship)

        bound_resources: tuple[tuple[ObjectRef, BoundResource], ...] = tuple(
            (logical_object.ref, bound_resource)
            for logical_object in objects
            for bound_resource in logical_object.bound_resources
        )
        return GraphFragment(
            logical_objects=objects,
            physical_objects=tuple(bound.object for _, bound in bound_resources),
            relationships=relationships,
            resource_bindings=tuple(
                ResourceBinding(
                    source=source,
                    target=bound.object.ref,
                    semantics=bound.semantics,
                )
                for source, bound in bound_resources
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

        local_predicates: list[PropertyPredicate[Any]] = []
        constrained_refs = refs
        for criterion in criteria:
            predicate = self.schema.predicate(criterion)
            if criterion.path.root_kind == kind and not criterion.path.relationship_members:
                local_predicates.append(predicate)
                continue
            matching_refs = self._matching_refs(db, kind, criterion.path, predicate)
            constrained_refs = (
                matching_refs if constrained_refs is None else constrained_refs & matching_refs
            )

        provider = self.schema.provider(kind)
        return self.resolve(
            db,
            provider,
            ObjectSelection(refs=constrained_refs, predicates=tuple(local_predicates)),
        )

    def _matching_refs(
        self,
        db: Session,
        source_kind: str,
        property_path: PropertyPath,
        predicate: PropertyPredicate[Any],
    ) -> frozenset[ObjectRef]:
        target_fragment = self.resolve(
            db,
            self.schema.provider(property_path.property.object_kind),
            ObjectSelection(predicates=(predicate,)),
        )
        target_refs = frozenset(obj.ref for obj in target_fragment.logical_objects)
        for binding in reversed(self.schema.path_bindings(property_path)):
            source_fragment = self.resolve(
                db,
                self.schema.provider(binding.kind.source_kind),
                binding.source_selection(target_refs),
            )
            target_refs = frozenset(obj.ref for obj in source_fragment.logical_objects)
        if source_kind == property_path.root_kind:
            return target_refs
        implicit_path = self.schema.belongs_to_path(source_kind, property_path.root_kind)
        for binding in reversed(implicit_path):
            source_fragment = self.resolve(
                db,
                self.schema.provider(binding.kind.source_kind),
                binding.source_selection(target_refs),
            )
            target_refs = frozenset(obj.ref for obj in source_fragment.logical_objects)
        return target_refs

    def traverse_refs(
        self,
        db: Session,
        refs: frozenset[ObjectRef],
        steps: tuple[tuple[RelationshipBinding[Any], bool], ...],
    ) -> frozenset[ObjectRef]:
        """Propagate object references through preplanned relationship steps."""

        current_refs = refs
        for binding, forward in steps:
            if forward:
                fragment = self.resolve(
                    db,
                    self.schema.provider(binding.kind.source_kind),
                    ObjectSelection(refs=current_refs),
                )
                current_refs = frozenset(
                    target
                    for logical_object in fragment.logical_objects
                    for target in binding.targets_for_source(logical_object)
                )
            else:
                fragment = self.resolve(
                    db,
                    self.schema.provider(binding.kind.source_kind),
                    binding.source_selection(current_refs),
                )
                current_refs = frozenset(
                    logical_object.ref for logical_object in fragment.logical_objects
                )
        return current_refs
