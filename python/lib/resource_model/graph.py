"""Resolved logical objects and their resource graph."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from lib.resource_model.resources import PhysicalObject, PhysicalObjectRef, ResourceObject, ResourceRef
from lib.resource_model.schema import LogicalObject, ObjectRef, Relationship, ResourceBinding


@dataclass(frozen=True, slots=True)
class GraphFragment:
    """Objects and edges discovered by one provider invocation."""

    logical_objects: tuple[LogicalObject, ...] = ()
    physical_objects: tuple[PhysicalObject, ...] = ()
    relationships: tuple[Relationship, ...] = ()
    resource_bindings: tuple[ResourceBinding, ...] = ()

    @property
    def objects(self) -> tuple[ResourceObject, ...]:
        return (*self.logical_objects, *self.physical_objects)


class ResourceGraph:
    """A mutable assembly of ORM-backed objects resolved in one unit of work."""

    def __init__(self) -> None:
        self._logical_objects: dict[ObjectRef, LogicalObject] = {}
        self._physical_objects: dict[PhysicalObjectRef, PhysicalObject] = {}
        self._relationships: set[Relationship] = set()
        self._resource_bindings: set[ResourceBinding] = set()

    @property
    def objects(self) -> tuple[ResourceObject, ...]:
        return (*self._logical_objects.values(), *self._physical_objects.values())

    @property
    def relationships(self) -> tuple[Relationship, ...]:
        return tuple(self._relationships)

    @property
    def resource_bindings(self) -> tuple[ResourceBinding, ...]:
        return tuple(self._resource_bindings)

    @property
    def logical_objects(self) -> tuple[LogicalObject, ...]:
        return tuple(self._logical_objects.values())

    @property
    def physical_objects(self) -> tuple[PhysicalObject, ...]:
        return tuple(self._physical_objects.values())

    def add(self, fragment: GraphFragment) -> None:
        for logical_object in fragment.logical_objects:
            existing = self._logical_objects.get(logical_object.ref)
            if existing is not None and existing != logical_object:
                raise ValueError(f"Conflicting resolutions of {logical_object.ref}")
            self._logical_objects[logical_object.ref] = logical_object
        for physical_object in fragment.physical_objects:
            existing = self._physical_objects.get(physical_object.ref)
            if existing is not None and existing != physical_object:
                raise ValueError(f"Conflicting resolutions of {physical_object.ref}")
            self._physical_objects[physical_object.ref] = physical_object
        self._relationships.update(fragment.relationships)
        self._resource_bindings.update(fragment.resource_bindings)

    def get(self, ref: ResourceRef) -> ResourceObject | None:
        if isinstance(ref, ObjectRef):
            return self._logical_objects.get(ref)
        return self._physical_objects.get(ref)

    def outgoing(self, ref: ObjectRef) -> Iterator[Relationship]:
        return (edge for edge in self._relationships if edge.source == ref)

    def unresolved_refs(self) -> frozenset[ObjectRef]:
        """Return relationship endpoints not yet resolved into this partial graph."""

        endpoints: Iterable[ObjectRef] = (
            endpoint for edge in self._relationships for endpoint in (edge.source, edge.target)
        )
        return frozenset(endpoint for endpoint in endpoints if endpoint not in self._logical_objects)
