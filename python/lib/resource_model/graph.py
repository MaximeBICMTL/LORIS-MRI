"""Resolved logical objects and their resource graph."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from lib.resource_model.schema import ObjectRef, Relationship, ResourceObject


@dataclass(frozen=True, slots=True)
class GraphFragment:
    """Objects and edges discovered by one provider invocation."""

    objects: tuple[ResourceObject, ...] = ()
    relationships: tuple[Relationship, ...] = ()


class ResourceGraph:
    """A mutable assembly of ORM-backed objects resolved in one unit of work."""

    def __init__(self) -> None:
        self._objects: dict[ObjectRef, ResourceObject] = {}
        self._relationships: set[Relationship] = set()

    @property
    def objects(self) -> tuple[ResourceObject, ...]:
        return tuple(self._objects.values())

    @property
    def relationships(self) -> tuple[Relationship, ...]:
        return tuple(self._relationships)

    def add(self, fragment: GraphFragment) -> None:
        for logical_object in fragment.objects:
            existing = self._objects.get(logical_object.ref)
            if existing is not None and existing != logical_object:
                raise ValueError(f"Conflicting resolutions of {logical_object.ref}")
            self._objects[logical_object.ref] = logical_object
        self._relationships.update(fragment.relationships)

    def get(self, ref: ObjectRef) -> ResourceObject | None:
        return self._objects.get(ref)

    def outgoing(self, ref: ObjectRef) -> Iterator[Relationship]:
        return (edge for edge in self._relationships if edge.source == ref)

    def unresolved_refs(self) -> frozenset[ObjectRef]:
        """Return relationship endpoints not yet resolved into this partial graph."""

        endpoints: Iterable[ObjectRef] = (
            endpoint for edge in self._relationships for endpoint in (edge.source, edge.target)
        )
        return frozenset(endpoint for endpoint in endpoints if endpoint not in self._objects)
