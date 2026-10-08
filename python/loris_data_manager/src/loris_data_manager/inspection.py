"""Read-only inspection of logical and physical LORIS resource objects."""

import json
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from loris_data_manager.graph import GraphFragment, ResourceGraph
from loris_data_manager.provider import ResourceModel, ResourceSchema
from loris_data_manager.providers.core import PROJECT, SESSION, SITE
from loris_data_manager.resources import (
    DatabaseRowObject,
    LocalPathObject,
    LocalPathType,
    ObjectRef,
    PhysicalObject,
    ResourceObject,
    ResourceRef,
)
from loris_data_manager.schema import (
    PropertyCriterion,
    PropertyPath,
    PropertyReadContext,
)


@dataclass(frozen=True, slots=True)
class FilesystemPropertyReadContext(PropertyReadContext):
    """Resolve observed local-path properties through configured storage roots."""

    storage_roots: Mapping[str, Path]

    def local_path_size(self, obj: LocalPathObject) -> int:
        try:
            root = self.storage_roots[obj.storage_root]
        except KeyError as error:
            raise ValueError(f"No path is configured for storage root {obj.storage_root!r}") from error
        path = root / obj.relative_path
        metadata = path.stat()
        if obj.expected_type is LocalPathType.FILE and not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"Expected a file at {obj.ref}")
        if obj.expected_type is LocalPathType.DIRECTORY and not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"Expected a directory at {obj.ref}")
        return metadata.st_size


@dataclass(frozen=True, slots=True)
class InspectionSelection:
    expression: str
    target_kind: str
    property_path: PropertyPath | None = None

    @property
    def property_name(self) -> str | None:
        return None if self.property_path is None else self.property_path.property.property_name


@dataclass(frozen=True, slots=True)
class InspectionQuery:
    selections: tuple[InspectionSelection, ...]
    criteria: tuple[PropertyCriterion, ...] = ()
    select_all: bool = False
    expand_related: bool = False

    def __post_init__(self) -> None:
        if not self.selections:
            raise ValueError("At least one object or property must be selected")
        if not self.select_all and not self.criteria:
            raise ValueError("At least one selector is required unless select_all is enabled")


@dataclass(frozen=True, slots=True)
class InspectionResult:
    schema: ResourceSchema
    read_context: PropertyReadContext
    graph: ResourceGraph
    selected: frozenset[ResourceRef]
    projections: tuple[tuple[InspectionSelection, frozenset[ResourceRef]], ...]


def inspect_resources(
    db: Session,
    schema: ResourceSchema,
    query: InspectionQuery,
    *,
    storage_roots: Mapping[str, Path] | None = None,
) -> InspectionResult:
    model = ResourceModel(schema)
    graph = ResourceGraph()
    selected: set[ResourceRef] = set()
    anchor_kind = _anchor_kind(query)
    anchor_fragment = model.select_objects(db, anchor_kind, criteria=query.criteria)
    anchor_refs = frozenset(obj.ref for obj in anchor_fragment.logical_objects)
    projections: list[tuple[InspectionSelection, frozenset[ResourceRef]]] = []

    for selection in query.selections:
        if schema.has_provider(selection.target_kind):
            target_refs = _project_logical_refs(
                db, schema, model, anchor_kind, anchor_refs, selection
            )
            target_fragment = model.select_objects(db, selection.target_kind, refs=target_refs)
            graph.add(target_fragment)
            projected_refs: frozenset[ResourceRef] = frozenset(
                obj.ref for obj in target_fragment.logical_objects
            )
        else:
            physical_objects = _project_physical_objects(
                db, schema, model, anchor_fragment, selection
            )
            graph.add(GraphFragment(physical_objects=physical_objects))
            projected_refs = frozenset(obj.ref for obj in physical_objects)
        selected.update(projected_refs)
        projections.append((selection, projected_refs))

    if query.expand_related:
        _expand_related(db, model, graph)

    return InspectionResult(
        schema=schema,
        read_context=FilesystemPropertyReadContext(storage_roots or {}),
        graph=graph,
        selected=frozenset(selected),
        projections=tuple(projections),
    )


def parse_inspection_selection(schema: ResourceSchema, expression: str) -> InspectionSelection:
    expression = expression.strip()
    selectable_kinds = {kind.name for kind in schema.object_kinds if schema.has_provider(kind.name)}
    if expression in selectable_kinds:
        return InspectionSelection(expression=expression, target_kind=expression)
    path = schema.property_path(expression)
    return InspectionSelection(expression, path.property.object_kind, path)


def _anchor_kind(query: InspectionQuery) -> str:
    roots = {
        selection.property_path.root_kind
        if selection.property_path is not None
        else selection.target_kind
        for selection in query.selections
    }
    if len(roots) != 1:
        raise ValueError(f"All --select expressions must currently share one root kind; got {sorted(roots)}")
    return next(iter(roots))


def _project_logical_refs(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    anchor_kind: str,
    anchor_refs: frozenset[ObjectRef],
    selection: InspectionSelection,
) -> frozenset[ObjectRef]:
    path = selection.property_path
    if path is not None and path.link_members:
        if path.root_kind != anchor_kind:
            raise ValueError(f"Explicit selection path {path} must start at query anchor {anchor_kind!r}")
        steps = tuple((link, True) for link in schema.path_links(path))
    else:
        steps = schema.belongs_to_connection(anchor_kind, selection.target_kind)
    return model.traverse_refs(db, anchor_refs, steps)


def _project_physical_objects(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    anchor_fragment: GraphFragment,
    selection: InspectionSelection,
) -> tuple[PhysicalObject, ...]:
    path = selection.property_path
    if path is None or not path.link_members:
        raise ValueError(f"Physical object kind {selection.target_kind!r} requires an explicit path")
    current: tuple[ResourceObject, ...] = anchor_fragment.logical_objects
    for link in schema.path_links(path):
        targets = tuple(target for obj in current for target in link.targets_for_source(obj))
        logical_refs = frozenset(target for target in targets if isinstance(target, ObjectRef))
        physical = tuple(target for target in targets if not isinstance(target, ObjectRef))
        if logical_refs and physical:
            raise ValueError(f"Link {link.source_kind}.{link.name} returned mixed target families")
        if logical_refs:
            fragment = model.select_objects(db, link.target_kind, refs=logical_refs)
            current = fragment.logical_objects
        else:
            current = physical
    return tuple(
        obj for obj in current if isinstance(obj, (DatabaseRowObject, LocalPathObject))
    )


def _expand_related(db: Session, model: ResourceModel, graph: ResourceGraph) -> None:
    for kind in (SESSION, PROJECT, SITE):
        keys = frozenset(int(ref.key) for ref in graph.unresolved_refs() if ref.kind == kind.name)
        if keys:
            graph.add(model.select_objects(db, kind.name, refs=_refs(kind.name, keys)))


def _refs(kind: str, keys: frozenset[int]) -> frozenset[ObjectRef]:
    return frozenset(ObjectRef(kind, str(key)) for key in keys)


def format_inspection_text(result: InspectionResult) -> str:
    if not result.selected:
        return "No matching objects."
    lines = [
        f"Selected {len(result.selected)} object(s); resolved {len(result.graph.objects)} object(s) in total."
    ]
    for obj in sorted(_visible_objects(result), key=lambda item: str(item.ref)):
        role = "selected" if obj.ref in result.selected else "related"
        lines.extend(("", f"{obj.ref} [{role}]", "  Properties:"))
        for member in _displayed_properties(result, obj):
            lines.append(f"    {member.name}: {member.get_value(obj, result.read_context)}")
        links = tuple(link for link in result.graph.links if link.source == obj.ref)
        if links:
            lines.append("  Links:")
            for link in sorted(links, key=lambda item: str(item.member)):
                definition = result.schema.link(link.member.object_kind, link.member.property_name)
                metadata = definition.lifecycle.value if definition.lifecycle is not None else "link"
                lines.append(f"    {link.member.property_name} [{metadata}] -> {link.target}")
    return "\n".join(lines)


def format_inspection_json(result: InspectionResult) -> str:
    document = {
        "selected": [str(ref) for ref in sorted(result.selected, key=str)],
        "selections": [
            {
                "expression": selection.expression,
                "property": selection.property_name,
                "objects": [str(ref) for ref in sorted(refs, key=str)],
            }
            for selection, refs in result.projections
        ],
        "objects": [
            _object_document(result, obj)
            for obj in sorted(_visible_objects(result), key=lambda item: str(item.ref))
        ],
        "links": [
            {
                "member": str(link.member),
                "source": str(link.source),
                "target": str(link.target),
                "lifecycle": (
                    definition.lifecycle.value
                    if (definition := result.schema.link(
                        link.member.object_kind, link.member.property_name
                    )).lifecycle
                    is not None
                    else None
                ),
                "target_resolved": result.graph.get(link.target) is not None,
            }
            for link in sorted(
                result.graph.links,
                key=lambda item: (str(item.member), str(item.source), str(item.target)),
            )
        ],
        "unresolved": [str(ref) for ref in sorted(result.graph.unresolved_refs())],
    }
    return json.dumps(document, indent=2, default=str)


def _object_document(result: InspectionResult, obj: ResourceObject) -> dict[str, object]:
    kind = result.schema.kind_for_object(obj)
    document: dict[str, object] = {
        "id": str(obj.ref),
        "kind": kind.name,
        "role": "selected" if obj.ref in result.selected else "related",
        "properties": {
            member.name: member.get_value(obj, result.read_context)
            for member in _displayed_properties(result, obj)
        },
    }
    if isinstance(obj.ref, ObjectRef):
        document["key"] = obj.ref.key
    if isinstance(obj, DatabaseRowObject):
        document.update(table=obj.ref.table.fullname, key=dict(obj.ref.key))
    return document


def _visible_objects(result: InspectionResult) -> tuple[ResourceObject, ...]:
    return (
        *result.graph.logical_objects,
        *(obj for obj in result.graph.physical_objects if obj.ref in result.selected),
    )


def _displayed_properties(result: InspectionResult, obj: ResourceObject):
    properties = result.schema.properties(result.schema.kind_for_object(obj).name)
    selected_names = _selected_property_names(result, obj.ref)
    return tuple(
        member for member in properties if selected_names is None or member.name in selected_names
    )


def _selected_property_names(result: InspectionResult, ref: ResourceRef) -> frozenset[str] | None:
    matching = [selection for selection, refs in result.projections if ref in refs]
    if not matching or any(selection.property_name is None for selection in matching):
        return None
    return frozenset(selection.property_name for selection in matching if selection.property_name)
