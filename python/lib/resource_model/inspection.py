"""Read-only inspection of logical LORIS objects and their bound resources."""

import json
from dataclasses import dataclass

from sqlalchemy.orm import Session

from lib.resource_model.graph import ResourceGraph
from lib.resource_model.provider import ResourceModel, ResourceSchema
from lib.resource_model.providers.core import (
    PROJECT,
    SESSION,
    SITE,
    register_core_schema,
)
from lib.resource_model.providers.dicom import register_dicom_schema
from lib.resource_model.resources import DatabaseRowObject, LocalPathObject, ResourceObject
from lib.resource_model.schema import (
    LogicalObject,
    ObjectRef,
    PropertyCriterion,
    PropertyPath,
    Relationship,
)


@dataclass(frozen=True, slots=True)
class InspectionSelection:
    """A whole logical object kind or one property to project."""

    expression: str
    target_kind: str
    property_path: PropertyPath | None = None

    @property
    def property_name(self) -> str | None:
        return None if self.property_path is None else self.property_path.property.property_name


@dataclass(frozen=True, slots=True)
class InspectionQuery:
    """Object/property projections and qualified criteria accepted by the inspector."""

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
    graph: ResourceGraph
    selected: frozenset[ObjectRef]
    projections: tuple[tuple[InspectionSelection, frozenset[ObjectRef]], ...]


def inspect_resources(db: Session, query: InspectionQuery) -> InspectionResult:
    """Resolve objects and resources selected by a read-only inspection query."""

    schema = make_resource_schema()
    model = ResourceModel(schema)
    graph = ResourceGraph()
    selected: set[ObjectRef] = set()
    anchor_kind = _anchor_kind(query)
    anchor_fragment = model.select_objects(db, anchor_kind, criteria=query.criteria)
    anchor_refs = frozenset(
        logical_object.ref for logical_object in anchor_fragment.logical_objects
    )
    projections: list[tuple[InspectionSelection, frozenset[ObjectRef]]] = []

    for selection in query.selections:
        target_refs = _project_refs(db, schema, model, anchor_kind, anchor_refs, selection)
        target_fragment = model.select_objects(db, selection.target_kind, refs=target_refs)
        graph.add(target_fragment)
        projected_refs = frozenset(
            logical_object.ref for logical_object in target_fragment.logical_objects
        )
        selected.update(projected_refs)
        projections.append((selection, projected_refs))

    if query.expand_related:
        missing_session_ids = frozenset(int(ref.key) for ref in graph.unresolved_refs() if ref.kind == SESSION.name)
        if missing_session_ids:
            graph.add(
                model.select_objects(
                    db,
                    SESSION.name,
                    refs=_refs(SESSION.name, missing_session_ids),
                )
            )
        missing_project_ids = frozenset(int(ref.key) for ref in graph.unresolved_refs() if ref.kind == PROJECT.name)
        if missing_project_ids:
            graph.add(
                model.select_objects(
                    db,
                    PROJECT.name,
                    refs=_refs(PROJECT.name, missing_project_ids),
                )
            )
        missing_site_ids = frozenset(int(ref.key) for ref in graph.unresolved_refs() if ref.kind == SITE.name)
        if missing_site_ids:
            graph.add(
                model.select_objects(
                    db,
                    SITE.name,
                    refs=_refs(SITE.name, missing_site_ids),
                )
            )

    return InspectionResult(
        graph=graph,
        selected=frozenset(selected),
        projections=tuple(projections),
    )


def parse_inspection_selection(schema: ResourceSchema, expression: str) -> InspectionSelection:
    """Parse a whole-object or property projection through the resource schema."""

    expression = expression.strip()
    object_kinds = {kind.name for kind in schema.object_kinds}
    if expression in object_kinds:
        return InspectionSelection(expression=expression, target_kind=expression)
    path = schema.property_path(expression)
    return InspectionSelection(
        expression=expression,
        target_kind=path.property.object_kind,
        property_path=path,
    )


def _anchor_kind(query: InspectionQuery) -> str:
    selection_roots = {
        selection.property_path.root_kind
        if selection.property_path is not None
        else selection.target_kind
        for selection in query.selections
    }
    if len(selection_roots) != 1:
        raise ValueError(
            f"All --select expressions must currently share one root kind; "
            f"got {sorted(selection_roots)}"
        )
    return next(iter(selection_roots))


def _project_refs(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    anchor_kind: str,
    anchor_refs: frozenset[ObjectRef],
    selection: InspectionSelection,
) -> frozenset[ObjectRef]:
    path = selection.property_path
    if path is not None and path.relationship_members:
        if path.root_kind != anchor_kind:
            raise ValueError(
                f"Explicit selection path {path} must start at query anchor {anchor_kind!r}"
            )
        steps = tuple((binding, True) for binding in schema.path_bindings(path))
    else:
        steps = schema.belongs_to_connection(anchor_kind, selection.target_kind)
    return model.traverse_refs(db, anchor_refs, steps)


def make_resource_schema() -> ResourceSchema:
    """Build the resource schema used by the proof-of-concept inspector."""

    schema = ResourceSchema()
    register_core_schema(schema)
    register_dicom_schema(schema)
    return schema


def _refs(kind: str, keys: frozenset[int] | None) -> frozenset[ObjectRef] | None:
    if keys is None:
        return None
    return frozenset(ObjectRef(kind, str(key)) for key in keys)


def format_inspection_text(result: InspectionResult) -> str:
    """Render an inspection result for a terminal."""

    if not result.selected:
        return "No matching logical objects."

    lines = [
        f"Selected {len(result.selected)} logical object(s); resolved {len(result.graph.objects)} object(s) in total."
    ]
    objects = sorted(result.graph.logical_objects, key=lambda item: str(item.ref))
    for logical_object in objects:
        role = "selected" if logical_object.ref in result.selected else "related"
        lines.extend(("", f"{logical_object.ref} [{role}]", "  Properties:"))
        selected_property_names = _selected_property_names(result, logical_object.ref)
        for property_definition in logical_object.properties:
            if (
                selected_property_names is not None
                and property_definition.name not in selected_property_names
            ):
                continue
            value = property_definition.get_value(logical_object)
            lines.append(f"    {property_definition.name}: {value}")

        if selected_property_names is not None:
            continue

        lines.append("  Resources:")
        bound_resources = _bound_resources(result.graph, logical_object)
        if not bound_resources:
            lines.append("    (none)")
        for semantics, resource in bound_resources:
            if isinstance(resource, DatabaseRowObject):
                key = ", ".join(f"{name}={value}" for name, value in resource.ref.key)
                lines.append(
                    f"    {semantics}: database row: {resource.ref.table.fullname} ({key})"
                )
            elif isinstance(resource, LocalPathObject):
                lines.append(
                    f"    {semantics}: local {resource.expected_type}: "
                    f"{resource.storage_root}:{resource.relative_path}"
                )
            else:
                raise TypeError(f"Unsupported physical resource object {resource!r}")

        relationships = sorted(
            _object_relationships(result.graph, logical_object),
            key=lambda item: (item.kind, item.source, item.target),
        )
        lines.append("  Relationships:")
        if not relationships:
            lines.append("    (none)")
        for relationship in relationships:
            direction = "->" if relationship.source == logical_object.ref else "<-"
            other = relationship.target if direction == "->" else relationship.source
            resolved = "resolved" if result.graph.get(other) is not None else "unresolved"
            lines.append(f"    {direction} {relationship.kind}: {other} [{resolved}]")

    return "\n".join(lines)


def format_inspection_json(result: InspectionResult) -> str:
    """Render a stable machine-readable representation of an inspection result."""

    objects = sorted(result.graph.logical_objects, key=lambda item: str(item.ref))
    document = {
        "selected": [str(ref) for ref in sorted(result.selected)],
        "selections": [
            {
                "expression": selection.expression,
                "property": selection.property_name,
                "objects": [str(ref) for ref in sorted(refs)],
            }
            for selection, refs in result.projections
        ],
        "objects": [
            {
                "id": str(logical_object.ref),
                "kind": logical_object.ref.kind,
                "key": logical_object.ref.key,
                "role": "selected" if logical_object.ref in result.selected else "related",
                "properties": _property_document(result, logical_object),
                "resources": (
                    [
                        {"semantics": str(semantics), **_resource_document(resource)}
                        for semantics, resource in _bound_resources(result.graph, logical_object)
                    ]
                    if _selected_property_names(result, logical_object.ref) is None
                    else []
                ),
            }
            for logical_object in objects
        ],
        "relationships": [
            {
                "kind": relationship.kind,
                "source": str(relationship.source),
                "target": str(relationship.target),
                "target_resolved": result.graph.get(relationship.target) is not None,
            }
            for relationship in sorted(
                result.graph.relationships,
                key=lambda item: (item.kind, item.source, item.target),
            )
        ],
        "unresolved": [str(ref) for ref in sorted(result.graph.unresolved_refs())],
    }
    return json.dumps(document, indent=2, default=str)


def _object_relationships(graph: ResourceGraph, logical_object: LogicalObject) -> tuple[Relationship, ...]:
    return tuple(
        relationship
        for relationship in graph.relationships
        if logical_object.ref in (relationship.source, relationship.target)
    )


def _selected_property_names(
    result: InspectionResult,
    ref: ObjectRef,
) -> frozenset[str] | None:
    """Return a property projection, or None for whole/related objects."""

    matching = [
        selection
        for selection, refs in result.projections
        if ref in refs
    ]
    if not matching or any(selection.property_name is None for selection in matching):
        return None
    return frozenset(
        selection.property_name
        for selection in matching
        if selection.property_name is not None
    )


def _property_document(
    result: InspectionResult,
    logical_object: LogicalObject,
) -> dict[str, object]:
    selected_names = _selected_property_names(result, logical_object.ref)
    return {
        property_definition.name: property_definition.get_value(logical_object)
        for property_definition in logical_object.properties
        if selected_names is None or property_definition.name in selected_names
    }


def _bound_resources(
    graph: ResourceGraph,
    logical_object: LogicalObject,
) -> tuple[tuple[str, ResourceObject], ...]:
    resources: list[tuple[str, ResourceObject]] = []
    for binding in graph.resource_bindings:
        if binding.source != logical_object.ref:
            continue
        resource = graph.get(binding.target)
        if resource is None:
            raise ValueError(f"Unresolved physical resource {binding.target}")
        resources.append((binding.semantics.value, resource))
    return tuple(sorted(resources, key=lambda item: str(item[1].ref)))


def _resource_document(resource: ResourceObject) -> dict[str, object]:
    if isinstance(resource, DatabaseRowObject):
        return {
            "type": "database_row",
            "id": str(resource.ref),
            "table": resource.ref.table.fullname,
            "key": dict(resource.ref.key),
        }
    if not isinstance(resource, LocalPathObject):
        raise TypeError(f"Unsupported physical resource object {resource!r}")
    return {
        "type": "local_path",
        "id": str(resource.ref),
        "storage_root": resource.storage_root,
        "path": str(resource.relative_path),
        "expected_type": resource.expected_type,
    }
