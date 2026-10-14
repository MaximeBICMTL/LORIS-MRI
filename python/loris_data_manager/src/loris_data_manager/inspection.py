"""Read-only inspection of logical and physical LORIS resource objects."""

import json
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Session

from loris_data_manager.graph import ResourceGraph
from loris_data_manager.projection import (
    ProjectionPlan,
    execute_projection,
    plan_projection,
)
from loris_data_manager.provider import ResourceModel, ResourceSchema
from loris_data_manager.providers.core import CANDIDATE, PROJECT, SESSION, SITE
from loris_data_manager.resources import (
    DatabaseRowObject,
    LocalPathObject,
    LocalPathType,
    ObjectRef,
    ResourceObject,
    ResourceRef,
)
from loris_data_manager.schema import (
    BatchLinkSource,
    LinkMember,
    LoadPolicy,
    MemberRef,
    ProjectionPath,
    PropertyCriterion,
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
    path: ProjectionPath

    @property
    def target_kind(self) -> str:
        return self.path.target_kind

    @property
    def property_name(self) -> str | None:
        return None if self.path.property is None else self.path.property.property_name


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
    matched: frozenset[ObjectRef]
    selected: frozenset[ResourceRef]
    projections: tuple[tuple[InspectionSelection, frozenset[ResourceRef]], ...]
    loaded_members: frozenset[tuple[ResourceRef, MemberRef]]


@dataclass(frozen=True, slots=True)
class InspectionPlan:
    """A database-independent inspection request lowered to executable SQL."""

    schema: ResourceSchema
    query: InspectionQuery
    projection: ProjectionPlan

    @property
    def anchor_kind(self) -> str:
        return self.projection.anchor_kind

    @property
    def primary_statement(self):
        return self.projection.primary_statement

    @property
    def links(self):
        return self.projection.links


def plan_inspection(schema: ResourceSchema, query: InspectionQuery) -> InspectionPlan:
    """Plan an inspection without opening a session or executing SQL."""

    anchor_kind = _anchor_kind(query)
    projection = plan_projection(
        schema,
        anchor_kind,
        tuple(selection.path for selection in query.selections),
        criteria=query.criteria,
    )
    return InspectionPlan(schema, query, projection)


def format_inspection_sql(plan: InspectionPlan, dialect: Dialect) -> str:
    """Render the non-executing SQL plan using the configured database dialect."""

    sql = str(
        plan.primary_statement.compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    lines = [
        f"-- Step 1: select and hydrate the {plan.anchor_kind} inspection graph",
        f"{sql};",
    ]
    batch_links = tuple(
        planned for planned in plan.links if isinstance(planned.link.source, BatchLinkSource)
    )
    for index, planned in enumerate(batch_links, start=2):
        source = planned.link.source
        assert isinstance(source, BatchLinkSource)
        statement = source.statement_for_keys(bindparam(source.input_name, expanding=True))
        template = str(statement.compile(dialect=dialect)).replace(
            f"__[POSTCOMPILE_{source.input_name}]", f"__{source.input_name}"
        )
        lines.extend(
            (
                "",
                f"-- Step {index}: {source.name}",
                f"-- Input: {source.input_name} from step 1, supplied in bounded batches.",
                f"{template};",
            )
        )
    if _has_unplanned_expansion(plan):
        lines.extend(
            (
                "",
                "-- Subsequent graph hydration is runtime-dependent.",
                "-- Input: object IDs produced by step 1 (supplied in bounded batches).",
                "-- The current providers may issue collection and related-object SELECT statements.",
            )
        )
    return "\n".join(lines)


def _has_unplanned_expansion(plan: InspectionPlan) -> bool:
    return plan.query.expand_related


def inspect_resources(
    db: Session,
    schema: ResourceSchema,
    query: InspectionQuery,
    *,
    storage_roots: Mapping[str, Path] | None = None,
) -> InspectionResult:
    return execute_inspection_plan(
        db,
        plan_inspection(schema, query),
        storage_roots=storage_roots,
    )


def execute_inspection_plan(
    db: Session,
    plan: InspectionPlan,
    *,
    storage_roots: Mapping[str, Path] | None = None,
) -> InspectionResult:
    """Execute a previously constructed inspection plan."""

    schema = plan.schema
    query = plan.query
    model = ResourceModel(schema)
    projection = execute_projection(db, plan.projection)
    graph = projection.graph
    loaded_members = set(projection.loaded_members)

    if query.expand_related:
        _expand_related(db, schema, model, graph, loaded_members)

    projections = tuple(zip(query.selections, projection.projected, strict=True))
    selected = frozenset(ref for _, refs in projections for ref in refs)

    return InspectionResult(
        schema=schema,
        read_context=FilesystemPropertyReadContext(storage_roots or {}),
        graph=graph,
        matched=projection.matched,
        selected=selected,
        projections=projections,
        loaded_members=frozenset(loaded_members),
    )


def parse_inspection_selection(schema: ResourceSchema, expression: str) -> InspectionSelection:
    expression = expression.strip()
    path = schema.projection_path(expression)
    if not schema.has_provider(path.root_kind):
        raise ValueError(f"Projection root {path.root_kind!r} has no provider")
    return InspectionSelection(expression, path)


def _anchor_kind(query: InspectionQuery) -> str:
    roots = {
        selection.path.root_kind
        for selection in query.selections
    }
    if len(roots) != 1:
        raise ValueError(f"All --select expressions must currently share one root kind; got {sorted(roots)}")
    return next(iter(roots))


def _expand_related(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    graph: ResourceGraph,
    loaded_members: set[tuple[ResourceRef, MemberRef]],
) -> None:
    for kind in (SESSION, CANDIDATE, PROJECT, SITE):
        keys = frozenset(int(ref.key) for ref in graph.unresolved_refs() if ref.kind == kind.name)
        if keys:
            fragment = model.select_objects(db, kind.name, refs=_refs(kind.name, keys))
            graph.add(fragment)
            loaded_members.update(
                (obj.ref, MemberRef(kind.name, member.name))
                for obj in fragment.logical_objects
                for member in kind.members
                if member.load_policy is LoadPolicy.DEFAULT
                and not (
                    isinstance(member, LinkMember)
                    and isinstance(member.source, BatchLinkSource)
                )
            )


def _refs(kind: str, keys: frozenset[int]) -> frozenset[ObjectRef]:
    return frozenset(ObjectRef(kind, str(key)) for key in keys)


def format_inspection_text(result: InspectionResult) -> str:
    if not result.matched:
        return "No matching objects."
    if not result.selected:
        return (
            f"Matched {len(result.matched)} anchor object(s); "
            "the requested projection selected no objects."
        )
    lines = [
        f"Selected {len(result.selected)} object(s); resolved {len(result.graph.objects)} object(s) in total."
    ]
    for obj in sorted(_visible_objects(result), key=lambda item: str(item.ref)):
        role = "selected" if obj.ref in result.selected else "related"
        lines.extend(("", f"{obj.ref} [{role}]", "  Properties:"))
        for member in _displayed_properties(result, obj):
            if _property_is_loaded(result, obj, member.name):
                lines.append(f"    {member.name}: {member.get_value(obj, result.read_context)}")
            else:
                lines.append(f"    {member.name} [on-demand]: not loaded")
        links = tuple(link for link in result.graph.links if link.source == obj.ref)
        unloaded_links = _unloaded_on_demand_links(result, obj)
        if links or unloaded_links:
            lines.append("  Links:")
            for link in sorted(links, key=lambda item: str(item.member)):
                definition = result.schema.link(link.member.object_kind, link.member.member_name)
                metadata = definition.lifecycle.value if definition.lifecycle is not None else "link"
                lines.append(f"    {link.member.member_name} [{metadata}] -> {link.target}")
            for definition in unloaded_links:
                metadata = definition.lifecycle.value if definition.lifecycle is not None else "link"
                lines.append(
                    f"    {definition.name} [{metadata}; on-demand]: not loaded"
                )
    return "\n".join(lines)


def format_inspection_json(result: InspectionResult) -> str:
    document = {
        "matched": [str(ref) for ref in sorted(result.matched, key=str)],
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
                        link.member.object_kind, link.member.member_name
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
            if _property_is_loaded(result, obj, member.name)
        },
    }
    unloaded_properties = [
        member.name
        for member in _displayed_properties(result, obj)
        if not _property_is_loaded(result, obj, member.name)
    ]
    unloaded_links = [definition.name for definition in _unloaded_on_demand_links(result, obj)]
    if unloaded_properties or unloaded_links:
        document["unloaded"] = {
            "properties": unloaded_properties,
            "links": unloaded_links,
        }
    if isinstance(obj.ref, ObjectRef):
        document["key"] = obj.ref.key
    if isinstance(obj, DatabaseRowObject):
        document.update(table=obj.ref.table.fullname, key=dict(obj.ref.key))
    return document


def _visible_objects(result: InspectionResult) -> tuple[ResourceObject, ...]:
    reachable = set(result.selected)
    while True:
        targets = {
            link.target for link in result.graph.links if link.source in reachable
        }
        if targets <= reachable:
            break
        reachable.update(targets)
    return (
        *(obj for obj in result.graph.logical_objects if obj.ref in reachable),
        *(obj for obj in result.graph.physical_objects if obj.ref in result.selected),
    )


def _displayed_properties(result: InspectionResult, obj: ResourceObject):
    properties = result.schema.properties(result.schema.kind_for_object(obj).name)
    selected_names = _selected_property_names(result, obj.ref)
    return tuple(
        member for member in properties if selected_names is None or member.name in selected_names
    )


def _property_is_loaded(result: InspectionResult, obj: ResourceObject, name: str) -> bool:
    kind = result.schema.kind_for_object(obj).name
    return (obj.ref, MemberRef(kind, name)) in result.loaded_members


def _unloaded_on_demand_links(
    result: InspectionResult,
    obj: ResourceObject,
) -> tuple[LinkMember[Any], ...]:
    if not _has_whole_object_projection(result, obj.ref):
        return ()
    kind = result.schema.kind_for_object(obj).name
    return tuple(
        link
        for link in result.schema.links(kind)
        if link.load_policy is LoadPolicy.ON_DEMAND
        and (obj.ref, MemberRef(kind, link.name)) not in result.loaded_members
    )


def _has_whole_object_projection(result: InspectionResult, ref: ResourceRef) -> bool:
    return any(
        selection.property_name is None and ref in refs
        for selection, refs in result.projections
    )


def _selected_property_names(result: InspectionResult, ref: ResourceRef) -> frozenset[str] | None:
    matching = [selection for selection, refs in result.projections if ref in refs]
    if not matching or any(selection.property_name is None for selection in matching):
        return None
    return frozenset(selection.property_name for selection in matching if selection.property_name)
