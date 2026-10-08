"""Read-only inspection of logical and physical LORIS resource objects."""

import json
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import bindparam
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from loris_data_manager.graph import GraphFragment, ResourceGraph
from loris_data_manager.provider import (
    OrmEntityLoad,
    ProviderLoadStep,
    ResourceModel,
    ResourceSchema,
)
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
    LinkMember,
    LoadPolicy,
    OrmLoadRequirement,
    PropertyCriterion,
    PropertyPath,
    PropertyReadContext,
    PropertyRef,
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
    loaded_links: frozenset[tuple[ResourceRef, PropertyRef]]


@dataclass(frozen=True, slots=True)
class InspectionPlan:
    """A database-independent inspection request lowered to executable SQL."""

    schema: ResourceSchema
    query: InspectionQuery
    anchor_kind: str
    primary_statement: Select[Any]
    entities: tuple["InspectionEntityPlan", ...]
    load_steps: tuple[ProviderLoadStep, ...]


@dataclass(frozen=True, slots=True)
class InspectionEntityPlan:
    """One logical ORM entity returned by the primary inspection statement."""

    path: tuple[str, ...]
    kind: str
    orm_load: OrmLoadRequirement
    links: frozenset[str]


def plan_inspection(schema: ResourceSchema, query: InspectionQuery) -> InspectionPlan:
    """Plan an inspection without opening a session or executing SQL."""

    anchor_kind = _anchor_kind(query)
    provider = schema.provider(anchor_kind)
    entities, projection_links = _plan_entities(schema, query, anchor_kind)
    anchor = entities[0]
    deferred_links = anchor.links & provider.deferred_links
    statement = ResourceModel(schema).plan_selection(
        anchor_kind,
        criteria=query.criteria,
        orm_load=anchor.orm_load,
        projection_links=projection_links,
        related_entities=tuple(
            OrmEntityLoad(entity.kind, entity.orm_load) for entity in entities[1:]
        ),
    )
    return InspectionPlan(
        schema,
        query,
        anchor_kind,
        statement,
        entities,
        provider.load_steps(deferred_links),
    )


def _plan_entities(
    schema: ResourceSchema,
    query: InspectionQuery,
    anchor_kind: str,
) -> tuple[tuple[InspectionEntityPlan, ...], tuple[LinkMember[Any], ...]]:
    requirements: dict[tuple[str, ...], OrmLoadRequirement] = {
        (): OrmLoadRequirement()
    }
    kinds: dict[tuple[str, ...], str] = {(): anchor_kind}
    links_by_path: dict[tuple[str, ...], set[str]] = {(): set()}
    projection_links: list[LinkMember[Any]] = []

    for selection in query.selections:
        path = selection.property_path
        if path is None and selection.target_kind == anchor_kind:
            for member in schema.properties(anchor_kind):
                if member.load_policy is LoadPolicy.DEFAULT:
                    requirements[()] |= member.orm_load
            for link in schema.links(anchor_kind):
                if link.load_policy is LoadPolicy.DEFAULT:
                    requirements[()] |= link.orm_load
                    links_by_path[()].add(link.name)
            continue
        if path is None:
            raise ValueError(
                f"Selection {selection.expression!r} is not rooted at {anchor_kind!r}"
            )
        if path.root_kind != anchor_kind:
            raise ValueError(f"Explicit selection path {path} must start at query anchor {anchor_kind!r}")
        current_path: tuple[str, ...] = ()
        for link in schema.path_links(path):
            requirements[current_path] |= link.orm_load
            links_by_path[current_path].add(link.name)
            if not schema.has_provider(link.target_kind):
                continue
            if link not in projection_links:
                projection_links.append(link)
            current_path = (*current_path, link.name)
            requirements.setdefault(current_path, OrmLoadRequirement())
            kinds.setdefault(current_path, link.target_kind)
            links_by_path.setdefault(current_path, set())
        if schema.has_provider(path.property.object_kind):
            requirements[current_path] |= schema.property(path.property).orm_load

    entities = tuple(
        InspectionEntityPlan(
            path=path,
            kind=kinds[path],
            orm_load=requirement,
            links=frozenset(links_by_path[path]),
        )
        for path, requirement in requirements.items()
    )
    return entities, tuple(projection_links)


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
    for index, step in enumerate(plan.load_steps, start=2):
        statement = step.statement_for_keys(bindparam(step.input_name, expanding=True))
        template = str(statement.compile(dialect=dialect)).replace(
            f"__[POSTCOMPILE_{step.input_name}]", f"__{step.input_name}"
        )
        lines.extend(
            (
                "",
                f"-- Step {index}: {step.name}",
                f"-- Input: {step.input_name} from step 1, supplied in bounded batches.",
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
    graph = ResourceGraph()
    selected: set[ResourceRef] = set()
    rows = tuple(db.execute(plan.primary_statement).unique())
    fragments: dict[tuple[str, ...], GraphFragment] = {}
    loaded_links: set[tuple[ResourceRef, PropertyRef]] = set()
    for index, entity in enumerate(plan.entities):
        provider = schema.provider(entity.kind)
        objects_by_ref: dict[ObjectRef, Any] = {}
        for row in rows:
            orm_object = row[index]
            if orm_object is None:
                continue
            logical_object = provider.object_from_orm(orm_object)
            objects_by_ref[logical_object.ref] = logical_object
        fragment = model.fragment(
            provider,
            tuple(objects_by_ref.values()),
            link_names=entity.links - provider.deferred_links,
        )
        fragments[entity.path] = fragment
        graph.add(fragment)
        loaded_links.update(
            (obj.ref, PropertyRef(entity.kind, link_name))
            for obj in fragment.logical_objects
            for link_name in entity.links
        )
    anchor_fragment = fragments[()]
    anchor_refs = frozenset(obj.ref for obj in anchor_fragment.logical_objects)
    for step in plan.load_steps:
        step_refs = frozenset(ref for ref in anchor_refs if ref.kind == step.input_kind)
        if not step_refs:
            continue
        keys = tuple(ref.key for ref in sorted(step_refs, key=lambda ref: ref.key))
        for offset in range(0, len(keys), step.batch_size):
            batch = keys[offset : offset + step.batch_size]
            rows = tuple(db.scalars(step.statement_for_keys(batch)))
            graph.add(step.fragment_from_rows(rows))
    projections: list[tuple[InspectionSelection, frozenset[ResourceRef]]] = []

    for selection in query.selections:
        if schema.has_provider(selection.target_kind):
            target_fragment = _projected_fragment(fragments, selection)
            projected_refs: frozenset[ResourceRef] = frozenset(
                obj.ref for obj in target_fragment.logical_objects
            )
        else:
            physical_objects = _project_physical_objects(
                db, schema, model, graph, anchor_fragment, selection
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
        loaded_links=frozenset(loaded_links),
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


def _projected_fragment(
    fragments: Mapping[tuple[str, ...], GraphFragment],
    selection: InspectionSelection,
) -> GraphFragment:
    path = selection.property_path
    entity_path = () if path is None else path.link_members
    try:
        return fragments[entity_path]
    except KeyError as error:
        raise ValueError(f"Selection {selection.expression!r} was not hydrated by its plan") from error


def _project_physical_objects(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    graph: ResourceGraph,
    anchor_fragment: GraphFragment,
    selection: InspectionSelection,
) -> tuple[PhysicalObject, ...]:
    path = selection.property_path
    if path is None or not path.link_members:
        raise ValueError(f"Physical object kind {selection.target_kind!r} requires an explicit path")
    current: tuple[ResourceObject, ...] = anchor_fragment.logical_objects
    for link in schema.path_links(path):
        targets: tuple[ObjectRef | ResourceObject, ...] = tuple(
            target
            for obj in current
            for concrete in graph.outgoing(obj.ref)
            if concrete.member == PropertyRef(link.source_kind, link.name)
            for target in (
                concrete.target
                if isinstance(concrete.target, ObjectRef)
                else graph.get(concrete.target)
            ,)
            if target is not None
        )
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
        cast(PhysicalObject, obj)
        for obj in current
        if isinstance(obj, (DatabaseRowObject, LocalPathObject))
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
            if _property_is_loaded(result, obj, member.name):
                lines.append(f"    {member.name}: {member.get_value(obj, result.read_context)}")
            else:
                lines.append(f"    {member.name} [on-demand]: not loaded")
        links = tuple(link for link in result.graph.links if link.source == obj.ref)
        unloaded_links = _unloaded_on_demand_links(result, obj)
        if links or unloaded_links:
            lines.append("  Links:")
            for link in sorted(links, key=lambda item: str(item.member)):
                definition = result.schema.link(link.member.object_kind, link.member.property_name)
                metadata = definition.lifecycle.value if definition.lifecycle is not None else "link"
                lines.append(f"    {link.member.property_name} [{metadata}] -> {link.target}")
            for definition in unloaded_links:
                metadata = definition.lifecycle.value if definition.lifecycle is not None else "link"
                lines.append(
                    f"    {definition.name} [{metadata}; on-demand]: not loaded"
                )
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
    selected_names = _selected_property_names(result, obj.ref)
    if selected_names is not None:
        return name in selected_names
    kind = result.schema.kind_for_object(obj).name
    member = result.schema.property(PropertyRef(kind, name))
    return member.load_policy is LoadPolicy.DEFAULT


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
        and (obj.ref, PropertyRef(kind, link.name)) not in result.loaded_links
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
