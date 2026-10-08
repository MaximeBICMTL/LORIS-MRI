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
from sqlalchemy.sql import Select

from loris_data_manager.graph import GraphFragment, ResourceGraph
from loris_data_manager.provider import (
    OrmEntityLoad,
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
    BatchLinkSource,
    LinkMember,
    LoadPolicy,
    MemberRef,
    ObjectLink,
    OrmLoadRequirement,
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
    anchor_kind: str
    primary_statement: Select[Any]
    entities: tuple["InspectionEntityPlan", ...]
    links: tuple["InspectionLinkPlan", ...]


@dataclass(frozen=True, slots=True)
class InspectionEntityPlan:
    """One logical ORM entity returned by the primary inspection statement."""

    path: tuple[str, ...]
    kind: str
    orm_load: OrmLoadRequirement
    properties: frozenset[str]


@dataclass(frozen=True, slots=True)
class InspectionLinkPlan:
    """One requested object-valued member and its source object path."""

    source_path: tuple[str, ...]
    link: LinkMember[Any]
    hydrate_target: bool


def plan_inspection(schema: ResourceSchema, query: InspectionQuery) -> InspectionPlan:
    """Plan an inspection without opening a session or executing SQL."""

    anchor_kind = _anchor_kind(query)
    entities, projection_links, links = _plan_entities(schema, query, anchor_kind)
    anchor = entities[0]
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
        links,
    )


def _plan_entities(
    schema: ResourceSchema,
    query: InspectionQuery,
    anchor_kind: str,
) -> tuple[
    tuple[InspectionEntityPlan, ...],
    tuple[LinkMember[Any], ...],
    tuple[InspectionLinkPlan, ...],
]:
    requirements: dict[tuple[str, ...], OrmLoadRequirement] = {
        (): OrmLoadRequirement()
    }
    kinds: dict[tuple[str, ...], str] = {(): anchor_kind}
    properties_by_path: dict[tuple[str, ...], set[str]] = {(): set()}
    projection_links: list[LinkMember[Any]] = []
    link_plans: list[InspectionLinkPlan] = []

    def request_link(
        source_path: tuple[str, ...],
        link: LinkMember[Any],
        *,
        hydrate_target: bool,
    ) -> None:
        for index, planned in enumerate(link_plans):
            if planned.source_path == source_path and planned.link is link:
                if hydrate_target and not planned.hydrate_target:
                    link_plans[index] = InspectionLinkPlan(source_path, link, True)
                return
        link_plans.append(InspectionLinkPlan(source_path, link, hydrate_target))

    for selection in query.selections:
        path = selection.path
        if path.root_kind != anchor_kind:
            raise ValueError(f"Explicit selection path {path} must start at query anchor {anchor_kind!r}")
        current_path: tuple[str, ...] = ()
        for link in schema.path_links(path):
            request_link(current_path, link, hydrate_target=True)
            if current_path in requirements:
                requirements[current_path] |= link.orm_load
            next_path = (*current_path, link.name)
            if (
                current_path not in requirements
                or isinstance(link.source, BatchLinkSource)
                or not link.queryable
                or not schema.has_provider(link.target_kind)
            ):
                current_path = next_path
                continue
            if link not in projection_links:
                projection_links.append(link)
            current_path = next_path
            requirements.setdefault(current_path, OrmLoadRequirement())
            kinds.setdefault(current_path, link.target_kind)
            properties_by_path.setdefault(current_path, set())
        if path.property is not None:
            if current_path in requirements:
                property_definition = schema.property(path.property)
                requirements[current_path] |= property_definition.orm_load
                properties_by_path[current_path].add(property_definition.name)
        else:
            for member in schema.properties(path.target_kind):
                if (
                    member.load_policy is LoadPolicy.DEFAULT
                    and current_path in requirements
                ):
                    requirements[current_path] |= member.orm_load
                    properties_by_path[current_path].add(member.name)
            for link in schema.links(path.target_kind):
                if link.load_policy is LoadPolicy.DEFAULT:
                    if current_path in requirements:
                        requirements[current_path] |= link.orm_load
                    request_link(current_path, link, hydrate_target=False)

    entities = tuple(
        InspectionEntityPlan(
            path=path,
            kind=kinds[path],
            orm_load=requirement,
            properties=frozenset(properties_by_path[path]),
        )
        for path, requirement in requirements.items()
    )
    return entities, tuple(projection_links), tuple(link_plans)


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
    graph = ResourceGraph()
    selected: set[ResourceRef] = set()
    rows = tuple(db.execute(plan.primary_statement).unique())
    fragments: dict[tuple[str, ...], GraphFragment] = {}
    loaded_members: set[tuple[ResourceRef, MemberRef]] = set()
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
            link_names=frozenset(),
        )
        fragments[entity.path] = fragment
        graph.add(fragment)
        loaded_members.update(
            (obj.ref, MemberRef(entity.kind, member_name))
            for obj in fragment.logical_objects
            for member_name in entity.properties
        )
    anchor_fragment = fragments[()]
    anchor_refs = frozenset(obj.ref for obj in anchor_fragment.logical_objects)
    for planned in plan.links:
        source_objects = _objects_at_path(
            schema,
            graph,
            anchor_fragment.logical_objects,
            plan.anchor_kind,
            planned.source_path,
        )
        _load_link(
            db,
            schema,
            model,
            graph,
            planned,
            source_objects,
            loaded_members,
        )
    projections: list[tuple[InspectionSelection, frozenset[ResourceRef]]] = []

    for selection in query.selections:
        projected_objects = _objects_at_path(
            schema,
            graph,
            anchor_fragment.logical_objects,
            plan.anchor_kind,
            selection.path.link_members,
        )
        projected_refs = frozenset(obj.ref for obj in projected_objects)
        if selection.path.property is not None:
            member_names = (selection.path.property.property_name,)
        else:
            member_names = tuple(
                member.name
                for member in schema.members(selection.target_kind)
                if member.load_policy is LoadPolicy.DEFAULT
            )
        loaded_members.update(
            (obj.ref, MemberRef(selection.target_kind, member_name))
            for obj in projected_objects
            for member_name in member_names
        )
        selected.update(projected_refs)
        projections.append((selection, projected_refs))

    if query.expand_related:
        _expand_related(db, schema, model, graph, loaded_members)

    return InspectionResult(
        schema=schema,
        read_context=FilesystemPropertyReadContext(storage_roots or {}),
        graph=graph,
        matched=anchor_refs,
        selected=frozenset(selected),
        projections=tuple(projections),
        loaded_members=frozenset(loaded_members),
    )


def _load_link(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    graph: ResourceGraph,
    planned: InspectionLinkPlan,
    source_objects: tuple[ResourceObject, ...],
    loaded_members: set[tuple[ResourceRef, MemberRef]],
) -> None:
    """Resolve one member for all source objects and record empty loads explicitly."""

    link = planned.link
    source_refs = frozenset(obj.ref for obj in source_objects)
    loaded_members.update(
        (ref, MemberRef(link.source_kind, link.name)) for ref in source_refs
    )
    if not source_refs:
        return

    if isinstance(link.source, BatchLinkSource):
        source = link.source
        keys = tuple(source.input_key(ref) for ref in sorted(source_refs, key=str))
        resolved: list[tuple[ResourceRef, ObjectRef | PhysicalObject]] = []
        for offset in range(0, len(keys), source.batch_size):
            batch = keys[offset : offset + source.batch_size]
            resolved.extend(
                source.target_from_row(row)
                for row in db.scalars(source.statement_for_keys(batch))
            )
    else:
        resolved = [
            (obj.ref, target)
            for obj in source_objects
            for target in link.targets_for_source(obj)
        ]

    unexpected_sources = {
        source_ref for source_ref, _ in resolved if source_ref not in source_refs
    }
    if unexpected_sources:
        raise ValueError(
            f"Link {link.source_kind}.{link.name} returned targets for unexpected sources"
        )
    target_kind = schema.object_kind(link.target_kind)
    for _, target in resolved:
        if isinstance(target, ObjectRef):
            if target.kind != link.target_kind:
                raise ValueError(
                    f"Invalid target kind for link {link.source_kind}.{link.name}"
                )
        elif not isinstance(target, target_kind.object_type):
            raise TypeError(
                f"Invalid target object for link {link.source_kind}.{link.name}"
            )
    graph.add(
        GraphFragment(
            physical_objects=tuple(
                target for _, target in resolved if not isinstance(target, ObjectRef)
            ),
            links=tuple(
                ObjectLink(
                    member=MemberRef(link.source_kind, link.name),
                    source=source_ref,
                    target=target if isinstance(target, ObjectRef) else target.ref,
                )
                for source_ref, target in resolved
            ),
        )
    )

    if not planned.hydrate_target or not schema.has_provider(link.target_kind):
        return
    unresolved = frozenset(
        target
        for _, target in resolved
        if isinstance(target, ObjectRef) and graph.get(target) is None
    )
    if not unresolved:
        return
    provider = schema.provider(link.target_kind)
    statement = model.plan_selection(
        link.target_kind,
        refs=unresolved,
        orm_load=OrmLoadRequirement(whole_entity=True),
    )
    graph.add(
        model.resolve_statement(
            db,
            provider,
            statement,
            link_names=frozenset(),
        )
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


def _objects_at_path(
    schema: ResourceSchema,
    graph: ResourceGraph,
    roots: tuple[ResourceObject, ...],
    root_kind: str,
    link_members: tuple[str, ...],
) -> tuple[ResourceObject, ...]:
    """Follow already-loaded graph links while preserving heterogeneous targets."""

    current = roots
    current_kind = root_kind
    for member_name in link_members:
        link = schema.link(current_kind, member_name)
        targets = tuple(
            target_object
            for obj in current
            for concrete in graph.outgoing(obj.ref)
            if concrete.member == MemberRef(link.source_kind, link.name)
            if (target_object := graph.get(concrete.target)) is not None
        )
        current = tuple({target.ref: target for target in targets}.values())
        current_kind = link.target_kind
    return current


def _expand_related(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    graph: ResourceGraph,
    loaded_members: set[tuple[ResourceRef, MemberRef]],
) -> None:
    for kind in (SESSION, PROJECT, SITE):
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
