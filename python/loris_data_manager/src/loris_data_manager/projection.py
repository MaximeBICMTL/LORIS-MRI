"""Generic planning and execution of semantic resource projections."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from loris_data_manager.graph import GraphFragment, ResourceGraph
from loris_data_manager.provider import OrmEntityLoad, ResourceModel, ResourceSchema
from loris_data_manager.resources import ObjectRef, PhysicalObject, ResourceObject, ResourceRef
from loris_data_manager.schema import (
    BatchLinkSource,
    LinkMember,
    LoadPolicy,
    MemberRef,
    ObjectLink,
    OrmLoadRequirement,
    ProjectionPath,
    PropertyCriterion,
)


@dataclass(frozen=True, slots=True)
class ResourceSelection:
    """One schema-resolved object or property projection."""

    expression: str
    path: ProjectionPath

    @property
    def target_kind(self) -> str:
        return self.path.target_kind

    @property
    def property_name(self) -> str | None:
        return None if self.path.property is None else self.path.property.property_name


@dataclass(frozen=True, slots=True)
class ResourceQuery:
    """Shared semantic selection consumed by resource operations."""

    selections: tuple[ResourceSelection, ...]
    criteria: tuple[PropertyCriterion, ...] = ()
    select_all: bool = False

    def __post_init__(self) -> None:
        if not self.selections:
            raise ValueError("At least one object or property must be selected")
        if not self.select_all and not self.criteria:
            raise ValueError("At least one selector is required unless select_all is enabled")

    @property
    def anchor_kind(self) -> str:
        roots = {selection.path.root_kind for selection in self.selections}
        if len(roots) != 1:
            raise ValueError(
                "All --select expressions must currently share one root kind; "
                f"got {sorted(roots)}"
            )
        return next(iter(roots))


def parse_resource_selection(schema: ResourceSchema, expression: str) -> ResourceSelection:
    """Resolve a public selection expression through the semantic schema."""

    expression = expression.strip()
    path = schema.projection_path(expression)
    if not schema.has_provider(path.root_kind):
        raise ValueError(f"Projection root {path.root_kind!r} has no provider")
    return ResourceSelection(expression, path)


@dataclass(frozen=True, slots=True)
class ProjectionEntityPlan:
    """One logical ORM entity returned by the primary projection statement."""

    path: tuple[str, ...]
    kind: str
    orm_load: OrmLoadRequirement
    properties: frozenset[str]


@dataclass(frozen=True, slots=True)
class ProjectionLinkPlan:
    """One requested object-valued member and its source object path."""

    source_path: tuple[str, ...]
    link: LinkMember[Any]
    hydrate_target: bool


@dataclass(frozen=True, slots=True)
class ProjectionPlan:
    """Database-independent projection paths lowered to executable SQL."""

    schema: ResourceSchema
    anchor_kind: str
    paths: tuple[ProjectionPath, ...]
    primary_statement: Select[Any]
    entities: tuple[ProjectionEntityPlan, ...]
    links: tuple[ProjectionLinkPlan, ...]


@dataclass(frozen=True, slots=True)
class ProjectionResult:
    """A resolved projection graph and the members known to be loaded."""

    graph: ResourceGraph
    matched: frozenset[ObjectRef]
    projected: tuple[frozenset[ResourceRef], ...]
    loaded_members: frozenset[tuple[ResourceRef, MemberRef]]


def plan_projection(
    schema: ResourceSchema,
    anchor_kind: str,
    paths: tuple[ProjectionPath, ...],
    *,
    criteria: tuple[PropertyCriterion, ...] = (),
    include_default_members: bool = True,
) -> ProjectionPlan:
    """Plan semantic projections without opening a database session."""

    entities, projection_links, links = _plan_entities(
        schema,
        anchor_kind,
        paths,
        include_default_members=include_default_members,
    )
    anchor = entities[0]
    statement = ResourceModel(schema).plan_selection(
        anchor_kind,
        criteria=criteria,
        orm_load=anchor.orm_load,
        projection_links=projection_links,
        related_entities=tuple(
            OrmEntityLoad(entity.kind, entity.orm_load) for entity in entities[1:]
        ),
    )
    return ProjectionPlan(schema, anchor_kind, paths, statement, entities, links)


def plan_resource_query(
    schema: ResourceSchema,
    query: ResourceQuery,
    *,
    include_default_members: bool = True,
) -> ProjectionPlan:
    """Lower a shared semantic query to one projection plan."""

    return plan_projection(
        schema,
        query.anchor_kind,
        tuple(selection.path for selection in query.selections),
        criteria=query.criteria,
        include_default_members=include_default_members,
    )


def _plan_entities(
    schema: ResourceSchema,
    anchor_kind: str,
    paths: tuple[ProjectionPath, ...],
    *,
    include_default_members: bool,
) -> tuple[
    tuple[ProjectionEntityPlan, ...],
    tuple[LinkMember[Any], ...],
    tuple[ProjectionLinkPlan, ...],
]:
    requirements: dict[tuple[str, ...], OrmLoadRequirement] = {(): OrmLoadRequirement()}
    kinds: dict[tuple[str, ...], str] = {(): anchor_kind}
    properties_by_path: dict[tuple[str, ...], set[str]] = {(): set()}
    projection_links: list[LinkMember[Any]] = []
    link_plans: list[ProjectionLinkPlan] = []

    def request_link(
        source_path: tuple[str, ...],
        link: LinkMember[Any],
        *,
        hydrate_target: bool,
    ) -> None:
        for index, planned in enumerate(link_plans):
            if planned.source_path == source_path and planned.link is link:
                if hydrate_target and not planned.hydrate_target:
                    link_plans[index] = ProjectionLinkPlan(source_path, link, True)
                return
        link_plans.append(ProjectionLinkPlan(source_path, link, hydrate_target))

    for path in paths:
        if path.root_kind != anchor_kind:
            raise ValueError(f"Projection path {path} must start at query anchor {anchor_kind!r}")
        current_path: tuple[str, ...] = ()
        for link in schema.path_links(path):
            request_link(current_path, link, hydrate_target=True)
            if current_path in requirements:
                requirements[current_path] |= link.orm_load
            next_path: tuple[str, ...] = (*current_path, link.name)
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
        elif include_default_members:
            for member in schema.properties(path.target_kind):
                if member.load_policy is LoadPolicy.DEFAULT and current_path in requirements:
                    requirements[current_path] |= member.orm_load
                    properties_by_path[current_path].add(member.name)
            for link in schema.links(path.target_kind):
                if link.load_policy is LoadPolicy.DEFAULT:
                    if current_path in requirements:
                        requirements[current_path] |= link.orm_load
                    request_link(current_path, link, hydrate_target=False)

    entities = tuple(
        ProjectionEntityPlan(
            path=path,
            kind=kinds[path],
            orm_load=requirement,
            properties=frozenset(properties_by_path[path]),
        )
        for path, requirement in requirements.items()
    )
    return entities, tuple(projection_links), tuple(link_plans)


def execute_projection(
    db: Session,
    plan: ProjectionPlan,
    *,
    lock_rows: bool = False,
) -> ProjectionResult:
    """Execute a projection plan while keeping resolved ORM objects session-bound."""

    schema = plan.schema
    model = ResourceModel(schema)
    graph = ResourceGraph()
    primary_statement = (
        plan.primary_statement.with_for_update()
        if lock_rows
        else plan.primary_statement
    )
    rows = tuple(db.execute(primary_statement).unique())
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
        fragment = model.fragment(provider, tuple(objects_by_ref.values()), link_names=frozenset())
        fragments[entity.path] = fragment
        graph.add(fragment)
        loaded_members.update(
            (obj.ref, MemberRef(entity.kind, member_name))
            for obj in fragment.logical_objects
            for member_name in entity.properties
        )

    anchor_fragment = fragments[()]
    for planned in plan.links:
        source_objects = objects_at_path(
            schema,
            graph,
            anchor_fragment.logical_objects,
            plan.anchor_kind,
            planned.source_path,
        )
        joined_targets: list[tuple[ResourceRef, ObjectRef | PhysicalObject]] | None = None
        target_path = (*planned.source_path, planned.link.name)
        if target_path in fragments:
            source_index = next(
                index for index, entity in enumerate(plan.entities) if entity.path == planned.source_path
            )
            target_index = next(
                index for index, entity in enumerate(plan.entities) if entity.path == target_path
            )
            source_provider = schema.provider(planned.link.source_kind)
            target_provider = schema.provider(planned.link.target_kind)
            joined_targets = []
            targets_by_source: dict[ResourceRef, set[ObjectRef]] = {}
            for row in rows:
                source_orm = row[source_index]
                target_orm = row[target_index]
                if source_orm is None or target_orm is None:
                    continue
                source_ref = source_provider.object_from_orm(source_orm).ref
                target_ref = target_provider.object_from_orm(target_orm).ref
                targets_by_source.setdefault(source_ref, set()).add(target_ref)
            joined_targets.extend(
                (source_ref, target_ref)
                for source_ref, targets in targets_by_source.items()
                for target_ref in targets
            )
        _load_link(
            db,
            schema,
            model,
            graph,
            planned,
            source_objects,
            loaded_members,
            lock_rows=lock_rows,
            resolved_targets=joined_targets,
        )

    projected: list[frozenset[ResourceRef]] = []
    for path in plan.paths:
        objects = objects_at_path(
            schema,
            graph,
            anchor_fragment.logical_objects,
            plan.anchor_kind,
            path.link_members,
        )
        refs = frozenset(obj.ref for obj in objects)
        if path.property is not None:
            member_names = (path.property.property_name,)
        else:
            member_names = tuple(
                member.name
                for member in schema.members(path.target_kind)
                if member.load_policy is LoadPolicy.DEFAULT
            )
        loaded_members.update(
            (obj.ref, MemberRef(path.target_kind, member_name))
            for obj in objects
            for member_name in member_names
        )
        projected.append(refs)

    return ProjectionResult(
        graph=graph,
        matched=frozenset(obj.ref for obj in anchor_fragment.logical_objects),
        projected=tuple(projected),
        loaded_members=frozenset(loaded_members),
    )


def objects_at_path(
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


def _load_link(
    db: Session,
    schema: ResourceSchema,
    model: ResourceModel,
    graph: ResourceGraph,
    planned: ProjectionLinkPlan,
    source_objects: tuple[ResourceObject, ...],
    loaded_members: set[tuple[ResourceRef, MemberRef]],
    *,
    lock_rows: bool,
    resolved_targets: list[tuple[ResourceRef, ObjectRef | PhysicalObject]] | None = None,
) -> None:
    link = planned.link
    source_refs = frozenset(obj.ref for obj in source_objects)
    loaded_members.update((ref, MemberRef(link.source_kind, link.name)) for ref in source_refs)
    if not source_refs:
        return

    if resolved_targets is not None:
        resolved = resolved_targets
    elif isinstance(link.source, BatchLinkSource):
        source = link.source
        keys = tuple(source.input_key(ref) for ref in sorted(source_refs, key=str))
        resolved: list[tuple[ResourceRef, ObjectRef | PhysicalObject]] = []
        for offset in range(0, len(keys), source.batch_size):
            batch = keys[offset : offset + source.batch_size]
            statement = source.statement_for_keys(batch)
            if lock_rows:
                statement = statement.with_for_update()
            resolved.extend(
                source.target_from_row(row)
                for row in db.scalars(statement)
            )
    else:
        resolved = [
            (obj.ref, target)
            for obj in source_objects
            for target in link.targets_for_source(obj)
        ]

    unexpected_sources = {source_ref for source_ref, _ in resolved if source_ref not in source_refs}
    if unexpected_sources:
        raise ValueError(f"Link {link.source_kind}.{link.name} returned targets for unexpected sources")
    target_kind = schema.object_kind(link.target_kind)
    for _, target in resolved:
        if isinstance(target, ObjectRef):
            if target.kind != link.target_kind:
                raise ValueError(f"Invalid target kind for link {link.source_kind}.{link.name}")
        elif not isinstance(target, target_kind.object_type):
            raise TypeError(f"Invalid target object for link {link.source_kind}.{link.name}")
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
    if lock_rows:
        statement = statement.with_for_update()
    graph.add(model.resolve_statement(db, provider, statement, link_names=frozenset()))
