"""Core logical object and member definitions."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from lib.resource_model.provider import ResourceSchema
from lib.resource_model.resources import DatabaseRowObject, ObjectRef, PhysicalObject
from lib.resource_model.schema import (
    BOOLEAN_TYPE,
    INTEGER_TYPE,
    STRING_TYPE,
    LifecycleSemantics,
    LinkMember,
    ObjectKind,
    ObjectSelection,
    PropertyQuery,
    RelationshipSemantics,
    SelectionConstraint,
    ValueMember,
)

SESSION_KIND = "session"
PROJECT_KIND = "project"
SITE_KIND = "site"
DATABASE_ROW_KIND = "database-row"
SourceT = TypeVar("SourceT")


@dataclass(frozen=True, slots=True)
class SessionObject:
    orm: DbSession

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(SESSION_KIND, str(self.orm.id))


SESSION_ID = ValueMember[SessionObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.id,
    query=PropertyQuery(INTEGER_TYPE, lambda query, value: query.where(DbSession.id == value)),
)
SESSION_PARTICIPANT_ID = ValueMember[SessionObject, int, int](
    name="participant-id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.candidate_id,
    query=PropertyQuery(
        INTEGER_TYPE, lambda query, value: query.where(DbSession.candidate_id == value)
    ),
)
SESSION_VISIT_LABEL = ValueMember[SessionObject, str, str](
    name="visit-label",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.visit_label,
    query=PropertyQuery(
        STRING_TYPE, lambda query, value: query.where(DbSession.visit_label == value)
    ),
)
SESSION_ACTIVE = ValueMember[SessionObject, bool, object](
    name="active",
    value_type=BOOLEAN_TYPE,
    get_value=lambda obj, _: obj.orm.active,
)


@dataclass(frozen=True, slots=True)
class ProjectObject:
    orm: DbProject

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(PROJECT_KIND, str(self.orm.id))


PROJECT_ID = ValueMember[ProjectObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.id,
    query=PropertyQuery(INTEGER_TYPE, lambda query, value: query.where(DbProject.id == value)),
)
PROJECT_NAME = ValueMember[ProjectObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.name,
    query=PropertyQuery(STRING_TYPE, lambda query, value: query.where(DbProject.name == value)),
)
PROJECT_ALIAS = ValueMember[ProjectObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.alias,
    query=PropertyQuery(STRING_TYPE, lambda query, value: query.where(DbProject.alias == value)),
)


@dataclass(frozen=True, slots=True)
class SiteObject:
    orm: DbSite

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(SITE_KIND, str(self.orm.id))


SITE_ID = ValueMember[SiteObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.id,
    query=PropertyQuery(INTEGER_TYPE, lambda query, value: query.where(DbSite.id == value)),
)
SITE_NAME = ValueMember[SiteObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.name,
    query=PropertyQuery(STRING_TYPE, lambda query, value: query.where(DbSite.name == value)),
)
SITE_ALIAS = ValueMember[SiteObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.alias,
    query=PropertyQuery(STRING_TYPE, lambda query, value: query.where(DbSite.alias == value)),
)


def _database_row_link(source_kind: str) -> LinkMember[Any]:
    return LinkMember(
        name="row",
        source_kind=source_kind,
        target_kind=DATABASE_ROW_KIND,
        targets_for_source=lambda obj: (DatabaseRowObject(obj.orm),),
        lifecycle=LifecycleSemantics.OWNS,
    )


SESSION_PROJECT = LinkMember[SessionObject](
    name="project",
    source_kind=SESSION_KIND,
    target_kind=PROJECT_KIND,
    targets_for_source=lambda obj: (ObjectRef(PROJECT_KIND, str(obj.orm.project_id)),),
    source_selection=lambda refs: ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: query.where(DbSession.project_id.in_(_target_ids(refs, PROJECT_KIND)))
            ),
        )
    ),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
SESSION_SITE = LinkMember[SessionObject](
    name="site",
    source_kind=SESSION_KIND,
    target_kind=SITE_KIND,
    targets_for_source=lambda obj: (ObjectRef(SITE_KIND, str(obj.orm.site_id)),),
    source_selection=lambda refs: ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: query.where(DbSession.site_id.in_(_target_ids(refs, SITE_KIND)))
            ),
        )
    ),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)

DATABASE_ROW = ObjectKind(DATABASE_ROW_KIND, DatabaseRowObject, ())
PROJECT = ObjectKind(
    PROJECT_KIND,
    ProjectObject,
    (PROJECT_ID, PROJECT_NAME, PROJECT_ALIAS, _database_row_link(PROJECT_KIND)),
)
SITE = ObjectKind(
    SITE_KIND,
    SiteObject,
    (SITE_ID, SITE_NAME, SITE_ALIAS, _database_row_link(SITE_KIND)),
)
SESSION = ObjectKind(
    SESSION_KIND,
    SessionObject,
    (
        SESSION_ID,
        SESSION_PARTICIPANT_ID,
        SESSION_VISIT_LABEL,
        SESSION_ACTIVE,
        SESSION_PROJECT,
        SESSION_SITE,
        _database_row_link(SESSION_KIND),
    ),
)


class SessionProvider:
    kind = SESSION

    def find(self, db: Session, selection: ObjectSelection[SessionObject]) -> tuple[SessionObject, ...]:
        statement = select(DbSession)
        keys = selection.keys_for(SESSION.name)
        if keys is not None:
            statement = statement.where(DbSession.id.in_(_integer_keys(keys, SESSION.name)))
        statement = selection.apply_filters(statement, _value_members(SESSION))
        return tuple(SessionObject(row) for row in db.scalars(statement))


class ProjectProvider:
    kind = PROJECT

    def find(self, db: Session, selection: ObjectSelection[ProjectObject]) -> tuple[ProjectObject, ...]:
        statement = select(DbProject)
        keys = selection.keys_for(PROJECT.name)
        if keys is not None:
            statement = statement.where(DbProject.id.in_(_integer_keys(keys, PROJECT.name)))
        statement = selection.apply_filters(statement, _value_members(PROJECT))
        return tuple(ProjectObject(row) for row in db.scalars(statement))


class SiteProvider:
    kind = SITE

    def find(self, db: Session, selection: ObjectSelection[SiteObject]) -> tuple[SiteObject, ...]:
        statement = select(DbSite)
        keys = selection.keys_for(SITE.name)
        if keys is not None:
            statement = statement.where(DbSite.id.in_(_integer_keys(keys, SITE.name)))
        statement = selection.apply_filters(statement, _value_members(SITE))
        return tuple(SiteObject(row) for row in db.scalars(statement))


def _value_members(kind: ObjectKind) -> tuple[ValueMember[Any, Any, Any], ...]:
    return tuple(member for member in kind.members if isinstance(member, ValueMember))


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def _target_ids(refs: frozenset[ObjectRef], kind: str) -> frozenset[int]:
    wrong_kinds = {ref.kind for ref in refs if ref.kind != kind}
    if wrong_kinds:
        raise ValueError(f"Expected {kind!r} references, got kinds {sorted(wrong_kinds)}")
    return _integer_keys(frozenset(ref.key for ref in refs), kind)


def register_core_schema(schema: ResourceSchema) -> None:
    schema.register_object_kind(DATABASE_ROW)
    schema.register_object_kind(PROJECT)
    schema.register_object_kind(SITE)
    schema.register_object_kind(SESSION)
    schema.register_provider(SessionProvider())
    schema.register_provider(ProjectProvider())
    schema.register_provider(SiteProvider())


def session_link(
    *,
    source_kind: str,
    targets_for_source: Callable[[SourceT], tuple[ObjectRef | PhysicalObject, ...]],
    source_selection: Callable[[frozenset[ObjectRef]], ObjectSelection[Any]],
) -> LinkMember[SourceT]:
    """Define the conventional object-valued member from a module object to a session."""

    return LinkMember(
        name="session",
        source_kind=source_kind,
        target_kind=SESSION.name,
        targets_for_source=targets_for_source,
        source_selection=source_selection,
        traversal_semantics=RelationshipSemantics.BELONGS_TO,
        lifecycle=LifecycleSemantics.REFERENCES,
        target_may_be_shared=True,
    )
