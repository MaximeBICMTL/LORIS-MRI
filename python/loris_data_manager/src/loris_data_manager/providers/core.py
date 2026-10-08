"""Core logical object and member definitions."""

from dataclasses import dataclass
from typing import Any

from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from loris_data_manager.provider import ResourceSchema
from loris_data_manager.resources import DatabaseRowObject, ObjectRef
from loris_data_manager.schema import (
    BOOLEAN_TYPE,
    INTEGER_TYPE,
    STRING_TYPE,
    LifecycleSemantics,
    LinkMember,
    ObjectKind,
    ObjectSelection,
    OrmColumnSource,
    OrmEntitySource,
    OrmForeignKeySource,
    PropertyQuery,
    RelationshipSemantics,
    ValueMember,
)

SESSION_KIND = "session"
PROJECT_KIND = "project"
SITE_KIND = "site"
DATABASE_ROW_KIND = "database-row"


@dataclass(frozen=True, slots=True)
class SessionObject:
    orm: DbSession

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(SESSION_KIND, str(self.orm.id))


SESSION_ID = ValueMember[SessionObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbSession.id),
    query=PropertyQuery(INTEGER_TYPE),
)
SESSION_PARTICIPANT_ID = ValueMember[SessionObject, int, int](
    name="participant-id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbSession.candidate_id),
    query=PropertyQuery(INTEGER_TYPE),
)
SESSION_VISIT_LABEL = ValueMember[SessionObject, str, str](
    name="visit-label",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbSession.visit_label),
    query=PropertyQuery(STRING_TYPE),
)
SESSION_ACTIVE = ValueMember[SessionObject, bool, object](
    name="active",
    value_type=BOOLEAN_TYPE,
    source=OrmColumnSource(DbSession.active),
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
    source=OrmColumnSource(DbProject.id),
    query=PropertyQuery(INTEGER_TYPE),
)
PROJECT_NAME = ValueMember[ProjectObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbProject.name),
    query=PropertyQuery(STRING_TYPE),
)
PROJECT_ALIAS = ValueMember[ProjectObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbProject.alias),
    query=PropertyQuery(STRING_TYPE),
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
    source=OrmColumnSource(DbSite.id),
    query=PropertyQuery(INTEGER_TYPE),
)
SITE_NAME = ValueMember[SiteObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbSite.name),
    query=PropertyQuery(STRING_TYPE),
)
SITE_ALIAS = ValueMember[SiteObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbSite.alias),
    query=PropertyQuery(STRING_TYPE),
)


def _database_row_link(source_kind: str) -> LinkMember[Any]:
    return LinkMember(
        name="row",
        source_kind=source_kind,
        target_kind=DATABASE_ROW_KIND,
        source=OrmEntitySource(),
        lifecycle=LifecycleSemantics.OWNS,
    )


SESSION_PROJECT = LinkMember[SessionObject](
    name="project",
    source_kind=SESSION_KIND,
    target_kind=PROJECT_KIND,
    source=OrmForeignKeySource(DbSession.project_id, DbProject.id),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
SESSION_SITE = LinkMember[SessionObject](
    name="site",
    source_kind=SESSION_KIND,
    target_kind=SITE_KIND,
    source=OrmForeignKeySource(DbSession.site_id, DbSite.id),
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
    orm_model = DbSession
    deferred_links = frozenset[str]()

    def statement(self, selection: ObjectSelection[SessionObject]):
        statement = select(DbSession)
        keys = selection.keys_for(SESSION.name)
        if keys is not None:
            statement = statement.where(DbSession.id.in_(_integer_keys(keys, SESSION.name)))
        return selection.apply_filters(statement, _value_members(SESSION))

    def object_from_orm(self, row: DbSession) -> SessionObject:
        return SessionObject(row)

    def load_steps(self, links: frozenset[str]):
        return ()

    def find(self, db: Session, selection: ObjectSelection[SessionObject]) -> tuple[SessionObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class ProjectProvider:
    kind = PROJECT
    orm_model = DbProject
    deferred_links = frozenset[str]()

    def statement(self, selection: ObjectSelection[ProjectObject]):
        statement = select(DbProject)
        keys = selection.keys_for(PROJECT.name)
        if keys is not None:
            statement = statement.where(DbProject.id.in_(_integer_keys(keys, PROJECT.name)))
        return selection.apply_filters(statement, _value_members(PROJECT))

    def object_from_orm(self, row: DbProject) -> ProjectObject:
        return ProjectObject(row)

    def load_steps(self, links: frozenset[str]):
        return ()

    def find(self, db: Session, selection: ObjectSelection[ProjectObject]) -> tuple[ProjectObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class SiteProvider:
    kind = SITE
    orm_model = DbSite
    deferred_links = frozenset[str]()

    def statement(self, selection: ObjectSelection[SiteObject]):
        statement = select(DbSite)
        keys = selection.keys_for(SITE.name)
        if keys is not None:
            statement = statement.where(DbSite.id.in_(_integer_keys(keys, SITE.name)))
        return selection.apply_filters(statement, _value_members(SITE))

    def object_from_orm(self, row: DbSite) -> SiteObject:
        return SiteObject(row)

    def load_steps(self, links: frozenset[str]):
        return ()

    def find(self, db: Session, selection: ObjectSelection[SiteObject]) -> tuple[SiteObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


def _value_members(kind: ObjectKind) -> tuple[ValueMember[Any, Any, Any], ...]:
    return tuple(member for member in kind.members if isinstance(member, ValueMember))


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


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
    source_attribute: InstrumentedAttribute[Any],
) -> LinkMember[Any]:
    """Define the conventional object-valued member from a module object to a session."""

    return LinkMember(
        name="session",
        source_kind=source_kind,
        target_kind=SESSION.name,
        source=OrmForeignKeySource(source_attribute, DbSession.id),
        traversal_semantics=RelationshipSemantics.BELONGS_TO,
        lifecycle=LifecycleSemantics.REFERENCES,
        target_may_be_shared=True,
    )
