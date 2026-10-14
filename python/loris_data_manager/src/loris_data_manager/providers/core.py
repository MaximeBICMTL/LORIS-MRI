"""Core logical object and member definitions."""

from dataclasses import dataclass
from typing import Any

from lib.db.models.candidate import DbCandidate
from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from lib.db.models.visit import DbVisit
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
    LoadPolicy,
    ObjectKind,
    ObjectSelection,
    OrmColumnSource,
    OrmEntitySource,
    OrmMappedLinkSource,
    PropertyQuery,
    RelationshipSemantics,
    ValueMember,
)

SESSION_KIND = "session"
CANDIDATE_KIND = "candidate"
VISIT_KIND = "visit"
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
SESSION_VISIT_LABEL = ValueMember[SessionObject, str, str](
    name="visit-label",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbSession.visit_label),
    query=PropertyQuery(STRING_TYPE),
)


@dataclass(frozen=True, slots=True)
class CandidateObject:
    orm: DbCandidate

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(CANDIDATE_KIND, str(self.orm.id))


CANDIDATE_ID = ValueMember[CandidateObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbCandidate.id),
    query=PropertyQuery(INTEGER_TYPE),
)
CANDIDATE_CAND_ID = ValueMember[CandidateObject, int, int](
    name="cand-id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbCandidate.cand_id),
    query=PropertyQuery(INTEGER_TYPE),
)
CANDIDATE_PSC_ID = ValueMember[CandidateObject, str, str](
    name="psc-id",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbCandidate.psc_id),
    query=PropertyQuery(STRING_TYPE),
)
CANDIDATE_ACTIVE = ValueMember[CandidateObject, bool, object](
    name="active",
    value_type=BOOLEAN_TYPE,
    source=OrmColumnSource(DbCandidate.active),
)


@dataclass(frozen=True, slots=True)
class VisitObject:
    orm: DbVisit

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(VISIT_KIND, str(self.orm.id))


VISIT_ID = ValueMember[VisitObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbVisit.id),
    query=PropertyQuery(INTEGER_TYPE),
)
VISIT_NAME = ValueMember[VisitObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbVisit.name),
    query=PropertyQuery(STRING_TYPE),
)
VISIT_LABEL = ValueMember[VisitObject, str, str](
    name="label",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbVisit.label),
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
    source=OrmMappedLinkSource(DbSession.project_id, DbProject.id),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
SESSION_SITE = LinkMember[SessionObject](
    name="site",
    source_kind=SESSION_KIND,
    target_kind=SITE_KIND,
    source=OrmMappedLinkSource(DbSession.site_id, DbSite.id),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
SESSION_CANDIDATE = LinkMember[SessionObject](
    name="candidate",
    source_kind=SESSION_KIND,
    target_kind=CANDIDATE_KIND,
    source=OrmMappedLinkSource(DbSession.candidate_id, DbCandidate.id),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
SESSION_VISIT = LinkMember[SessionObject](
    name="visit",
    source_kind=SESSION_KIND,
    target_kind=VISIT_KIND,
    source=OrmMappedLinkSource(DbSession.visit_label, DbVisit.label, DbVisit.id),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
    load_policy=LoadPolicy.ON_DEMAND,
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
CANDIDATE = ObjectKind(
    CANDIDATE_KIND,
    CandidateObject,
    (
        CANDIDATE_ID,
        CANDIDATE_CAND_ID,
        CANDIDATE_PSC_ID,
        CANDIDATE_ACTIVE,
        _database_row_link(CANDIDATE_KIND),
    ),
)
VISIT = ObjectKind(
    VISIT_KIND,
    VisitObject,
    (VISIT_ID, VISIT_NAME, VISIT_LABEL, _database_row_link(VISIT_KIND)),
)
SESSION = ObjectKind(
    SESSION_KIND,
    SessionObject,
    (
        SESSION_ID,
        SESSION_VISIT_LABEL,
        SESSION_ACTIVE,
        SESSION_CANDIDATE,
        SESSION_VISIT,
        SESSION_PROJECT,
        SESSION_SITE,
        _database_row_link(SESSION_KIND),
    ),
)


class SessionProvider:
    kind = SESSION
    orm_model = DbSession

    def statement(self, selection: ObjectSelection[SessionObject]):
        statement = select(DbSession)
        keys = selection.keys_for(SESSION.name)
        if keys is not None:
            statement = statement.where(DbSession.id.in_(_integer_keys(keys, SESSION.name)))
        return selection.apply_filters(statement, _value_members(SESSION))

    def object_from_orm(self, row: DbSession) -> SessionObject:
        return SessionObject(row)

    def find(self, db: Session, selection: ObjectSelection[SessionObject]) -> tuple[SessionObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class CandidateProvider:
    kind = CANDIDATE
    orm_model = DbCandidate

    def statement(self, selection: ObjectSelection[CandidateObject]):
        statement = select(DbCandidate)
        keys = selection.keys_for(CANDIDATE.name)
        if keys is not None:
            statement = statement.where(DbCandidate.id.in_(_integer_keys(keys, CANDIDATE.name)))
        return selection.apply_filters(statement, _value_members(CANDIDATE))

    def object_from_orm(self, row: DbCandidate) -> CandidateObject:
        return CandidateObject(row)

    def find(
        self, db: Session, selection: ObjectSelection[CandidateObject]
    ) -> tuple[CandidateObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class VisitProvider:
    kind = VISIT
    orm_model = DbVisit

    def statement(self, selection: ObjectSelection[VisitObject]):
        statement = select(DbVisit)
        keys = selection.keys_for(VISIT.name)
        if keys is not None:
            statement = statement.where(DbVisit.id.in_(_integer_keys(keys, VISIT.name)))
        return selection.apply_filters(statement, _value_members(VISIT))

    def object_from_orm(self, row: DbVisit) -> VisitObject:
        return VisitObject(row)

    def find(self, db: Session, selection: ObjectSelection[VisitObject]) -> tuple[VisitObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class ProjectProvider:
    kind = PROJECT
    orm_model = DbProject

    def statement(self, selection: ObjectSelection[ProjectObject]):
        statement = select(DbProject)
        keys = selection.keys_for(PROJECT.name)
        if keys is not None:
            statement = statement.where(DbProject.id.in_(_integer_keys(keys, PROJECT.name)))
        return selection.apply_filters(statement, _value_members(PROJECT))

    def object_from_orm(self, row: DbProject) -> ProjectObject:
        return ProjectObject(row)

    def find(self, db: Session, selection: ObjectSelection[ProjectObject]) -> tuple[ProjectObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


class SiteProvider:
    kind = SITE
    orm_model = DbSite

    def statement(self, selection: ObjectSelection[SiteObject]):
        statement = select(DbSite)
        keys = selection.keys_for(SITE.name)
        if keys is not None:
            statement = statement.where(DbSite.id.in_(_integer_keys(keys, SITE.name)))
        return selection.apply_filters(statement, _value_members(SITE))

    def object_from_orm(self, row: DbSite) -> SiteObject:
        return SiteObject(row)

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
    schema.register_object_kind(CANDIDATE)
    schema.register_object_kind(VISIT)
    schema.register_object_kind(SESSION)
    schema.register_provider(SessionProvider())
    schema.register_provider(ProjectProvider())
    schema.register_provider(SiteProvider())
    schema.register_provider(CandidateProvider())
    schema.register_provider(VisitProvider())


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
        source=OrmMappedLinkSource(source_attribute, DbSession.id),
        traversal_semantics=RelationshipSemantics.BELONGS_TO,
        lifecycle=LifecycleSemantics.REFERENCES,
        target_may_be_shared=True,
    )
