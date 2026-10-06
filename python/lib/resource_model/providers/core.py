"""Core logical object and relationship definitions."""

from dataclasses import dataclass
from typing import Any, ClassVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from lib.resource_model.provider import ResourceSchema
from lib.resource_model.resources import DatabaseRowObject
from lib.resource_model.schema import (
    BOOLEAN_TYPE,
    INTEGER_TYPE,
    STRING_TYPE,
    BoundResource,
    ObjectKind,
    ObjectProperty,
    ObjectRef,
    ObjectSelection,
    PropertyQuery,
    RelationshipBinding,
    RelationshipKind,
    RelationshipSemantics,
    SelectionConstraint,
)


@dataclass(frozen=True, slots=True)
class SessionObject:
    """Logical session bound to an ORM instance in the current unit of work."""

    orm: DbSession
    properties: ClassVar[tuple[ObjectProperty[Any, Any, Any], ...]]

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(SESSION.name, str(self.orm.id))

    @property
    def bound_resources(self) -> tuple[BoundResource, ...]:
        return (BoundResource(DatabaseRowObject(self.orm)),)


SESSION_ID = ObjectProperty[SessionObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.id,
    query=PropertyQuery(
        operand_type=INTEGER_TYPE,
        apply=lambda query, value: query.where(DbSession.id == value),
    ),
)
SESSION_PARTICIPANT_ID = ObjectProperty[SessionObject, int, int](
    name="participant-id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.candidate_id,
    query=PropertyQuery(
        operand_type=INTEGER_TYPE,
        apply=lambda query, value: query.where(DbSession.candidate_id == value),
    ),
)
SESSION_VISIT_LABEL = ObjectProperty[SessionObject, str, str](
    name="visit-label",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.visit_label,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbSession.visit_label == value),
    ),
)
SESSION_ACTIVE = ObjectProperty[SessionObject, bool, object](
    name="active",
    value_type=BOOLEAN_TYPE,
    get_value=lambda obj: obj.orm.active,
)
SessionObject.properties = (
    SESSION_ID,
    SESSION_PARTICIPANT_ID,
    SESSION_VISIT_LABEL,
    SESSION_ACTIVE,
)


SESSION = ObjectKind("session", SessionObject)


@dataclass(frozen=True, slots=True)
class ProjectObject:
    """Logical LORIS project bound to its ORM row."""

    orm: DbProject
    properties: ClassVar[tuple[ObjectProperty[Any, Any, Any], ...]]

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(PROJECT.name, str(self.orm.id))

    @property
    def bound_resources(self) -> tuple[BoundResource, ...]:
        return (BoundResource(DatabaseRowObject(self.orm)),)


PROJECT_ID = ObjectProperty[ProjectObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.id,
    query=PropertyQuery(
        operand_type=INTEGER_TYPE,
        apply=lambda query, value: query.where(DbProject.id == value),
    ),
)
PROJECT_NAME = ObjectProperty[ProjectObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.name,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbProject.name == value),
    ),
)
PROJECT_ALIAS = ObjectProperty[ProjectObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.alias,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbProject.alias == value),
    ),
)
ProjectObject.properties = (
    PROJECT_ID,
    PROJECT_NAME,
    PROJECT_ALIAS,
)


PROJECT = ObjectKind("project", ProjectObject)


@dataclass(frozen=True, slots=True)
class SiteObject:
    """Logical LORIS site bound to its ORM row."""

    orm: DbSite
    properties: ClassVar[tuple[ObjectProperty[Any, Any, Any], ...]]

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(SITE.name, str(self.orm.id))

    @property
    def bound_resources(self) -> tuple[BoundResource, ...]:
        return (BoundResource(DatabaseRowObject(self.orm)),)


SITE_ID = ObjectProperty[SiteObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.id,
    query=PropertyQuery(
        operand_type=INTEGER_TYPE,
        apply=lambda query, value: query.where(DbSite.id == value),
    ),
)
SITE_NAME = ObjectProperty[SiteObject, str, str](
    name="name",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.name,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbSite.name == value),
    ),
)
SITE_ALIAS = ObjectProperty[SiteObject, str, str](
    name="alias",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.alias,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbSite.alias == value),
    ),
)
SiteObject.properties = (
    SITE_ID,
    SITE_NAME,
    SITE_ALIAS,
)


SITE = ObjectKind("site", SiteObject)
SESSION_PROJECT = RelationshipKind(
    name="session-belongs-to-project",
    member_name="project",
    source_kind=SESSION.name,
    target_kind=PROJECT.name,
    semantics=RelationshipSemantics.BELONGS_TO,
    target_may_be_shared=True,
)
SESSION_SITE = RelationshipKind(
    name="session-belongs-to-site",
    member_name="site",
    source_kind=SESSION.name,
    target_kind=SITE.name,
    semantics=RelationshipSemantics.BELONGS_TO,
    target_may_be_shared=True,
)
SESSION_PROJECT_BINDING = RelationshipBinding[SessionObject](
    kind=SESSION_PROJECT,
    targets_for_source=lambda obj: (ObjectRef(PROJECT.name, str(obj.orm.project_id)),),
    source_selection=lambda refs: ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: query.where(
                    DbSession.project_id.in_(_target_ids(refs, PROJECT.name))
                )
            ),
        )
    ),
)
SESSION_SITE_BINDING = RelationshipBinding[SessionObject](
    kind=SESSION_SITE,
    targets_for_source=lambda obj: (ObjectRef(SITE.name, str(obj.orm.site_id)),),
    source_selection=lambda refs: ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: query.where(DbSession.site_id.in_(_target_ids(refs, SITE.name)))
            ),
        )
    ),
)


class SessionProvider:
    kind = SESSION

    def find(self, db: Session, selection: ObjectSelection[SessionObject]) -> tuple[SessionObject, ...]:
        statement = select(DbSession)
        keys = selection.keys_for(SESSION.name)
        if keys is not None:
            statement = statement.where(DbSession.id.in_(_integer_keys(keys, SESSION.name)))
        statement = selection.apply_filters(statement, SessionObject.properties)

        return tuple(SessionObject(row) for row in db.scalars(statement))


class ProjectProvider:
    kind = PROJECT

    def find(self, db: Session, selection: ObjectSelection[ProjectObject]) -> tuple[ProjectObject, ...]:
        statement = select(DbProject)
        keys = selection.keys_for(PROJECT.name)
        if keys is not None:
            statement = statement.where(DbProject.id.in_(_integer_keys(keys, PROJECT.name)))
        statement = selection.apply_filters(statement, ProjectObject.properties)
        return tuple(ProjectObject(row) for row in db.scalars(statement))


class SiteProvider:
    kind = SITE

    def find(self, db: Session, selection: ObjectSelection[SiteObject]) -> tuple[SiteObject, ...]:
        statement = select(DbSite)
        keys = selection.keys_for(SITE.name)
        if keys is not None:
            statement = statement.where(DbSite.id.in_(_integer_keys(keys, SITE.name)))
        statement = selection.apply_filters(statement, SiteObject.properties)
        return tuple(SiteObject(row) for row in db.scalars(statement))


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
    """Register the logical kinds owned by the LORIS core."""

    schema.register_object_kind(SESSION)
    schema.register_object_kind(PROJECT)
    schema.register_object_kind(SITE)
    schema.register_relationship_kind(SESSION_PROJECT)
    schema.register_relationship_kind(SESSION_SITE)
    schema.register_provider(SessionProvider())
    schema.register_provider(ProjectProvider())
    schema.register_provider(SiteProvider())
    schema.register_relationship_binding(SESSION_PROJECT_BINDING)
    schema.register_relationship_binding(SESSION_SITE_BINDING)


def session_relationship(
    *,
    name: str,
    source_kind: str,
) -> RelationshipKind:
    """Define the conventional relationship from a module object to a session."""

    return RelationshipKind(
        name=name,
        member_name="session",
        source_kind=source_kind,
        target_kind=SESSION.name,
        semantics=RelationshipSemantics.BELONGS_TO,
        target_may_be_shared=True,
    )
