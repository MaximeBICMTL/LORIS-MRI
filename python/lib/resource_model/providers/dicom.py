"""Proof-of-concept DICOM archive projection."""

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, ClassVar

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import Select

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.mri_upload import DbMriUpload
from lib.resource_model.provider import ResourceSchema
from lib.resource_model.providers.core import SESSION, session_relationship
from lib.resource_model.resources import DatabaseRow, LocalPath, LocalPathType, Resource
from lib.resource_model.schema import (
    INTEGER_TYPE,
    STRING_TYPE,
    ObjectKind,
    ObjectProperty,
    ObjectRef,
    ObjectSelection,
    PropertyQuery,
    RelationshipBinding,
    SelectionConstraint,
)


@dataclass(frozen=True, slots=True)
class DicomArchiveObject:
    """Logical DICOM archive bound to an ORM graph in the current unit of work."""

    orm: DbDicomArchive
    properties: ClassVar[tuple[ObjectProperty[Any, Any, Any], ...]]

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(DICOM_ARCHIVE.name, str(self.orm.id))

    @property
    def resources(self) -> tuple[Resource, ...]:
        resources: list[Resource] = [
            DatabaseRow.from_orm(self.orm),
            *(DatabaseRow.from_orm(series) for series in self.orm.series),
            *(DatabaseRow.from_orm(archive_file) for archive_file in self.orm.files),
            *(DatabaseRow.from_orm(upload) for upload in self.orm.mri_uploads),
        ]
        if self.orm.path is not None:
            resources.append(
                LocalPath(
                    storage_root="dicom_archive",
                    relative_path=PurePosixPath(self.orm.path.as_posix()),
                    path_type=LocalPathType.FILE,
                )
            )
        return tuple(resources)

    def session_refs(self) -> tuple[ObjectRef, ...]:
        """Return the sessions associated through direct or MRI-upload links."""

        return tuple(ObjectRef(SESSION.name, str(session_id)) for session_id in _session_ids(self))


def _session_ids(obj: DicomArchiveObject) -> tuple[int, ...]:
    session_ids = {upload.session_id for upload in obj.orm.mri_uploads if upload.session_id is not None}
    if obj.orm.session_id is not None:
        session_ids.add(obj.orm.session_id)
    return tuple(sorted(session_ids))


def _filter_by_session_ids(query: Select[Any], session_ids: frozenset[int]) -> Select[Any]:
    return query.where(
        or_(
            DbDicomArchive.session_id.in_(session_ids),
            DbDicomArchive.mri_uploads.any(DbMriUpload.session_id.in_(session_ids)),
        )
    )


DICOM_ID = ObjectProperty[DicomArchiveObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.id,
    query=PropertyQuery(
        operand_type=INTEGER_TYPE,
        apply=lambda query, value: query.where(DbDicomArchive.id == value),
    ),
)
DICOM_STUDY_UID = ObjectProperty[DicomArchiveObject, str, str](
    name="study-uid",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.study_uid,
    query=PropertyQuery(
        operand_type=STRING_TYPE,
        apply=lambda query, value: query.where(DbDicomArchive.study_uid == value),
    ),
)
DICOM_PATIENT_NAME = ObjectProperty[DicomArchiveObject, str, object](
    name="patient-name",
    value_type=STRING_TYPE,
    get_value=lambda obj: obj.orm.patient_name,
)
DICOM_ACQUISITION_COUNT = ObjectProperty[DicomArchiveObject, int, object](
    name="acquisition-count",
    value_type=INTEGER_TYPE,
    get_value=lambda obj: obj.orm.acquisition_count,
)
DicomArchiveObject.properties = (
    DICOM_ID,
    DICOM_STUDY_UID,
    DICOM_PATIENT_NAME,
    DICOM_ACQUISITION_COUNT,
)


DICOM_ARCHIVE = ObjectKind("dicom-archive", DicomArchiveObject)
DICOM_ARCHIVE_SESSION = session_relationship(
    name="dicom-archive-belongs-to-session",
    source_kind=DICOM_ARCHIVE.name,
)
DICOM_ARCHIVE_SESSION_BINDING = RelationshipBinding[DicomArchiveObject](
    kind=DICOM_ARCHIVE_SESSION,
    targets_for_source=lambda obj: obj.session_refs(),
    source_selection=lambda refs: ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: _filter_by_session_ids(query, _target_ids(refs, SESSION.name))
            ),
        )
    ),
)


class DicomArchiveProvider:
    """Resolve DICOM archives, including their subordinate ORM rows and archive path."""

    kind = DICOM_ARCHIVE

    def find(
        self,
        db: Session,
        selection: ObjectSelection[DicomArchiveObject],
    ) -> tuple[DicomArchiveObject, ...]:
        statement = select(DbDicomArchive).options(
            selectinload(DbDicomArchive.series),
            selectinload(DbDicomArchive.files),
            selectinload(DbDicomArchive.mri_uploads),
        )
        keys = selection.keys_for(DICOM_ARCHIVE.name)
        if keys is not None:
            statement = statement.where(DbDicomArchive.id.in_(_integer_keys(keys, DICOM_ARCHIVE.name)))
        statement = selection.apply_filters(statement, DicomArchiveObject.properties)

        return tuple(DicomArchiveObject(row) for row in db.scalars(statement))


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


def register_dicom_schema(schema: ResourceSchema) -> None:
    """Extend a core resource schema with DICOM concepts."""

    schema.register_object_kind(DICOM_ARCHIVE)
    schema.register_relationship_kind(DICOM_ARCHIVE_SESSION)
    schema.register_provider(DicomArchiveProvider())
    schema.register_relationship_binding(DICOM_ARCHIVE_SESSION_BINDING)
