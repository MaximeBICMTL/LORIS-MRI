"""Proof-of-concept DICOM archive projection."""

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from lib.db.models.dicom_archive import DbDicomArchive
from lib.resource_model.provider import ResourceSchema
from lib.resource_model.providers.core import (
    DATABASE_ROW_KIND,
    SESSION,
    session_link,
)
from lib.resource_model.resources import DatabaseRowObject, LocalPathObject, LocalPathType, ObjectRef
from lib.resource_model.schema import (
    INTEGER_TYPE,
    STRING_TYPE,
    LifecycleSemantics,
    LinkMember,
    ObjectKind,
    ObjectSelection,
    PropertyQuery,
    SelectionConstraint,
    ValueMember,
)

DICOM_ARCHIVE_KIND = "dicom-archive"
LOCAL_PATH_KIND = "local-path"


LOCAL_PATH_STORAGE_ROOT = ValueMember[LocalPathObject, str, object](
    name="storage-root",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.storage_root,
)
LOCAL_PATH_RELATIVE_PATH = ValueMember[LocalPathObject, str, object](
    name="relative-path",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: str(obj.relative_path),
)
LOCAL_PATH_EXPECTED_TYPE = ValueMember[LocalPathObject, str, object](
    name="expected-type",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.expected_type.value,
)
LOCAL_PATH_SIZE = ValueMember[LocalPathObject, int, object](
    name="size",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, context: context.local_path_size(obj),
)
LOCAL_PATH = ObjectKind(
    LOCAL_PATH_KIND,
    LocalPathObject,
    (
        LOCAL_PATH_STORAGE_ROOT,
        LOCAL_PATH_RELATIVE_PATH,
        LOCAL_PATH_EXPECTED_TYPE,
        LOCAL_PATH_SIZE,
    ),
)


@dataclass(frozen=True, slots=True)
class DicomArchiveObject:
    orm: DbDicomArchive

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(DICOM_ARCHIVE_KIND, str(self.orm.id))


DICOM_ID = ValueMember[DicomArchiveObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.id,
    query=PropertyQuery(
        INTEGER_TYPE, lambda query, value: query.where(DbDicomArchive.id == value)
    ),
)
DICOM_STUDY_UID = ValueMember[DicomArchiveObject, str, str](
    name="study-uid",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.study_uid,
    query=PropertyQuery(
        STRING_TYPE, lambda query, value: query.where(DbDicomArchive.study_uid == value)
    ),
)
DICOM_PATIENT_NAME = ValueMember[DicomArchiveObject, str, object](
    name="patient-name",
    value_type=STRING_TYPE,
    get_value=lambda obj, _: obj.orm.patient_name,
)
DICOM_ACQUISITION_COUNT = ValueMember[DicomArchiveObject, int, object](
    name="acquisition-count",
    value_type=INTEGER_TYPE,
    get_value=lambda obj, _: obj.orm.acquisition_count,
)


def _session_targets(obj: DicomArchiveObject) -> tuple[ObjectRef, ...]:
    if obj.orm.session_id is None:
        return ()
    return (ObjectRef(SESSION.name, str(obj.orm.session_id)),)


def _session_source_selection(refs: frozenset[ObjectRef]) -> ObjectSelection[Any]:
    return ObjectSelection(
        constraints=(
            SelectionConstraint(
                lambda query: query.where(
                    DbDicomArchive.session_id.in_(_target_ids(refs, SESSION.name))
                )
            ),
        )
    )


DICOM_ARCHIVE_SESSION = session_link(
    source_kind=DICOM_ARCHIVE_KIND,
    targets_for_source=_session_targets,
    source_selection=_session_source_selection,
)
DICOM_ARCHIVE_ROW = LinkMember[DicomArchiveObject](
    name="row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    targets_for_source=lambda obj: (DatabaseRowObject(obj.orm),),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_SERIES_ROWS = LinkMember[DicomArchiveObject](
    name="series-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    targets_for_source=lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.series),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_FILE_ROWS = LinkMember[DicomArchiveObject](
    name="file-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    targets_for_source=lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.files),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_UPLOAD_ROWS = LinkMember[DicomArchiveObject](
    name="upload-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    targets_for_source=lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.mri_uploads),
    lifecycle=LifecycleSemantics.REFERENCES,
)
DICOM_ARCHIVE_FILE = LinkMember[DicomArchiveObject](
    name="file",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=LOCAL_PATH_KIND,
    targets_for_source=lambda obj: (
        (
            LocalPathObject(
                storage_root="dicom-archive",
                relative_path=PurePosixPath(obj.orm.path.as_posix()),
                expected_type=LocalPathType.FILE,
            ),
        )
        if obj.orm.path is not None
        else ()
    ),
    lifecycle=LifecycleSemantics.OWNS,
)

DICOM_ARCHIVE = ObjectKind(
    DICOM_ARCHIVE_KIND,
    DicomArchiveObject,
    (
        DICOM_ID,
        DICOM_STUDY_UID,
        DICOM_PATIENT_NAME,
        DICOM_ACQUISITION_COUNT,
        DICOM_ARCHIVE_SESSION,
        DICOM_ARCHIVE_ROW,
        DICOM_ARCHIVE_SERIES_ROWS,
        DICOM_ARCHIVE_FILE_ROWS,
        DICOM_ARCHIVE_UPLOAD_ROWS,
        DICOM_ARCHIVE_FILE,
    ),
)


class DicomArchiveProvider:
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
        properties = tuple(
            member for member in DICOM_ARCHIVE.members if isinstance(member, ValueMember)
        )
        statement = selection.apply_filters(statement, properties)
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
    schema.register_object_kind(LOCAL_PATH)
    schema.register_object_kind(DICOM_ARCHIVE)
    schema.register_provider(DicomArchiveProvider())
