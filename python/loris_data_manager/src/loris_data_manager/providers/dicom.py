"""Proof-of-concept DICOM archive projection."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.dicom_archive_file import DbDicomArchiveFile
from lib.db.models.dicom_archive_series import DbDicomArchiveSeries
from lib.db.models.mri_upload import DbMriUpload
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import Select

from loris_data_manager.graph import GraphFragment
from loris_data_manager.provider import ProviderLoadStep, ResourceSchema
from loris_data_manager.providers.core import (
    DATABASE_ROW_KIND,
    session_link,
)
from loris_data_manager.resources import DatabaseRowObject, LocalPathObject, LocalPathType, ObjectRef
from loris_data_manager.schema import (
    INTEGER_TYPE,
    STRING_TYPE,
    CallableLinkSource,
    CallableValueSource,
    LifecycleSemantics,
    LinkMember,
    ObjectKind,
    ObjectLink,
    ObjectSelection,
    OrmColumnLinkSource,
    OrmColumnSource,
    OrmEntitySource,
    PropertyQuery,
    PropertyRef,
    ValueMember,
)

DICOM_ARCHIVE_KIND = "dicom-archive"
LOCAL_PATH_KIND = "local-path"


LOCAL_PATH_STORAGE_ROOT = ValueMember[LocalPathObject, str, object](
    name="storage-root",
    value_type=STRING_TYPE,
    source=CallableValueSource(lambda obj, _: obj.storage_root),
)
LOCAL_PATH_RELATIVE_PATH = ValueMember[LocalPathObject, str, object](
    name="relative-path",
    value_type=STRING_TYPE,
    source=CallableValueSource(lambda obj, _: str(obj.relative_path)),
)
LOCAL_PATH_EXPECTED_TYPE = ValueMember[LocalPathObject, str, object](
    name="expected-type",
    value_type=STRING_TYPE,
    source=CallableValueSource(lambda obj, _: obj.expected_type.value),
)
LOCAL_PATH_SIZE = ValueMember[LocalPathObject, int, object](
    name="size",
    value_type=INTEGER_TYPE,
    source=CallableValueSource(lambda obj, context: context.local_path_size(obj)),
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
    source=OrmColumnSource(DbDicomArchive.id),
    query=PropertyQuery(INTEGER_TYPE),
)
DICOM_STUDY_UID = ValueMember[DicomArchiveObject, str, str](
    name="study-uid",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbDicomArchive.study_uid),
    query=PropertyQuery(STRING_TYPE),
)
DICOM_PATIENT_NAME = ValueMember[DicomArchiveObject, str, object](
    name="patient-name",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbDicomArchive.patient_name),
)
DICOM_ACQUISITION_COUNT = ValueMember[DicomArchiveObject, int, object](
    name="acquisition-count",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbDicomArchive.acquisition_count),
)


DICOM_ARCHIVE_SESSION = session_link(
    source_kind=DICOM_ARCHIVE_KIND,
    source_attribute=DbDicomArchive.session_id,
)
DICOM_ARCHIVE_ROW = LinkMember[DicomArchiveObject](
    name="row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=OrmEntitySource(),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_SERIES_ROWS = LinkMember[DicomArchiveObject](
    name="series-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=CallableLinkSource(
        lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.series)
    ),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_FILE_ROWS = LinkMember[DicomArchiveObject](
    name="file-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=CallableLinkSource(
        lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.files)
    ),
    lifecycle=LifecycleSemantics.OWNS,
)
DICOM_ARCHIVE_UPLOAD_ROWS = LinkMember[DicomArchiveObject](
    name="upload-row",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=CallableLinkSource(
        lambda obj: tuple(DatabaseRowObject(row) for row in obj.orm.mri_uploads)
    ),
    lifecycle=LifecycleSemantics.REFERENCES,
)
DICOM_ARCHIVE_FILE = LinkMember[DicomArchiveObject](
    name="file",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=LOCAL_PATH_KIND,
    source=OrmColumnLinkSource[Path | None](
        DbDicomArchive.path,
        lambda path: (
            LocalPathObject(
                storage_root="dicom-archive",
                relative_path=PurePosixPath(path.as_posix()),
                expected_type=LocalPathType.FILE,
            ),
        ) if path is not None else (),
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
    orm_model = DbDicomArchive
    deferred_links = frozenset(
        {
            DICOM_ARCHIVE_SERIES_ROWS.name,
            DICOM_ARCHIVE_FILE_ROWS.name,
            DICOM_ARCHIVE_UPLOAD_ROWS.name,
        }
    )

    def statement(self, selection: ObjectSelection[DicomArchiveObject]):
        statement = select(DbDicomArchive)
        keys = selection.keys_for(DICOM_ARCHIVE.name)
        if keys is not None:
            statement = statement.where(DbDicomArchive.id.in_(_integer_keys(keys, DICOM_ARCHIVE.name)))
        properties = tuple(
            member for member in DICOM_ARCHIVE.members if isinstance(member, ValueMember)
        )
        return selection.apply_filters(statement, properties)

    def object_from_orm(self, row: DbDicomArchive) -> DicomArchiveObject:
        return DicomArchiveObject(row)

    def load_steps(self, links: frozenset[str]) -> tuple[ProviderLoadStep, ...]:
        steps: list[ProviderLoadStep] = []
        if DICOM_ARCHIVE_SERIES_ROWS.name in links:
            steps.append(
                _database_row_load_step(
                    DICOM_ARCHIVE_SERIES_ROWS,
                    "load DICOM series rows",
                    _series_statement,
                    _series_source_id,
                )
            )
        if DICOM_ARCHIVE_FILE_ROWS.name in links:
            steps.append(
                _database_row_load_step(
                    DICOM_ARCHIVE_FILE_ROWS,
                    "load DICOM file rows",
                    _file_statement,
                    _file_source_id,
                )
            )
        if DICOM_ARCHIVE_UPLOAD_ROWS.name in links:
            steps.append(
                _database_row_load_step(
                    DICOM_ARCHIVE_UPLOAD_ROWS,
                    "load MRI upload rows",
                    _upload_statement,
                    _upload_source_id,
                )
            )
        return tuple(steps)

    def find(
        self,
        db: Session,
        selection: ObjectSelection[DicomArchiveObject],
    ) -> tuple[DicomArchiveObject, ...]:
        statement = self.statement(selection).options(
            selectinload(DbDicomArchive.series),
            selectinload(DbDicomArchive.files),
            selectinload(DbDicomArchive.mri_uploads),
        )
        return tuple(self.object_from_orm(row) for row in db.scalars(statement))


def _database_row_load_step(
    link: LinkMember[DicomArchiveObject],
    name: str,
    statement_for_keys: Callable[[Any], Select[Any]],
    source_id: Callable[[Any], int | None],
) -> ProviderLoadStep:
    def fragment_from_rows(rows: tuple[Any, ...]) -> GraphFragment:
        physical = tuple(DatabaseRowObject(row) for row in rows)
        return GraphFragment(
            physical_objects=physical,
            links=tuple(
                ObjectLink(
                    member=PropertyRef(DICOM_ARCHIVE.name, link.name),
                    source=ObjectRef(DICOM_ARCHIVE.name, str(source_id(row))),
                    target=obj.ref,
                )
                for row, obj in zip(rows, physical, strict=True)
            ),
        )

    return ProviderLoadStep(
        name=name,
        input_name="anchor_ids",
        input_kind=DICOM_ARCHIVE.name,
        statement_for_keys=statement_for_keys,
        fragment_from_rows=fragment_from_rows,
    )


def _series_statement(keys: Any) -> Select[Any]:
    return select(DbDicomArchiveSeries).where(DbDicomArchiveSeries.archive_id.in_(keys))


def _series_source_id(row: Any) -> int:
    assert isinstance(row, DbDicomArchiveSeries)
    return row.archive_id


def _file_statement(keys: Any) -> Select[Any]:
    return select(DbDicomArchiveFile).where(DbDicomArchiveFile.archive_id.in_(keys))


def _file_source_id(row: Any) -> int:
    assert isinstance(row, DbDicomArchiveFile)
    return row.archive_id


def _upload_statement(keys: Any) -> Select[Any]:
    return select(DbMriUpload).where(DbMriUpload.dicom_archive_id.in_(keys))


def _upload_source_id(row: Any) -> int | None:
    assert isinstance(row, DbMriUpload)
    return row.dicom_archive_id


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def register_dicom_schema(schema: ResourceSchema) -> None:
    schema.register_object_kind(LOCAL_PATH)
    schema.register_object_kind(DICOM_ARCHIVE)
    schema.register_provider(DicomArchiveProvider())
