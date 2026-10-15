"""Logical imaging-file resources and their provenance."""

from dataclasses import dataclass
from pathlib import PurePosixPath

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.file import DbFile
from lib.db.models.file_parameter import DbFileParameter
from lib.db.models.parameter_type import DbParameterType
from sqlalchemy import select
from sqlalchemy.orm import Session

from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import DATABASE_ROW_KIND, session_link
from loris_data_manager.providers.dicom import DICOM_ARCHIVE_KIND, LOCAL_PATH_KIND
from loris_data_manager.resources import LocalPathType, ObjectRef
from loris_data_manager.schema import (
    INTEGER_TYPE,
    STRING_TYPE,
    BatchLinkSource,
    LifecycleSemantics,
    LinkMember,
    LoadPolicy,
    LocalPathColumnSource,
    LocalPathMapping,
    LocalPathSetBinding,
    ObjectKind,
    ObjectSelection,
    OrmColumnSource,
    OrmEntitySource,
    OrmMappedLinkSource,
    PropertyQuery,
    PropertyType,
    RelationshipSemantics,
    StorageRoot,
    ValueMember,
)

IMAGING_FILE_KIND = "imaging-file"
OPTIONAL_STRING_TYPE = PropertyType[str | None]("string or null", lambda text: text)
OPTIONAL_INTEGER_TYPE = PropertyType[int | None]("integer or null", int)


@dataclass(frozen=True, slots=True)
class ImagingFileObject:
    orm: DbFile

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(IMAGING_FILE_KIND, str(self.orm.id))


IMAGING_FILE_ID = ValueMember[ImagingFileObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbFile.id),
    query=PropertyQuery(INTEGER_TYPE),
)
IMAGING_FILE_OUTPUT_TYPE = ValueMember[ImagingFileObject, str, str](
    name="output-type",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbFile.output_type),
    query=PropertyQuery(STRING_TYPE),
)
IMAGING_FILE_FILE_TYPE = ValueMember[ImagingFileObject, str | None, str](
    name="file-type",
    value_type=OPTIONAL_STRING_TYPE,
    source=OrmColumnSource(DbFile.file_type),
    query=PropertyQuery(STRING_TYPE),
)
IMAGING_FILE_SERIES_UID = ValueMember[ImagingFileObject, str | None, str](
    name="series-uid",
    value_type=OPTIONAL_STRING_TYPE,
    source=OrmColumnSource(DbFile.series_uid),
    query=PropertyQuery(STRING_TYPE),
)
IMAGING_FILE_SCAN_TYPE_ID = ValueMember[ImagingFileObject, int | None, int](
    name="scan-type-id",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbFile.scan_type_id),
    query=PropertyQuery(INTEGER_TYPE),
)
IMAGING_FILE_SESSION = session_link(
    source_kind=IMAGING_FILE_KIND,
    source_attribute=DbFile.session_id,
)
IMAGING_FILE_SOURCE_ARCHIVE = LinkMember[ImagingFileObject](
    name="source-archive",
    source_kind=IMAGING_FILE_KIND,
    target_kind=DICOM_ARCHIVE_KIND,
    source=OrmMappedLinkSource(DbFile.dicom_archive_id, DbDicomArchive.id),
    traversal_semantics=RelationshipSemantics.DERIVED_FROM,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
IMAGING_FILE_ROW = LinkMember[ImagingFileObject](
    name="row",
    source_kind=IMAGING_FILE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=OrmEntitySource(),
    lifecycle=LifecycleSemantics.OWNS,
)
IMAGING_FILE_FILE = LinkMember[ImagingFileObject](
    name="file",
    source_kind=IMAGING_FILE_KIND,
    target_kind=LOCAL_PATH_KIND,
    source=LocalPathColumnSource(
        LocalPathMapping(
            attribute=DbFile.path,
            storage_root="imaging-data",
            expected_type=LocalPathType.FILE,
        )
    ),
    lifecycle=LifecycleSemantics.OWNS,
)
IMAGING_FILE_PARAMETER_ROWS = LinkMember[ImagingFileObject](
    name="parameter-row",
    source_kind=IMAGING_FILE_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=BatchLinkSource.database_rows(
        source_kind=IMAGING_FILE_KIND,
        source_attribute=DbFile.id,
        target_attribute=DbFileParameter.file_id,
        name="load imaging-file parameter rows",
        input_name="imaging_file_ids",
    ),
    lifecycle=LifecycleSemantics.OWNS,
    load_policy=LoadPolicy.ON_DEMAND,
)


def _parameter_path_link(
    name: str,
    parameter_names: tuple[str, ...],
    *,
    path_prefix: PurePosixPath = PurePosixPath(),
) -> LinkMember[ImagingFileObject]:
    path = LocalPathMapping(
        attribute=DbFileParameter.value,
        storage_root="imaging-data",
        expected_type=LocalPathType.FILE,
        path_prefix=path_prefix,
    )
    source = BatchLinkSource(
        name=f"load imaging-file {name} paths",
        input_name="imaging_file_ids",
        statement_for_keys=lambda keys: (
            select(DbFileParameter)
            .join(DbParameterType, DbFileParameter.type_id == DbParameterType.id)
            .where(
                DbFileParameter.file_id.in_(keys),
                DbParameterType.name.in_(parameter_names),
                DbFileParameter.value.is_not(None),
                DbFileParameter.value != "",
            )
        ),
        input_key=lambda ref: int(ref.key) if isinstance(ref, ObjectRef) else ref,
        target_from_row=lambda row: (
            ObjectRef(IMAGING_FILE_KIND, str(row.file_id)),
            path.target_from_value(row.value),
        ),
        set_binding=LocalPathSetBinding(
            source_attribute=DbFile.id,
            owner_attribute=DbFileParameter.file_id,
            path=path,
        ),
    )
    return LinkMember(
        name=name,
        source_kind=IMAGING_FILE_KIND,
        target_kind=LOCAL_PATH_KIND,
        source=source,
        lifecycle=LifecycleSemantics.OWNS,
        load_policy=LoadPolicy.ON_DEMAND,
    )


IMAGING_FILE_BIDS_JSON_SIDECAR = _parameter_path_link(
    "bids-json-sidecar",
    ("bids_json_file",),
)
IMAGING_FILE_BVAL_FILE = _parameter_path_link(
    "bval-file",
    ("bids_bval", "check_bval_filename"),
)
IMAGING_FILE_BVEC_FILE = _parameter_path_link(
    "bvec-file",
    ("bids_bvec", "check_bvec_filename"),
)
IMAGING_FILE_PREVIEW_IMAGE = _parameter_path_link(
    "preview-image",
    ("check_pic_filename",),
    path_prefix=PurePosixPath("pic"),
)

IMAGING_FILE = ObjectKind(
    IMAGING_FILE_KIND,
    ImagingFileObject,
    (
        IMAGING_FILE_ID,
        IMAGING_FILE_OUTPUT_TYPE,
        IMAGING_FILE_FILE_TYPE,
        IMAGING_FILE_SERIES_UID,
        IMAGING_FILE_SCAN_TYPE_ID,
        IMAGING_FILE_SESSION,
        IMAGING_FILE_SOURCE_ARCHIVE,
        IMAGING_FILE_ROW,
        IMAGING_FILE_FILE,
        IMAGING_FILE_PARAMETER_ROWS,
        IMAGING_FILE_BIDS_JSON_SIDECAR,
        IMAGING_FILE_BVAL_FILE,
        IMAGING_FILE_BVEC_FILE,
        IMAGING_FILE_PREVIEW_IMAGE,
    ),
)


class ImagingFileProvider:
    kind = IMAGING_FILE
    orm_model = DbFile

    def statement(self, selection: ObjectSelection[ImagingFileObject]):
        statement = select(DbFile)
        keys = selection.keys_for(IMAGING_FILE.name)
        if keys is not None:
            statement = statement.where(
                DbFile.id.in_(_integer_keys(keys, IMAGING_FILE.name))
            )
        return selection.apply_filters(
            statement,
            (
                IMAGING_FILE_ID,
                IMAGING_FILE_OUTPUT_TYPE,
                IMAGING_FILE_FILE_TYPE,
                IMAGING_FILE_SERIES_UID,
                IMAGING_FILE_SCAN_TYPE_ID,
            ),
        )

    def object_from_orm(self, row: DbFile) -> ImagingFileObject:
        return ImagingFileObject(row)

    def find(
        self,
        db: Session,
        selection: ObjectSelection[ImagingFileObject],
    ) -> tuple[ImagingFileObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def register_imaging_schema(schema: ResourceSchema) -> None:
    schema.register_storage_root(StorageRoot("imaging-data", "dataDirBasePath"))
    schema.register_object_kind(IMAGING_FILE)
    schema.register_provider(ImagingFileProvider())
