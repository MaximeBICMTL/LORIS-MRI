"""Logical MRI-upload workflow records and their non-owning context."""

from dataclasses import dataclass
from typing import Any

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.mri_upload import DbMriUpload
from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import DATABASE_ROW_KIND, session_link
from loris_data_manager.providers.dicom import DICOM_ARCHIVE_KIND
from loris_data_manager.resources import ObjectRef
from loris_data_manager.schema import (
    BOOLEAN_TYPE,
    INTEGER_TYPE,
    STRING_TYPE,
    BatchLinkSource,
    LifecycleSemantics,
    LinkMember,
    LoadPolicy,
    ObjectKind,
    ObjectSelection,
    OrmColumnSource,
    OrmEntitySource,
    OrmMappedLinkSource,
    PropertyQuery,
    PropertyType,
    RelationshipSemantics,
    ValueMember,
)

MRI_UPLOAD_KIND = "mri-upload"
OPTIONAL_BOOLEAN_TYPE = PropertyType[bool | None](
    "boolean or null", lambda text: BOOLEAN_TYPE.parse(text)
)
OPTIONAL_INTEGER_TYPE = PropertyType[int | None]("integer or null", int)


@dataclass(frozen=True, slots=True)
class MriUploadObject:
    orm: DbMriUpload

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(MRI_UPLOAD_KIND, str(self.orm.id))


MRI_UPLOAD_ID = ValueMember[MriUploadObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbMriUpload.id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_UPLOAD_UPLOADED_BY = ValueMember[MriUploadObject, str, str](
    name="uploaded-by",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbMriUpload.uploaded_by),
    query=PropertyQuery(STRING_TYPE),
)
MRI_UPLOAD_PATIENT_NAME = ValueMember[MriUploadObject, str, str](
    name="patient-name",
    value_type=STRING_TYPE,
    source=OrmColumnSource(DbMriUpload.patient_name),
    query=PropertyQuery(STRING_TYPE),
)
MRI_UPLOAD_INSERTION_COMPLETE = ValueMember[MriUploadObject, bool, bool](
    name="insertion-complete",
    value_type=BOOLEAN_TYPE,
    source=OrmColumnSource(DbMriUpload.insertion_complete),
    query=PropertyQuery(BOOLEAN_TYPE),
)
MRI_UPLOAD_INSERTING = ValueMember[MriUploadObject, bool | None, bool](
    name="inserting",
    value_type=OPTIONAL_BOOLEAN_TYPE,
    source=OrmColumnSource(DbMriUpload.inserting),
    query=PropertyQuery(BOOLEAN_TYPE),
)
MRI_UPLOAD_MINC_INSERTED_COUNT = ValueMember[MriUploadObject, int | None, Any](
    name="minc-inserted-count",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriUpload.number_of_minc_inserted),
)
MRI_UPLOAD_MINC_CREATED_COUNT = ValueMember[MriUploadObject, int | None, Any](
    name="minc-created-count",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriUpload.number_of_minc_created),
)
MRI_UPLOAD_CANDIDATE_INFO_VALIDATED = ValueMember[MriUploadObject, bool | None, bool](
    name="candidate-info-validated",
    value_type=OPTIONAL_BOOLEAN_TYPE,
    source=OrmColumnSource(DbMriUpload.is_candidate_info_validated),
    query=PropertyQuery(BOOLEAN_TYPE),
)
MRI_UPLOAD_DICOM_ARCHIVE_VALIDATED = ValueMember[MriUploadObject, bool, bool](
    name="dicom-archive-validated",
    value_type=BOOLEAN_TYPE,
    source=OrmColumnSource(DbMriUpload.is_dicom_archive_validated),
    query=PropertyQuery(BOOLEAN_TYPE),
)
MRI_UPLOAD_PHANTOM = ValueMember[MriUploadObject, bool, bool](
    name="phantom",
    value_type=BOOLEAN_TYPE,
    source=OrmColumnSource(DbMriUpload.is_phantom),
    query=PropertyQuery(BOOLEAN_TYPE),
)

MRI_UPLOAD_SESSION = session_link(
    source_kind=MRI_UPLOAD_KIND,
    source_attribute=DbMriUpload.session_id,
)
MRI_UPLOAD_DICOM_ARCHIVE = LinkMember[MriUploadObject](
    name="dicom-archive",
    source_kind=MRI_UPLOAD_KIND,
    target_kind=DICOM_ARCHIVE_KIND,
    source=OrmMappedLinkSource(DbMriUpload.dicom_archive_id, DbDicomArchive.id),
    traversal_semantics=RelationshipSemantics.PRODUCES,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
DICOM_ARCHIVE_MRI_UPLOAD = LinkMember[Any](
    name="mri-upload",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=MRI_UPLOAD_KIND,
    source=BatchLinkSource(
        name="load DICOM archive MRI uploads",
        input_name="dicom_archive_ids",
        statement_for_keys=lambda keys: (
            select(DbMriUpload)
            .where(DbMriUpload.dicom_archive_id.in_(keys))
            .options(
                load_only(
                    DbMriUpload.id,
                    DbMriUpload.dicom_archive_id,
                    raiseload=True,
                )
            )
        ),
        input_key=lambda ref: int(ref.key) if isinstance(ref, ObjectRef) else ref,
        target_from_row=lambda row: (
            ObjectRef(DICOM_ARCHIVE_KIND, str(row.dicom_archive_id)),
            ObjectRef(MRI_UPLOAD_KIND, str(row.id)),
        ),
    ),
    traversal_semantics=RelationshipSemantics.DERIVED_FROM,
    lifecycle=LifecycleSemantics.REFERENCES,
    load_policy=LoadPolicy.ON_DEMAND,
)
MRI_UPLOAD_ROW = LinkMember[MriUploadObject](
    name="row",
    source_kind=MRI_UPLOAD_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=OrmEntitySource(),
    lifecycle=LifecycleSemantics.OWNS,
)

MRI_UPLOAD = ObjectKind(
    MRI_UPLOAD_KIND,
    MriUploadObject,
    (
        MRI_UPLOAD_ID,
        MRI_UPLOAD_UPLOADED_BY,
        MRI_UPLOAD_PATIENT_NAME,
        MRI_UPLOAD_INSERTION_COMPLETE,
        MRI_UPLOAD_INSERTING,
        MRI_UPLOAD_MINC_INSERTED_COUNT,
        MRI_UPLOAD_MINC_CREATED_COUNT,
        MRI_UPLOAD_CANDIDATE_INFO_VALIDATED,
        MRI_UPLOAD_DICOM_ARCHIVE_VALIDATED,
        MRI_UPLOAD_PHANTOM,
        MRI_UPLOAD_SESSION,
        MRI_UPLOAD_DICOM_ARCHIVE,
        MRI_UPLOAD_ROW,
    ),
)


class MriUploadProvider:
    kind = MRI_UPLOAD
    orm_model = DbMriUpload

    def statement(self, selection: ObjectSelection[MriUploadObject]):
        statement = select(DbMriUpload)
        keys = selection.keys_for(MRI_UPLOAD.name)
        if keys is not None:
            statement = statement.where(
                DbMriUpload.id.in_(_integer_keys(keys, MRI_UPLOAD.name))
            )
        return selection.apply_filters(statement, _value_members())

    def object_from_orm(self, row: DbMriUpload) -> MriUploadObject:
        return MriUploadObject(row)

    def find(
        self,
        db: Session,
        selection: ObjectSelection[MriUploadObject],
    ) -> tuple[MriUploadObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


def _value_members() -> tuple[ValueMember[Any, Any, Any], ...]:
    return tuple(member for member in MRI_UPLOAD.members if isinstance(member, ValueMember))


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def register_mri_upload_schema(schema: ResourceSchema) -> None:
    schema.register_object_kind(MRI_UPLOAD)
    schema.register_provider(MriUploadProvider())
    schema.register_member(DICOM_ARCHIVE_KIND, DICOM_ARCHIVE_MRI_UPLOAD)
