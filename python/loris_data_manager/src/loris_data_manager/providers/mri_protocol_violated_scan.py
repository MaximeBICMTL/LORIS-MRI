"""Logical MRI scans that could not be assigned to an acquisition protocol."""

from dataclasses import dataclass
from typing import Any

from lib.db.models.candidate import DbCandidate
from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.mri_protocol_violated_scan import DbMriProtocolViolatedScan
from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import CANDIDATE_KIND, DATABASE_ROW_KIND
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
    NullableReferenceSetBinding,
    ObjectKind,
    ObjectSelection,
    OrmColumnSource,
    OrmEntitySource,
    OrmMappedLinkSource,
    PropertyQuery,
    PropertyType,
    ReferenceUpdateSemantics,
    RelationshipSemantics,
    ValueMember,
)

MRI_PROTOCOL_VIOLATED_SCAN_KIND = "mri-protocol-violated-scan"
OPTIONAL_STRING_TYPE = PropertyType[str | None]("string or null", lambda text: text)
OPTIONAL_INTEGER_TYPE = PropertyType[int | None]("integer or null", int)


@dataclass(frozen=True, slots=True)
class MriProtocolViolatedScanObject:
    orm: DbMriProtocolViolatedScan

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(MRI_PROTOCOL_VIOLATED_SCAN_KIND, str(self.orm.id))


def _string_member(
    name: str, attribute: Any
) -> ValueMember[MriProtocolViolatedScanObject, str | None, str]:
    return ValueMember(
        name=name,
        value_type=OPTIONAL_STRING_TYPE,
        source=OrmColumnSource(attribute),
        query=PropertyQuery(STRING_TYPE),
    )


MRI_PROTOCOL_VIOLATED_SCAN_ID = ValueMember[
    MriProtocolViolatedScanObject, int, int
](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbMriProtocolViolatedScan.id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_PROTOCOL_VIOLATED_SCAN_PSC_ID = _string_member(
    "psc-id", DbMriProtocolViolatedScan.pscid
)
MRI_PROTOCOL_VIOLATED_SCAN_PATIENT_NAME = _string_member(
    "patient-name", DbMriProtocolViolatedScan.patient_name
)
MRI_PROTOCOL_VIOLATED_SCAN_SERIES_DESCRIPTION = _string_member(
    "series-description", DbMriProtocolViolatedScan.series_description
)
MRI_PROTOCOL_VIOLATED_SCAN_SERIES_UID = _string_member(
    "series-uid", DbMriProtocolViolatedScan.series_uid
)
MRI_PROTOCOL_VIOLATED_SCAN_REPETITION_TIME = _string_member(
    "repetition-time", DbMriProtocolViolatedScan.tr_range
)
MRI_PROTOCOL_VIOLATED_SCAN_ECHO_TIME = _string_member(
    "echo-time", DbMriProtocolViolatedScan.te_range
)
MRI_PROTOCOL_VIOLATED_SCAN_INVERSION_TIME = _string_member(
    "inversion-time", DbMriProtocolViolatedScan.ti_range
)
MRI_PROTOCOL_VIOLATED_SCAN_SLICE_THICKNESS = _string_member(
    "slice-thickness", DbMriProtocolViolatedScan.slice_thickness_range
)
MRI_PROTOCOL_VIOLATED_SCAN_X_SPACE = _string_member(
    "x-space", DbMriProtocolViolatedScan.xspace_range
)
MRI_PROTOCOL_VIOLATED_SCAN_Y_SPACE = _string_member(
    "y-space", DbMriProtocolViolatedScan.yspace_range
)
MRI_PROTOCOL_VIOLATED_SCAN_Z_SPACE = _string_member(
    "z-space", DbMriProtocolViolatedScan.zspace_range
)
MRI_PROTOCOL_VIOLATED_SCAN_X_STEP = _string_member(
    "x-step", DbMriProtocolViolatedScan.xstep_range
)
MRI_PROTOCOL_VIOLATED_SCAN_Y_STEP = _string_member(
    "y-step", DbMriProtocolViolatedScan.ystep_range
)
MRI_PROTOCOL_VIOLATED_SCAN_Z_STEP = _string_member(
    "z-step", DbMriProtocolViolatedScan.zstep_range
)
MRI_PROTOCOL_VIOLATED_SCAN_TIME = _string_member(
    "time", DbMriProtocolViolatedScan.time_range
)
MRI_PROTOCOL_VIOLATED_SCAN_IMAGE_TYPE = _string_member(
    "image-type", DbMriProtocolViolatedScan.image_type
)
MRI_PROTOCOL_VIOLATED_SCAN_PHASE_ENCODING_DIRECTION = _string_member(
    "phase-encoding-direction", DbMriProtocolViolatedScan.phase_encoding_direction
)
MRI_PROTOCOL_VIOLATED_SCAN_ECHO_NUMBER = _string_member(
    "echo-number", DbMriProtocolViolatedScan.echo_number
)
MRI_PROTOCOL_VIOLATED_SCAN_PROTOCOL_GROUP_ID = ValueMember[
    MriProtocolViolatedScanObject, int | None, int
](
    name="protocol-group-id",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriProtocolViolatedScan.protocol_group_id),
    query=PropertyQuery(INTEGER_TYPE),
)

MRI_PROTOCOL_VIOLATED_SCAN_CANDIDATE = LinkMember[MriProtocolViolatedScanObject](
    name="candidate",
    source_kind=MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    target_kind=CANDIDATE_KIND,
    source=OrmMappedLinkSource(
        DbMriProtocolViolatedScan.candidate_id,
        DbCandidate.id,
    ),
    traversal_semantics=RelationshipSemantics.BELONGS_TO,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
MRI_PROTOCOL_VIOLATED_SCAN_DICOM_ARCHIVE = LinkMember[
    MriProtocolViolatedScanObject
](
    name="dicom-archive",
    source_kind=MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    target_kind=DICOM_ARCHIVE_KIND,
    source=OrmMappedLinkSource(
        DbMriProtocolViolatedScan.dicom_archive_id,
        DbDicomArchive.id,
    ),
    traversal_semantics=RelationshipSemantics.DERIVED_FROM,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
DICOM_ARCHIVE_PROTOCOL_VIOLATED_SCAN = LinkMember[Any](
    name="protocol-violated-scan",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    source=BatchLinkSource(
        name="load DICOM archive protocol-violated scans",
        input_name="dicom_archive_ids",
        statement_for_keys=lambda keys: (
            select(DbMriProtocolViolatedScan)
            .where(DbMriProtocolViolatedScan.dicom_archive_id.in_(keys))
            .options(
                load_only(
                    DbMriProtocolViolatedScan.id,
                    DbMriProtocolViolatedScan.dicom_archive_id,
                    raiseload=True,
                )
            )
        ),
        input_key=lambda ref: int(ref.key) if isinstance(ref, ObjectRef) else ref,
        target_from_row=lambda row: (
            ObjectRef(DICOM_ARCHIVE_KIND, str(row.dicom_archive_id)),
            ObjectRef(MRI_PROTOCOL_VIOLATED_SCAN_KIND, str(row.id)),
        ),
        set_binding=NullableReferenceSetBinding(
            source_attribute=DbDicomArchive.id,
            target_attribute=DbMriProtocolViolatedScan.dicom_archive_id,
        ),
    ),
    traversal_semantics=RelationshipSemantics.PRODUCES,
    lifecycle=LifecycleSemantics.REFERENCES,
    reference_update=ReferenceUpdateSemantics.SET_NULL,
    load_policy=LoadPolicy.ON_DEMAND,
)
MRI_PROTOCOL_VIOLATED_SCAN_ROW = LinkMember[MriProtocolViolatedScanObject](
    name="row",
    source_kind=MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=OrmEntitySource(),
    lifecycle=LifecycleSemantics.OWNS,
)
MRI_PROTOCOL_VIOLATED_SCAN_FILE = LinkMember[MriProtocolViolatedScanObject](
    name="file",
    source_kind=MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    target_kind=LOCAL_PATH_KIND,
    source=LocalPathColumnSource(
        LocalPathMapping(
            attribute=DbMriProtocolViolatedScan.file_path,
            storage_root="imaging-data",
            expected_type=LocalPathType.FILE,
        )
    ),
    lifecycle=LifecycleSemantics.OWNS,
)

MRI_PROTOCOL_VIOLATED_SCAN = ObjectKind(
    MRI_PROTOCOL_VIOLATED_SCAN_KIND,
    MriProtocolViolatedScanObject,
    (
        MRI_PROTOCOL_VIOLATED_SCAN_ID,
        MRI_PROTOCOL_VIOLATED_SCAN_PSC_ID,
        MRI_PROTOCOL_VIOLATED_SCAN_PATIENT_NAME,
        MRI_PROTOCOL_VIOLATED_SCAN_SERIES_DESCRIPTION,
        MRI_PROTOCOL_VIOLATED_SCAN_SERIES_UID,
        MRI_PROTOCOL_VIOLATED_SCAN_REPETITION_TIME,
        MRI_PROTOCOL_VIOLATED_SCAN_ECHO_TIME,
        MRI_PROTOCOL_VIOLATED_SCAN_INVERSION_TIME,
        MRI_PROTOCOL_VIOLATED_SCAN_SLICE_THICKNESS,
        MRI_PROTOCOL_VIOLATED_SCAN_X_SPACE,
        MRI_PROTOCOL_VIOLATED_SCAN_Y_SPACE,
        MRI_PROTOCOL_VIOLATED_SCAN_Z_SPACE,
        MRI_PROTOCOL_VIOLATED_SCAN_X_STEP,
        MRI_PROTOCOL_VIOLATED_SCAN_Y_STEP,
        MRI_PROTOCOL_VIOLATED_SCAN_Z_STEP,
        MRI_PROTOCOL_VIOLATED_SCAN_TIME,
        MRI_PROTOCOL_VIOLATED_SCAN_IMAGE_TYPE,
        MRI_PROTOCOL_VIOLATED_SCAN_PHASE_ENCODING_DIRECTION,
        MRI_PROTOCOL_VIOLATED_SCAN_ECHO_NUMBER,
        MRI_PROTOCOL_VIOLATED_SCAN_PROTOCOL_GROUP_ID,
        MRI_PROTOCOL_VIOLATED_SCAN_CANDIDATE,
        MRI_PROTOCOL_VIOLATED_SCAN_DICOM_ARCHIVE,
        MRI_PROTOCOL_VIOLATED_SCAN_ROW,
        MRI_PROTOCOL_VIOLATED_SCAN_FILE,
    ),
)


class MriProtocolViolatedScanProvider:
    kind = MRI_PROTOCOL_VIOLATED_SCAN
    orm_model = DbMriProtocolViolatedScan

    def statement(self, selection: ObjectSelection[MriProtocolViolatedScanObject]):
        statement = select(DbMriProtocolViolatedScan)
        keys = selection.keys_for(MRI_PROTOCOL_VIOLATED_SCAN.name)
        if keys is not None:
            statement = statement.where(
                DbMriProtocolViolatedScan.id.in_(
                    _integer_keys(keys, MRI_PROTOCOL_VIOLATED_SCAN.name)
                )
            )
        return selection.apply_filters(statement, _value_members())

    def object_from_orm(
        self, row: DbMriProtocolViolatedScan
    ) -> MriProtocolViolatedScanObject:
        return MriProtocolViolatedScanObject(row)

    def find(
        self,
        db: Session,
        selection: ObjectSelection[MriProtocolViolatedScanObject],
    ) -> tuple[MriProtocolViolatedScanObject, ...]:
        return tuple(
            self.object_from_orm(row) for row in db.scalars(self.statement(selection))
        )


def _value_members() -> tuple[ValueMember[Any, Any, Any], ...]:
    return tuple(
        member
        for member in MRI_PROTOCOL_VIOLATED_SCAN.members
        if isinstance(member, ValueMember)
    )


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def register_mri_protocol_violated_scan_schema(schema: ResourceSchema) -> None:
    schema.register_object_kind(MRI_PROTOCOL_VIOLATED_SCAN)
    schema.register_provider(MriProtocolViolatedScanProvider())
    schema.register_member(DICOM_ARCHIVE_KIND, DICOM_ARCHIVE_PROTOCOL_VIOLATED_SCAN)
