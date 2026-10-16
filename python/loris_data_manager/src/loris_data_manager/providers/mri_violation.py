"""Logical MRI violations and their owned physical resources."""

from dataclasses import dataclass
from typing import Any

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.mri_violation_log import DbMriViolationLog
from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import DATABASE_ROW_KIND
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

MRI_VIOLATION_KIND = "mri-violation"
OPTIONAL_STRING_TYPE = PropertyType[str | None]("string or null", lambda text: text)
OPTIONAL_INTEGER_TYPE = PropertyType[int | None]("integer or null", int)


@dataclass(frozen=True, slots=True)
class MriViolationObject:
    orm: DbMriViolationLog

    @property
    def ref(self) -> ObjectRef:
        return ObjectRef(MRI_VIOLATION_KIND, str(self.orm.id))


def _string_member(
    name: str, attribute: Any
) -> ValueMember[MriViolationObject, str | None, str]:
    return ValueMember(
        name=name,
        value_type=OPTIONAL_STRING_TYPE,
        source=OrmColumnSource(attribute),
        query=PropertyQuery(STRING_TYPE),
    )


MRI_VIOLATION_ID = ValueMember[MriViolationObject, int, int](
    name="id",
    value_type=INTEGER_TYPE,
    source=OrmColumnSource(DbMriViolationLog.id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_VIOLATION_SERIES_UID = _string_member("series-uid", DbMriViolationLog.series_uid)
MRI_VIOLATION_PATIENT_NAME = _string_member(
    "patient-name", DbMriViolationLog.patient_name
)
MRI_VIOLATION_CANDIDATE_ID = ValueMember[MriViolationObject, int | None, int](
    name="candidate-id",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriViolationLog.candidate_id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_VIOLATION_VISIT_LABEL = _string_member(
    "visit-label", DbMriViolationLog.visit_label
)
MRI_VIOLATION_CHECK_ID = ValueMember[MriViolationObject, int | None, int](
    name="check-id",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriViolationLog.check_id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_VIOLATION_SCAN_TYPE_ID = ValueMember[MriViolationObject, int | None, int](
    name="scan-type-id",
    value_type=OPTIONAL_INTEGER_TYPE,
    source=OrmColumnSource(DbMriViolationLog.scan_type_id),
    query=PropertyQuery(INTEGER_TYPE),
)
MRI_VIOLATION_SEVERITY = _string_member("severity", DbMriViolationLog.severity)
MRI_VIOLATION_HEADER = _string_member("header", DbMriViolationLog.header)
MRI_VIOLATION_VALUE = _string_member("value", DbMriViolationLog.value)
MRI_VIOLATION_VALID_RANGE = _string_member("valid-range", DbMriViolationLog.valid_range)
MRI_VIOLATION_VALID_REGEX = _string_member("valid-regex", DbMriViolationLog.valid_regex)

MRI_VIOLATION_DICOM_ARCHIVE = LinkMember[MriViolationObject](
    name="dicom-archive",
    source_kind=MRI_VIOLATION_KIND,
    target_kind=DICOM_ARCHIVE_KIND,
    source=OrmMappedLinkSource(
        DbMriViolationLog.dicom_archive_id, DbDicomArchive.id
    ),
    traversal_semantics=RelationshipSemantics.DERIVED_FROM,
    lifecycle=LifecycleSemantics.REFERENCES,
    target_may_be_shared=True,
)
DICOM_ARCHIVE_MRI_VIOLATION = LinkMember[Any](
    name="mri-violation",
    source_kind=DICOM_ARCHIVE_KIND,
    target_kind=MRI_VIOLATION_KIND,
    source=BatchLinkSource(
        name="load DICOM archive MRI violations",
        input_name="dicom_archive_ids",
        statement_for_keys=lambda keys: (
            select(DbMriViolationLog)
            .where(DbMriViolationLog.dicom_archive_id.in_(keys))
            .options(
                load_only(
                    DbMriViolationLog.id,
                    DbMriViolationLog.dicom_archive_id,
                    raiseload=True,
                )
            )
        ),
        input_key=lambda ref: int(ref.key) if isinstance(ref, ObjectRef) else ref,
        target_from_row=lambda row: (
            ObjectRef(DICOM_ARCHIVE_KIND, str(row.dicom_archive_id)),
            ObjectRef(MRI_VIOLATION_KIND, str(row.id)),
        ),
    ),
    traversal_semantics=RelationshipSemantics.PRODUCES,
    lifecycle=LifecycleSemantics.REFERENCES,
    load_policy=LoadPolicy.ON_DEMAND,
)
MRI_VIOLATION_ROW = LinkMember[MriViolationObject](
    name="row",
    source_kind=MRI_VIOLATION_KIND,
    target_kind=DATABASE_ROW_KIND,
    source=OrmEntitySource(),
    lifecycle=LifecycleSemantics.OWNS,
)
MRI_VIOLATION_FILE = LinkMember[MriViolationObject](
    name="file",
    source_kind=MRI_VIOLATION_KIND,
    target_kind=LOCAL_PATH_KIND,
    source=LocalPathColumnSource(
        LocalPathMapping(
            attribute=DbMriViolationLog.file_path,
            storage_root="imaging-data",
            expected_type=LocalPathType.FILE,
        )
    ),
    lifecycle=LifecycleSemantics.OWNS,
)

MRI_VIOLATION = ObjectKind(
    MRI_VIOLATION_KIND,
    MriViolationObject,
    (
        MRI_VIOLATION_ID,
        MRI_VIOLATION_SERIES_UID,
        MRI_VIOLATION_PATIENT_NAME,
        MRI_VIOLATION_CANDIDATE_ID,
        MRI_VIOLATION_VISIT_LABEL,
        MRI_VIOLATION_CHECK_ID,
        MRI_VIOLATION_SCAN_TYPE_ID,
        MRI_VIOLATION_SEVERITY,
        MRI_VIOLATION_HEADER,
        MRI_VIOLATION_VALUE,
        MRI_VIOLATION_VALID_RANGE,
        MRI_VIOLATION_VALID_REGEX,
        MRI_VIOLATION_DICOM_ARCHIVE,
        MRI_VIOLATION_ROW,
        MRI_VIOLATION_FILE,
    ),
)


class MriViolationProvider:
    kind = MRI_VIOLATION
    orm_model = DbMriViolationLog

    def statement(self, selection: ObjectSelection[MriViolationObject]):
        statement = select(DbMriViolationLog)
        keys = selection.keys_for(MRI_VIOLATION.name)
        if keys is not None:
            statement = statement.where(
                DbMriViolationLog.id.in_(_integer_keys(keys, MRI_VIOLATION.name))
            )
        return selection.apply_filters(statement, _value_members())

    def object_from_orm(self, row: DbMriViolationLog) -> MriViolationObject:
        return MriViolationObject(row)

    def find(
        self,
        db: Session,
        selection: ObjectSelection[MriViolationObject],
    ) -> tuple[MriViolationObject, ...]:
        return tuple(self.object_from_orm(row) for row in db.scalars(self.statement(selection)))


def _value_members() -> tuple[ValueMember[Any, Any, Any], ...]:
    return tuple(member for member in MRI_VIOLATION.members if isinstance(member, ValueMember))


def _integer_keys(keys: frozenset[str], kind: str) -> frozenset[int]:
    try:
        return frozenset(int(key) for key in keys)
    except ValueError as error:
        raise ValueError(f"References to {kind!r} must use integer keys") from error


def register_mri_violation_schema(schema: ResourceSchema) -> None:
    schema.register_object_kind(MRI_VIOLATION)
    schema.register_provider(MriViolationProvider())
    schema.register_member(DICOM_ARCHIVE_KIND, DICOM_ARCHIVE_MRI_VIOLATION)
