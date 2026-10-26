from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.mri_protocol_check_group as db_mri_protocol_check_group
import lib.db.models.mri_scan_type as db_mri_scan_type
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbMriViolationLog(Base, kw_only=True, eq=False):
    __tablename__ = 'mri_violations_log'

    id                      : Mapped[int]              = mapped_column('LogID', primary_key=True, init=False)
    time_run                : Mapped[datetime]         = mapped_column('TimeRun', insert_default=datetime.now, default_factory=datetime.now)
    series_uid              : Mapped[str | None]       = mapped_column('SeriesUID', default=None)
    dicom_archive_id        : Mapped[int | None]       \
        = mapped_column('TarchiveID', ForeignKey('tarchive.TarchiveID'), default=None)
    file_path               : Mapped[Path | None]      = mapped_column('MincFile', StringPath, default=None)
    patient_name            : Mapped[str | None]       = mapped_column('PatientName', default=None)
    candidate_id            : Mapped[int | None]       = mapped_column('CandidateID', ForeignKey('candidate.ID'), default=None)
    visit_label             : Mapped[str | None]       = mapped_column('Visit_label', default=None)
    check_id                : Mapped[int | None]       = mapped_column('CheckID', default=None)
    scan_type_id            : Mapped[int | None]       \
        = mapped_column('MriScanTypeID', ForeignKey('mri_scan_type.MriScanTypeID'), default=None)
    severity                : Mapped[str | None]       = mapped_column('Severity', default=None)
    header                  : Mapped[str | None]       = mapped_column('Header', default=None)
    value                   : Mapped[str | None]       = mapped_column('Value', default=None)
    valid_range             : Mapped[str | None]       = mapped_column('ValidRange', default=None)
    valid_regex             : Mapped[str | None]       = mapped_column('ValidRegex', default=None)
    echo_time               : Mapped[float | None]     = mapped_column('EchoTime', default=None)
    phase_encoding_direction: Mapped[str | None]       = mapped_column('PhaseEncodingDirection', default=None)
    echo_number             : Mapped[str | None]       = mapped_column('EchoNumber', default=None)
    protocol_check_group_id : Mapped[int | None]       \
        = mapped_column('MriProtocolChecksGroupID', ForeignKey('mri_protocol_checks_group.MriProtocolChecksGroupID'), default=None)

    archive              : Mapped[Optional['db_dicom_archive.DbDicomArchive']] \
        = relationship('DbDicomArchive', back_populates='violations_log', init=False)
    candidate            : Mapped[Optional['db_candidate.DbCandidate']] \
        = relationship('DbCandidate', back_populates='violations_log', init=False)
    scan_type            : Mapped[Optional['db_mri_scan_type.DbMriScanType']] \
        = relationship('DbMriScanType', back_populates='violations_log', init=False)
    protocol_check_group : Mapped[Optional['db_mri_protocol_check_group.DbMriProtocolCheckGroup']] \
        = relationship('DbMriProtocolCheckGroup', back_populates='violations_log', init=False)
