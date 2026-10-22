from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.mri_protocol_group as db_mri_protocol_group
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbMriProtocolViolatedScan(Base):
    __tablename__ = 'mri_protocol_violated_scans'

    id                       : Mapped[int]             = mapped_column('ID', primary_key=True)
    candidate_id             : Mapped[int | None]      = mapped_column('CandidateID', ForeignKey('candidate.ID'))
    pscid                    : Mapped[str | None]      = mapped_column('PSCID', String(255))
    dicom_archive_id         : Mapped[int | None]      = mapped_column('TarchiveID', ForeignKey('tarchive.TarchiveID'))
    time_run                 : Mapped[datetime | None] = mapped_column('time_run')
    series_description       : Mapped[str | None]      = mapped_column('series_description', String(255))
    file_path                : Mapped[Path | None]     = mapped_column('minc_location', StringPath(255))
    patient_name             : Mapped[str | None]      = mapped_column('PatientName', String(255))
    tr_range                 : Mapped[str | None]      = mapped_column('TR_range', String(255))
    te_range                 : Mapped[str | None]      = mapped_column('TE_range', String(255))
    ti_range                 : Mapped[str | None]      = mapped_column('TI_range', String(255))
    slice_thickness_range    : Mapped[str | None]      = mapped_column('slice_thickness_range', String(255))
    xspace_range             : Mapped[str | None]      = mapped_column('xspace_range', String(255))
    yspace_range             : Mapped[str | None]      = mapped_column('yspace_range', String(255))
    zspace_range             : Mapped[str | None]      = mapped_column('zspace_range', String(255))
    xstep_range              : Mapped[str | None]      = mapped_column('xstep_range', String(255))
    ystep_range              : Mapped[str | None]      = mapped_column('ystep_range', String(255))
    zstep_range              : Mapped[str | None]      = mapped_column('zstep_range', String(255))
    time_range               : Mapped[str | None]      = mapped_column('time_range', String(255))
    series_uid               : Mapped[str | None]      = mapped_column('SeriesUID', String(64))
    image_type               : Mapped[str | None]      = mapped_column('image_type', String(255))
    phase_encoding_direction : Mapped[str | None]      = mapped_column('PhaseEncodingDirection', String(3))
    echo_number              : Mapped[str | None]      = mapped_column('EchoNumber', String(20))
    protocol_group_id        : Mapped[int | None]      \
        = mapped_column('MriProtocolGroupID', ForeignKey('mri_protocol_group.MriProtocolGroupID'))

    candidate     : Mapped[Optional['db_candidate.DbCandidate']] \
        = relationship('DbCandidate', back_populates='violated_scans')
    archive       : Mapped[Optional['db_dicom_archive.DbDicomArchive']] \
        = relationship('DbDicomArchive', back_populates='violated_scans')
    protocol_group: Mapped['db_mri_protocol_group.DbMriProtocolGroup']\
        = relationship('DbMriProtocolGroup', back_populates='violated_scans')
