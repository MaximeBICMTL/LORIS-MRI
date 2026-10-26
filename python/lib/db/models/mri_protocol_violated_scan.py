from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.mri_protocol_group as db_mri_protocol_group
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbMriProtocolViolatedScan(Base, kw_only=True, eq=False):
    __tablename__ = 'mri_protocol_violated_scans'

    id                       : Mapped[int]             = mapped_column('ID', primary_key=True, init=False)
    candidate_id             : Mapped[int | None]      = mapped_column('CandidateID', ForeignKey('candidate.ID'), default=None)
    pscid                    : Mapped[str | None]      = mapped_column('PSCID', default=None)
    dicom_archive_id         : Mapped[int | None]      = mapped_column('TarchiveID', ForeignKey('tarchive.TarchiveID'), default=None)
    time_run                 : Mapped[datetime | None] = mapped_column('time_run', default=None)
    series_description       : Mapped[str | None]      = mapped_column('series_description', default=None)
    file_path                : Mapped[Path | None]     = mapped_column('minc_location', StringPath, default=None)
    patient_name             : Mapped[str | None]      = mapped_column('PatientName', default=None)
    tr_range                 : Mapped[str | None]      = mapped_column('TR_range', default=None)
    te_range                 : Mapped[str | None]      = mapped_column('TE_range', default=None)
    ti_range                 : Mapped[str | None]      = mapped_column('TI_range', default=None)
    slice_thickness_range    : Mapped[str | None]      = mapped_column('slice_thickness_range', default=None)
    xspace_range             : Mapped[str | None]      = mapped_column('xspace_range', default=None)
    yspace_range             : Mapped[str | None]      = mapped_column('yspace_range', default=None)
    zspace_range             : Mapped[str | None]      = mapped_column('zspace_range', default=None)
    xstep_range              : Mapped[str | None]      = mapped_column('xstep_range', default=None)
    ystep_range              : Mapped[str | None]      = mapped_column('ystep_range', default=None)
    zstep_range              : Mapped[str | None]      = mapped_column('zstep_range', default=None)
    time_range               : Mapped[str | None]      = mapped_column('time_range', default=None)
    series_uid               : Mapped[str | None]      = mapped_column('SeriesUID', default=None)
    image_type               : Mapped[str | None]      = mapped_column('image_type', default=None)
    phase_encoding_direction : Mapped[str | None]      = mapped_column('PhaseEncodingDirection', default=None)
    echo_number              : Mapped[str | None]      = mapped_column('EchoNumber', default=None)
    protocol_group_id        : Mapped[int | None]      \
        = mapped_column('MriProtocolGroupID', ForeignKey('mri_protocol_group.MriProtocolGroupID'), default=None)

    candidate     : Mapped[Optional['db_candidate.DbCandidate']] \
        = relationship('DbCandidate', back_populates='violated_scans', init=False)
    archive       : Mapped[Optional['db_dicom_archive.DbDicomArchive']] \
        = relationship('DbDicomArchive', back_populates='violated_scans', init=False)
    protocol_group: Mapped['db_mri_protocol_group.DbMriProtocolGroup']\
        = relationship('DbMriProtocolGroup', back_populates='violated_scans', init=False)
