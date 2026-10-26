from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.session as db_session
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool
from lib.db.decorators.string_path import StringPath
from lib.db.decorators.y_n_bool import YNBool


class DbMriUpload(Base, kw_only=True, eq=False):
    __tablename__ = 'mri_upload'

    id                          : Mapped[int]             = mapped_column('UploadID', primary_key=True, init=False)
    uploaded_by                 : Mapped[str]             = mapped_column('UploadedBy', default='')
    upload_date                 : Mapped[datetime | None] = mapped_column('UploadDate', default=None)
    upload_path                 : Mapped[Path]            = mapped_column('UploadLocation', StringPath, insert_default='')
    decompressed_path           : Mapped[Path]            = mapped_column('DecompressedLocation', StringPath, insert_default='')
    insertion_complete          : Mapped[bool]            = mapped_column('InsertionComplete', IntBool, default=False)
    inserting                   : Mapped[bool | None]     = mapped_column('Inserting', IntBool, default=None)
    patient_name                : Mapped[str]             = mapped_column('PatientName', default='')
    number_of_minc_inserted     : Mapped[int | None]      = mapped_column('number_of_mincInserted', default=None)
    number_of_minc_created      : Mapped[int | None]      = mapped_column('number_of_mincCreated', default=None)
    dicom_archive_id            : Mapped[int | None] \
        = mapped_column('TarchiveID', ForeignKey('tarchive.TarchiveID'), default=None)
    session_id                  : Mapped[int | None]      = mapped_column('SessionID', ForeignKey('session.ID'), default=None)
    is_candidate_info_validated : Mapped[bool | None]     = mapped_column('IsCandidateInfoValidated', IntBool, default=None)
    is_dicom_archive_validated  : Mapped[bool]            = mapped_column('IsTarchiveValidated', IntBool, default=False)
    is_phantom                  : Mapped[bool]            = mapped_column('IsPhantom', YNBool, default=False)

    dicom_archive : Mapped[Optional['db_dicom_archive.DbDicomArchive']] \
        = relationship('DbDicomArchive', back_populates='mri_uploads', init=False)
    session       : Mapped[Optional['db_session.DbSession']] \
        = relationship('DbSession', init=False)
