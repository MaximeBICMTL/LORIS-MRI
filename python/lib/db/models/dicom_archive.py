from datetime import date, datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.dicom_archive_file as db_dicom_archive_file
import lib.db.models.dicom_archive_series as db_dicom_archive_series
import lib.db.models.file as db_file
import lib.db.models.mri_protocol_violated_scan as db_mri_protocol_violated_scan
import lib.db.models.mri_upload as db_mri_upload
import lib.db.models.mri_violation_log as db_mri_violation_log
import lib.db.models.session as db_session
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbDicomArchive(Base, kw_only=True, eq=False):
    __tablename__ = 'tarchive'

    id                       : Mapped[int]             = mapped_column('TarchiveID', primary_key=True, init=False)
    study_uid                : Mapped[str]             = mapped_column('StudyInstanceUID')
    patient_id               : Mapped[str]             = mapped_column('PatientID', default='')
    patient_name             : Mapped[str]             = mapped_column('PatientName', default='')
    patient_birthdate        : Mapped[date | None]     = mapped_column('PatientDoB', default=None)
    patient_sex              : Mapped[str | None]      = mapped_column('PatientSex', default=None)
    institution_name         : Mapped[str | None]      = mapped_column('InstitutionName', default=None)
    last_update              : Mapped[datetime | None] = mapped_column('LastUpdate', default=None)
    date_acquired            : Mapped[date | None]     = mapped_column('DateAcquired', default=None)
    date_first_archived      : Mapped[datetime | None] = mapped_column('DateFirstArchived', default=None)
    date_last_archived       : Mapped[datetime | None] = mapped_column('DateLastArchived', default=None)
    acquisition_count        : Mapped[int]             = mapped_column('AcquisitionCount', default=0)
    dicom_file_count         : Mapped[int]             = mapped_column('DicomFileCount', default=0)
    non_dicom_file_count     : Mapped[int]             = mapped_column('NonDicomFileCount', default=0)
    md5_sum_dicom_only       : Mapped[str]             = mapped_column('md5sumDicomOnly')
    md5_sum_archive          : Mapped[str]             = mapped_column('md5sumArchive')
    creating_user            : Mapped[str]             = mapped_column('CreatingUser')
    scanner_manufacturer     : Mapped[str | None]      = mapped_column('ScannerManufacturer', default=None)
    scanner_model            : Mapped[str | None]      = mapped_column('ScannerModel', default=None)
    scanner_serial_number    : Mapped[str | None]      = mapped_column('ScannerSerialNumber', default=None)
    scanner_software_version : Mapped[str | None]      = mapped_column('ScannerSoftwareVersion', default=None)
    session_id               : Mapped[int | None]      = mapped_column('SessionID', ForeignKey('session.ID'), default=None)
    create_info              : Mapped[str | None]      = mapped_column('CreateInfo', default=None)

    source_path: Mapped[Path] = mapped_column('SourceLocation', StringPath)
    """
    The path of the source DICOM study directory from which this DICOM archive was imported.
    """

    path: Mapped[Path] = mapped_column('ArchiveLocation', StringPath)
    """
    The path of this DICOM archive relative to the LORIS DICOM archive directory.
    """

    series      : Mapped[list['db_dicom_archive_series.DbDicomArchiveSeries']] \
        = relationship('DbDicomArchiveSeries', back_populates='archive', init=False)
    files       : Mapped[list['db_dicom_archive_file.DbDicomArchiveFile']] \
        = relationship('DbDicomArchiveFile', back_populates='archive', init=False)
    mri_uploads : Mapped[list['db_mri_upload.DbMriUpload']] \
        = relationship('DbMriUpload', back_populates='dicom_archive', init=False)
    session     : Mapped[Optional['db_session.DbSession']] \
        = relationship('DbSession', init=False)
    violated_scans: Mapped[list['db_mri_protocol_violated_scan.DbMriProtocolViolatedScan']] \
        = relationship('DbMriProtocolViolatedScan', back_populates='archive', init=False)
    violations_log       : Mapped[list['db_mri_violation_log.DbMriViolationLog']] \
        = relationship('DbMriViolationLog', back_populates='archive', init=False)

    mri_files: Mapped[list['db_file.DbFile']] = relationship('DbFile', back_populates='dicom_archive', init=False)
    """
    The volumetric MRI files (NIfTI or MINC) generated from this DICOM archive.
    """
