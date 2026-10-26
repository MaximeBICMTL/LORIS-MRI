from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.dicom_archive_series as db_dicom_archive_series
from lib.db.base import Base


class DbDicomArchiveFile(Base, kw_only=True, eq=False):
    __tablename__ = 'tarchive_files'

    id                 : Mapped[int]        = mapped_column('TarchiveFileID', primary_key=True, init=False)
    archive_id         : Mapped[int]        = mapped_column('TarchiveID', ForeignKey('tarchive.TarchiveID'))
    series_id          : Mapped[int | None] \
        = mapped_column('TarchiveSeriesID', ForeignKey('tarchive_series.TarchiveSeriesID'), default=None)
    series_number      : Mapped[int | None] = mapped_column('SeriesNumber', default=None)
    series_description : Mapped[str | None] = mapped_column('SeriesDescription', default=None)
    file_number        : Mapped[int | None] = mapped_column('FileNumber', default=None)
    echo_number        : Mapped[int | None] = mapped_column('EchoNumber', default=None)
    md5_sum            : Mapped[str]        = mapped_column('Md5Sum')
    file_name          : Mapped[str]        = mapped_column('FileName')

    archive : Mapped['db_dicom_archive.DbDicomArchive'] \
        = relationship('DbDicomArchive', back_populates='files', init=False)
    series  : Mapped[Optional['db_dicom_archive_series.DbDicomArchiveSeries']] \
        = relationship('DbDicomArchiveSeries', back_populates='files', init=False)
