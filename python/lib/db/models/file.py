from datetime import date, datetime
from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.bids_file as db_bids_file
import lib.db.models.dicom_archive as db_dicom_archive
import lib.db.models.file_parameter as db_file_parameter
import lib.db.models.session as db_session
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool
from lib.db.decorators.int_datetime import IntDatetime
from lib.db.decorators.string_path import StringPath


class DbFile(Base, kw_only=True, eq=False):
    __tablename__ = 'files'

    id                             : Mapped[int]          = mapped_column('FileID', primary_key=True, init=False)
    session_id                     : Mapped[int]          = mapped_column('SessionID', ForeignKey('session.ID'))
    path                           : Mapped[Path]         = mapped_column('File', StringPath, insert_default='')
    series_uid                     : Mapped[str | None]   = mapped_column('SeriesUID', default=None)
    echo_time                      : Mapped[float | None] = mapped_column('EchoTime', default=None)
    phase_encoding_direction       : Mapped[str | None]   = mapped_column('PhaseEncodingDirection', default=None)
    echo_number                    : Mapped[str | None]   = mapped_column('EchoNumber', default=None)
    coordinate_space               : Mapped[str | None]   = mapped_column('CoordinateSpace', default=None)
    output_type                    : Mapped[str]          = mapped_column('OutputType', default='')
    scan_type_id                   : Mapped[int | None]   = mapped_column('MriScanTypeID', default=None)
    file_type                      : Mapped[str | None]   = mapped_column('FileType', default=None)
    inserted_by_user_id            : Mapped[str]          = mapped_column('InsertedByUserID', default='')
    insert_time                    : Mapped[datetime]     = mapped_column('InsertTime', IntDatetime, default=datetime.fromtimestamp(0))
    source_pipeline                : Mapped[str | None]   = mapped_column('SourcePipeline', default=None)
    pipeline_date                  : Mapped[date | None]  = mapped_column('PipelineDate', default=None)
    source_file_id                 : Mapped[int | None]   = mapped_column('SourceFileID', default=None)
    process_protocol_id            : Mapped[int | None]   = mapped_column('ProcessProtocolID', default=None)
    caveat                         : Mapped[bool | None]  = mapped_column('Caveat', IntBool, default=None)
    dicom_archive_id               : Mapped[int | None]   = mapped_column('TarchiveSource', ForeignKey('tarchive.TarchiveID'), default=None)
    hrrt_archive_id                : Mapped[int | None]   = mapped_column('HrrtArchiveID', default=None)
    scanner_id                     : Mapped[int | None]   = mapped_column('ScannerID', default=None)
    acquisition_order_per_modality : Mapped[int | None]   = mapped_column('AcqOrderPerModality', default=None)
    acquisition_date               : Mapped[date | None]  = mapped_column('AcquisitionDate', default=None)

    bids_info_id: Mapped[int | None] = mapped_column('BidsInfoID', ForeignKey('bids_file.ID', ondelete='SET NULL'), default=None)
    """
    The ID of the BIDS information of this file, if any.
    """

    bids_info: Mapped['db_bids_file.DbBidsFile | None'] = relationship('DbBidsFile', init=False)
    """
    The BIDS information of this file, if any.
    """

    session: Mapped['db_session.DbSession'] = relationship('DbSession', back_populates='files', init=False)
    """
    The session to which this file belongs.
    """

    parameters: Mapped[list['db_file_parameter.DbFileParameter']] = relationship('DbFileParameter', back_populates='file', init=False)
    """
    The parameters attached to this file.
    """

    dicom_archive: Mapped['db_dicom_archive.DbDicomArchive | None'] = relationship('DbDicomArchive', back_populates='mri_files', init=False)
    """
    The source DICOM archive from which this file was generated.
    """
