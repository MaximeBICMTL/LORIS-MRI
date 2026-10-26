from datetime import datetime
from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.bids_file as db_bids_file
import lib.db.models.meg_ctf_head_shape_file as db_meg_ctf_head_shape_file
import lib.db.models.physio_channel as db_physio_channel
import lib.db.models.physio_event_archive as db_physio_event_archive
import lib.db.models.physio_event_file as db_physio_event_file
import lib.db.models.physio_file_archive as db_physio_file_archive
import lib.db.models.physio_file_parameter as db_phyiso_file_parameter
import lib.db.models.physio_modality as db_physio_modality
import lib.db.models.physio_output_type as db_physio_output_type
import lib.db.models.physio_task_event as db_physio_task_event
import lib.db.models.session as db_session
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbPhysioFile(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_file'

    id               : Mapped[int]             = mapped_column('PhysiologicalFileID', primary_key=True, init=False)
    modality_id      : Mapped[int | None]      = mapped_column('PhysiologicalModalityID', ForeignKey('physiological_modality.PhysiologicalModalityID'), default=None)
    output_type_id   : Mapped[int]             = mapped_column('PhysiologicalOutputTypeID', ForeignKey('physiological_output_type.PhysiologicalOutputTypeID'))
    session_id       : Mapped[int]             = mapped_column('SessionID', ForeignKey('session.ID'))
    insert_time      : Mapped[datetime]        = mapped_column('InsertTime', insert_default=datetime.now, default_factory=datetime.now)
    type             : Mapped[str | None]      = mapped_column('FileType', default=None)
    acquisition_time : Mapped[datetime | None] = mapped_column('AcquisitionTime', default=None)
    inserted_by_user : Mapped[str]             = mapped_column('InsertedByUser')
    index            : Mapped[int | None]      = mapped_column('Index', default=None)
    parent_id        : Mapped[int | None]      = mapped_column('ParentID', default=None)

    path: Mapped[Path] = mapped_column('FilePath', StringPath)
    """
    The path of this physiological file relative to the LORIS data directory. The file may notably
    be a directory for MEG CTF data.
    """

    bids_info_id: Mapped[int | None] = mapped_column('BidsInfoID', ForeignKey('bids_file.ID', ondelete='SET NULL'), default=None)
    """
    The ID of the BIDS information of this file, if any.
    """

    head_shape_file_id: Mapped[int | None] = mapped_column('HeadShapeFileID', ForeignKey('meg_ctf_head_shape_file.ID', ondelete='SET NULL'), default=None)
    """
    ID of the head shape file associated to this file, which is only present for MEG CTF files.
    """

    output_type   : Mapped['db_physio_output_type.DbPhysioOutputType']             = relationship('DbPhysioOutputType', init=False)
    modality      : Mapped['db_physio_modality.DbPhysioModality | None']           = relationship('DbPhysioModality', init=False)
    session       : Mapped['db_session.DbSession']                                 = relationship('DbSession', init=False)
    parameters    : Mapped[list['db_phyiso_file_parameter.DbPhysioFileParameter']] = relationship('DbPhysioFileParameter', back_populates='file', init=False)
    archive       : Mapped['db_physio_file_archive.DbPhysioFileArchive | None']    = relationship('DbPhysioFileArchive', back_populates='physio_file', init=False)
    event_archive : Mapped['db_physio_event_archive.DbPhysioEventArchive | None']  = relationship('DbPhysioEventArchive', back_populates='physio_file', init=False)
    channels      : Mapped[list['db_physio_channel.DbPhysioChannel']]              = relationship('DbPhysioChannel', back_populates='physio_file', init=False)
    event_files   : Mapped[list['db_physio_event_file.DbPhysioEventFile']]         = relationship('DbPhysioEventFile', back_populates='physio_file', init=False)
    task_events   : Mapped[list['db_physio_task_event.DbPhysioTaskEvent']]         = relationship('DbPhysioTaskEvent', back_populates='physio_file', init=False)

    bids_info: Mapped['db_bids_file.DbBidsFile | None'] = relationship('DbBidsFile', init=False)
    """
    The BIDS information of this file, if any.
    """

    head_shape_file: Mapped['db_meg_ctf_head_shape_file.DbMegCtfHeadShapeFile | None'] = relationship('DbMegCtfHeadShapeFile', init=False)
    """
    The head shape file associated to this file, which is only present for MEG CTF files.
    """
