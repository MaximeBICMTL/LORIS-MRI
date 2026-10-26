from datetime import datetime
from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.bids_file as db_bids_file
import lib.db.models.imaging_file_type as db_imaging_file_type
import lib.db.models.physio_event_parameter as db_physio_event_parameter
import lib.db.models.physio_file as db_physio_file
import lib.db.models.physio_task_event as db_physio_task_event
import lib.db.models.project as db_project
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbPhysioEventFile(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_event_file'

    id             : Mapped[int]         = mapped_column('EventFileID', primary_key=True, init=False)
    physio_file_id : Mapped[int | None]  = mapped_column('PhysiologicalFileID', ForeignKey('physiological_file.PhysiologicalFileID'), default=None)
    project_id     : Mapped[int | None]  = mapped_column('ProjectID', ForeignKey('Project.ProjectID'), default=None)
    file_type      : Mapped[str]         = mapped_column('FileType', ForeignKey('ImagingFileTypes.type'))
    file_path      : Mapped[Path | None] = mapped_column('FilePath', StringPath, default=None)
    last_update    : Mapped[datetime]    = mapped_column('LastUpdate', insert_default=datetime.now, default_factory=datetime.now)
    last_written   : Mapped[datetime]    = mapped_column('LastWritten', insert_default=datetime.now, default_factory=datetime.now)

    bids_info_id: Mapped[int | None] = mapped_column('BidsInfoID', ForeignKey('bids_file.ID', ondelete='SET NULL'), default=None)
    """
    The ID of the BIDS information of this event file, if any.
    """

    physio_file       : Mapped['db_physio_file.DbPhysioFile | None']                     = relationship('DbPhysioFile', init=False)
    project           : Mapped['db_project.DbProject | None']                            = relationship('DbProject', init=False)
    imaging_file_type : Mapped['db_imaging_file_type.DbImagingFileType | None']          = relationship('DbImagingFileType', init=False)
    task_events       : Mapped[list['db_physio_task_event.DbPhysioTaskEvent']]           = relationship('DbPhysioTaskEvent', back_populates='event_file', init=False)
    event_parameters  : Mapped[list['db_physio_event_parameter.DbPhysioEventParameter']] = relationship('DbPhysioEventParameter', back_populates='event_file', init=False)

    bids_info: Mapped['db_bids_file.DbBidsFile | None'] = relationship('DbBidsFile', init=False)
    """
    The BIDS information of this event file, if any.
    """
