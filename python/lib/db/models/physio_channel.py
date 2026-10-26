from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.physio_channel_type as db_physio_channel_type
import lib.db.models.physio_file as db_physio_file
import lib.db.models.physio_status_type as db_physio_status_type
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbPhysioChannel(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_channel'

    id                 : Mapped[int]            = mapped_column('PhysiologicalChannelID', primary_key=True, init=False)
    physio_file_id     : Mapped[int]            = mapped_column('PhysiologicalFileID', ForeignKey('physiological_file.PhysiologicalFileID'))
    channel_type_id    : Mapped[int]            = mapped_column('PhysiologicalChannelTypeID', ForeignKey('physiological_channel_type.PhysiologicalChannelTypeID'))
    status_type_id     : Mapped[int | None]     = mapped_column('PhysiologicalStatusTypeID', ForeignKey('physiological_status_type.PhysiologicalStatusTypeID'), default=None)
    insert_time        : Mapped[datetime]       = mapped_column('InsertTime', insert_default=datetime.now, default_factory=datetime.now)
    name               : Mapped[str]            = mapped_column('Name')
    description        : Mapped[str | None]     = mapped_column('Description', default=None)
    sampling_frequency : Mapped[int | None]     = mapped_column('SamplingFrequency', default=None)
    low_cutoff         : Mapped[Decimal | None] = mapped_column('LowCutoff', default=None)
    high_cutoff        : Mapped[Decimal | None] = mapped_column('HighCutoff', default=None)
    manual_flag        : Mapped[Decimal | None] = mapped_column('ManualFlag', default=None)
    notch              : Mapped[int | None]     = mapped_column('Notch', default=None)
    reference          : Mapped[str | None]     = mapped_column('Reference', default=None)
    status_description : Mapped[str | None]     = mapped_column('StatusDescription', default=None)
    unit               : Mapped[str | None]     = mapped_column('Unit', default=None)
    file_path          : Mapped[Path | None]    = mapped_column('FilePath', StringPath, default=None)

    physio_file  : Mapped['db_physio_file.DbPhysioFile']                = relationship('DbPhysioFile', back_populates='channels', init=False)
    channel_type : Mapped['db_physio_channel_type.DbPhysioChannelType'] = relationship('DbPhysioChannelType', init=False)
    status_type  : Mapped['db_physio_status_type.DbPhysioStatusType']   = relationship('DbPhysioStatusType', init=False)
