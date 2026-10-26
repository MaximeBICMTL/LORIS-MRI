from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.file as db_file
import lib.db.models.parameter_type as db_parameter_type
from lib.db.base import Base
from lib.db.decorators.int_datetime import IntDatetime


class DbFileParameter(Base, kw_only=True, eq=False):
    __tablename__ = 'parameter_file'

    id          : Mapped[int]        = mapped_column('ParameterFileID', primary_key=True, init=False)
    file_id     : Mapped[int]        = mapped_column('FileID', ForeignKey('files.FileID'))
    type_id     : Mapped[int]        = mapped_column('ParameterTypeID', ForeignKey('parameter_type.ParameterTypeID'))
    value       : Mapped[str | None] = mapped_column('Value', default=None)
    insert_time : Mapped[datetime]   = mapped_column('InsertTime', IntDatetime, default=datetime.fromtimestamp(0))

    file: Mapped['db_file.DbFile'] \
        = relationship('DbFile', back_populates='parameters', init=False)
    type: Mapped['db_parameter_type.DbParameterType'] \
        = relationship('DbParameterType', back_populates='file_parameters', init=False)
