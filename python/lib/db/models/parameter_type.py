from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.file_parameter as db_file_parameter
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbParameterType(Base):
    __tablename__ = 'parameter_type'

    id               : Mapped[int]          = mapped_column('ParameterTypeID', primary_key=True)
    name             : Mapped[str]          = mapped_column('Name', String(255), default='')
    alias            : Mapped[str | None]   = mapped_column('Alias', String(255))
    data_type        : Mapped[str | None]   = mapped_column('Type', Text)
    description      : Mapped[str | None]   = mapped_column('Description', Text)
    range_min        : Mapped[float | None] = mapped_column('RangeMin')
    range_max        : Mapped[float | None] = mapped_column('RangeMax')
    source_field     : Mapped[str | None]   = mapped_column('SourceField', Text)
    source_from      : Mapped[str | None]   = mapped_column('SourceFrom', String(255))
    source_condition : Mapped[str | None]   = mapped_column('SourceCondition', Text)
    queryable        : Mapped[bool | None]  = mapped_column('Queryable', IntBool, default=True)
    is_file          : Mapped[bool | None]  = mapped_column('IsFile', IntBool, default=False)

    file_parameters: Mapped[list['db_file_parameter.DbFileParameter']] \
        = relationship('DbFileParameter', back_populates='type')
