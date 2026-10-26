from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.file_parameter as db_file_parameter
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbParameterType(Base, kw_only=True, eq=False):
    __tablename__ = 'parameter_type'

    id               : Mapped[int]          = mapped_column('ParameterTypeID', primary_key=True, init=False)
    name             : Mapped[str]          = mapped_column('Name', default='')
    alias            : Mapped[str | None]   = mapped_column('Alias', default=None)
    data_type        : Mapped[str | None]   = mapped_column('Type', default=None)
    description      : Mapped[str | None]   = mapped_column('Description', default=None)
    range_min        : Mapped[float | None] = mapped_column('RangeMin', default=None)
    range_max        : Mapped[float | None] = mapped_column('RangeMax', default=None)
    source_field     : Mapped[str | None]   = mapped_column('SourceField', default=None)
    source_from      : Mapped[str | None]   = mapped_column('SourceFrom', default=None)
    source_condition : Mapped[str | None]   = mapped_column('SourceCondition', default=None)
    queryable        : Mapped[bool | None]  = mapped_column('Queryable', IntBool, default=True)
    is_file          : Mapped[bool | None]  = mapped_column('IsFile', IntBool, default=False)

    file_parameters: Mapped[list['db_file_parameter.DbFileParameter']] \
        = relationship('DbFileParameter', back_populates='type', init=False)
