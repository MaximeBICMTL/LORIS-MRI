from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbParameterTypeCategory(Base, kw_only=True, eq=False):
    __tablename__ = 'parameter_type_category'

    id   : Mapped[int]        = mapped_column('ParameterTypeCategoryID', primary_key=True, init=False)
    name : Mapped[str | None] = mapped_column('Name', default=None)
    type : Mapped[str | None] = mapped_column('Type', default='Metavars')
