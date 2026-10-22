from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbParameterTypeCategory(Base):
    __tablename__ = 'parameter_type_category'

    id   : Mapped[int]        = mapped_column('ParameterTypeCategoryID', primary_key=True)
    name : Mapped[str | None] = mapped_column('Name', String(255))
    type : Mapped[str | None] = mapped_column('Type', String(10), default='Metavars')
