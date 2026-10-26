from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbSex(Base, kw_only=True, eq=False):
    __tablename__ = 'sex'

    name : Mapped[str]        = mapped_column('Name', primary_key=True)
    color: Mapped[str | None] = mapped_column('Colour', default=None)
