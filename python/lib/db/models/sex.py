from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbSex(Base):
    __tablename__ = 'sex'

    name : Mapped[str]        = mapped_column('Name', String(255), primary_key=True)
    color: Mapped[str | None] = mapped_column('Colour', String(50))
