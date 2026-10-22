from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbHedSchema(Base):
    __tablename__ = 'hed_schema'

    id          : Mapped[int]        = mapped_column('ID', primary_key=True)
    name        : Mapped[str]        = mapped_column('Name', String(255))
    version     : Mapped[str]        = mapped_column('Version', String(255))
    description : Mapped[str | None] = mapped_column('Description', Text)
    url         : Mapped[str]        = mapped_column('URL', String(255))
