from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbHedSchemaNode(Base, kw_only=True, eq=False):
    __tablename__ = 'hed_schema_nodes'

    id          : Mapped[int]        = mapped_column('ID', primary_key=True, init=False)
    parent_id   : Mapped[int | None] = mapped_column('ParentID', ForeignKey('hed_schema_nodes.ID'), default=None)
    schema_id   : Mapped[int]        = mapped_column('SchemaID', ForeignKey('hed_schema.ID'))
    name        : Mapped[str]        = mapped_column('Name')
    long_name   : Mapped[str]        = mapped_column('LongName')
    description : Mapped[str]        = mapped_column('Description')
