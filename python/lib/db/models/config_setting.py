from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.config_category as db_config_category
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbConfigSetting(Base):
    __tablename__ = 'ConfigSettings'

    id             : Mapped[int]         = mapped_column('ID', primary_key=True)
    name           : Mapped[str]         = mapped_column('Name', String(255))
    description    : Mapped[str | None]  = mapped_column('Description', String(255))
    visible        : Mapped[bool | None] = mapped_column('Visible', IntBool, default=False)
    allow_multiple : Mapped[bool | None] = mapped_column('AllowMultiple', IntBool, default=False)
    data_type      : Mapped[str | None]  = mapped_column('DataType', Text)
    parent_id      : Mapped[int | None]  = mapped_column('Parent')
    category_id    : Mapped[int | None]  = mapped_column(
        'CategoryID',
        ForeignKey('ConfigCategories.ID'),
    )
    label          : Mapped[str | None]  = mapped_column('Label', String(255))
    order_number   : Mapped[int | None]  = mapped_column('OrderNumber')
    multilingual   : Mapped[bool | None] = mapped_column('Multilingual', IntBool, default=False)

    category: Mapped['db_config_category.DbConfigCategory | None'] \
        = relationship('DbConfigCategory', back_populates='settings')
