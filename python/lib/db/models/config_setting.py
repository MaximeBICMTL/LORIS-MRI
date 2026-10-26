from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.config_category as db_config_category
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbConfigSetting(Base, kw_only=True, eq=False):
    __tablename__ = 'ConfigSettings'

    id             : Mapped[int]         = mapped_column('ID', primary_key=True, init=False)
    name           : Mapped[str]         = mapped_column('Name')
    description    : Mapped[str | None]  = mapped_column('Description', default=None)
    visible        : Mapped[bool | None] = mapped_column('Visible', IntBool, default=False)
    allow_multiple : Mapped[bool | None] = mapped_column('AllowMultiple', IntBool, default=False)
    data_type      : Mapped[str | None]  = mapped_column('DataType', default=None)
    parent_id      : Mapped[int | None]  = mapped_column('Parent', default=None)
    category_id    : Mapped[int | None]  = mapped_column(
        'CategoryID',
        ForeignKey('ConfigCategories.ID'),
        default=None,
    )
    label          : Mapped[str | None]  = mapped_column('Label', default=None)
    order_number   : Mapped[int | None]  = mapped_column('OrderNumber', default=None)
    multilingual   : Mapped[bool | None] = mapped_column('Multilingual', IntBool, default=False)

    category: Mapped['db_config_category.DbConfigCategory | None'] \
        = relationship('DbConfigCategory', back_populates='settings', init=False)
