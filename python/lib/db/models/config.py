from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.config_setting as db_config_setting
from lib.db.base import Base


class DbConfig(Base, kw_only=True, eq=False):
    __tablename__ = 'Config'

    id         : Mapped[int]        = mapped_column('ID', primary_key=True, init=False)
    setting_id : Mapped[int]        = mapped_column('ConfigID', ForeignKey('ConfigSettings.ID'))
    value      : Mapped[str | None] = mapped_column('Value', default=None)

    setting : Mapped['db_config_setting.DbConfigSetting'] = relationship('DbConfigSetting', init=False)
