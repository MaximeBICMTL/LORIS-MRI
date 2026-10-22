from sqlalchemy import ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.config_setting as db_config_setting
from lib.db.base import Base


class DbConfig(Base):
    __tablename__ = 'Config'

    id         : Mapped[int]        = mapped_column('ID', primary_key=True)
    setting_id : Mapped[int]        = mapped_column('ConfigID', ForeignKey('ConfigSettings.ID'))
    value      : Mapped[str | None] = mapped_column('Value', Text)

    setting : Mapped['db_config_setting.DbConfigSetting'] = relationship('DbConfigSetting')
