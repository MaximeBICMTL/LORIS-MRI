from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.notification_type as db_notification_type
from lib.db.base import Base
from lib.db.decorators.y_n_bool import YNBool


class DbNotificationSpool(Base, kw_only=True, eq=False):
    __tablename__ = 'notification_spool'

    id           : Mapped[int]             = mapped_column('NotificationID', primary_key=True, init=False)
    type_id      : Mapped[int] \
        = mapped_column('NotificationTypeID', ForeignKey('notification_types.NotificationTypeID'))
    process_id   : Mapped[int | None]      = mapped_column('ProcessID', default=None)
    time_spooled : Mapped[datetime | None] = mapped_column('TimeSpooled', default=None)
    message      : Mapped[str | None]      = mapped_column('Message', default=None)
    error        : Mapped[bool | None]     = mapped_column('Error', YNBool, default=None)
    verbose      : Mapped[bool]            = mapped_column('Verbose', YNBool, default=False)
    sent         : Mapped[bool]            = mapped_column('Sent', YNBool, default=False)
    site_id      : Mapped[int | None]      = mapped_column('CenterID', default=None)
    origin       : Mapped[str | None]      = mapped_column('Origin', default=None)
    active       : Mapped[bool]            = mapped_column('Active', YNBool, default=True)

    type : Mapped['db_notification_type.DbNotificationType'] = relationship('DbNotificationType', init=False)
