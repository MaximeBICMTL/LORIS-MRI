from datetime import date

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.project as db_project
import lib.db.models.site as db_site
import lib.db.models.user_project  # type: ignore  # ruff:ignore[unused-import]
import lib.db.models.user_site  # type: ignore  # ruff:ignore[unused-import]
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool
from lib.db.decorators.y_n_bool import YNBool


class DbUser(Base):
    __tablename__ = 'users'

    id                       : Mapped[int]          = mapped_column('ID', primary_key=True)
    username                 : Mapped[str]          = mapped_column('UserID', String(255), default='')
    password                 : Mapped[str | None]   = mapped_column('Password', String(255))
    real_name                : Mapped[str | None]   = mapped_column('Real_name', String(255))
    first_name               : Mapped[str | None]   = mapped_column('First_name', String(255))
    last_name                : Mapped[str | None]   = mapped_column('Last_name', String(255))
    degree                   : Mapped[str | None]   = mapped_column('Degree', String(255))
    position_title           : Mapped[str | None]   = mapped_column('Position_title', String(255))
    institution              : Mapped[str | None]   = mapped_column('Institution', String(255))
    department               : Mapped[str | None]   = mapped_column('Department', String(255))
    address                  : Mapped[str | None]   = mapped_column('Address', String(255))
    city                     : Mapped[str | None]   = mapped_column('City', String(255))
    state                    : Mapped[str | None]   = mapped_column('State', String(255))
    zip_code                 : Mapped[str | None]   = mapped_column('Zip_code', String(255))
    country                  : Mapped[str | None]   = mapped_column('Country', String(255))
    phone                    : Mapped[str | None]   = mapped_column('Phone', String(15))
    fax                      : Mapped[str | None]   = mapped_column('Fax', String(255))
    email                    : Mapped[str]          = mapped_column('Email', String(255), default='')
    privilege                : Mapped[bool]         = mapped_column('Privilege', IntBool, default=False)
    pscpi                    : Mapped[bool]         = mapped_column('PSCPI', YNBool, default=False)
    db_access                : Mapped[str]          = mapped_column('DBAccess', String(10), default='')
    active                   : Mapped[bool]         = mapped_column('Active', YNBool, default=True)
    password_hash            : Mapped[str | None]   = mapped_column('Password_hash', String(255))
    password_change_required : Mapped[bool]         = mapped_column('PasswordChangeRequired', IntBool, default=False)
    totp_secret              : Mapped[bytes | None] = mapped_column('TOTPSecret')
    pending_approval         : Mapped[bool | None]  = mapped_column('Pending_approval', YNBool, default=True)
    doc_repo_notifications   : Mapped[bool | None]  = mapped_column('Doc_Repo_Notifications', YNBool, default=False)
    language_preference      : Mapped[int | None]   = mapped_column('language_preference')
    active_from              : Mapped[date | None]  = mapped_column('active_from')
    active_to                : Mapped[date | None]  = mapped_column('active_to')
    account_request_date     : Mapped[date | None]  = mapped_column('account_request_date')

    projects: Mapped[list['db_project.DbProject']] = relationship('DbProject', secondary='user_project_rel')
    """
    The projects to which this user belongs to.
    """

    sites: Mapped[list['db_site.DbSite']] = relationship('DbSite', secondary='user_psc_rel')
    """
    The sites to which this user belongs to.
    """
