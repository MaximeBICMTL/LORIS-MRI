from datetime import date

from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.project as db_project
import lib.db.models.site as db_site
import lib.db.models.user_project  # type: ignore  # ruff:ignore[unused-import]
import lib.db.models.user_site  # type: ignore  # ruff:ignore[unused-import]
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool
from lib.db.decorators.y_n_bool import YNBool


class DbUser(Base, kw_only=True, eq=False):
    __tablename__ = 'users'

    id                       : Mapped[int]          = mapped_column('ID', primary_key=True, init=False)
    username                 : Mapped[str]          = mapped_column('UserID', default='')
    password                 : Mapped[str | None]   = mapped_column('Password', default=None)
    real_name                : Mapped[str | None]   = mapped_column('Real_name', default=None)
    first_name               : Mapped[str | None]   = mapped_column('First_name', default=None)
    last_name                : Mapped[str | None]   = mapped_column('Last_name', default=None)
    degree                   : Mapped[str | None]   = mapped_column('Degree', default=None)
    position_title           : Mapped[str | None]   = mapped_column('Position_title', default=None)
    institution              : Mapped[str | None]   = mapped_column('Institution', default=None)
    department               : Mapped[str | None]   = mapped_column('Department', default=None)
    address                  : Mapped[str | None]   = mapped_column('Address', default=None)
    city                     : Mapped[str | None]   = mapped_column('City', default=None)
    state                    : Mapped[str | None]   = mapped_column('State', default=None)
    zip_code                 : Mapped[str | None]   = mapped_column('Zip_code', default=None)
    country                  : Mapped[str | None]   = mapped_column('Country', default=None)
    phone                    : Mapped[str | None]   = mapped_column('Phone', default=None)
    fax                      : Mapped[str | None]   = mapped_column('Fax', default=None)
    email                    : Mapped[str]          = mapped_column('Email', default='')
    privilege                : Mapped[bool]         = mapped_column('Privilege', IntBool, default=False)
    pscpi                    : Mapped[bool]         = mapped_column('PSCPI', YNBool, default=False)
    db_access                : Mapped[str]          = mapped_column('DBAccess', default='')
    active                   : Mapped[bool]         = mapped_column('Active', YNBool, default=True)
    password_hash            : Mapped[str | None]   = mapped_column('Password_hash', default=None)
    password_change_required : Mapped[bool]         = mapped_column('PasswordChangeRequired', IntBool, default=False)
    totp_secret              : Mapped[bytes | None] = mapped_column('TOTPSecret', default=None)
    pending_approval         : Mapped[bool | None]  = mapped_column('Pending_approval', YNBool, default=True)
    doc_repo_notifications   : Mapped[bool | None]  = mapped_column('Doc_Repo_Notifications', YNBool, default=False)
    language_preference      : Mapped[int | None]   = mapped_column('language_preference', default=None)
    active_from              : Mapped[date | None]  = mapped_column('active_from', default=None)
    active_to                : Mapped[date | None]  = mapped_column('active_to', default=None)
    account_request_date     : Mapped[date | None]  = mapped_column('account_request_date', default=None)

    projects: Mapped[list['db_project.DbProject']] = relationship('DbProject', secondary='user_project_rel', init=False)
    """
    The projects to which this user belongs to.
    """

    sites: Mapped[list['db_site.DbSite']] = relationship('DbSite', secondary='user_psc_rel', init=False)
    """
    The sites to which this user belongs to.
    """
