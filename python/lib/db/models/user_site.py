from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbUserSite(Base, kw_only=True, eq=False):
    """
    Relationship between users and sites.
    """

    __tablename__ = 'user_psc_rel'

    user_id: Mapped[int] = mapped_column('UserID',   ForeignKey('users.ID'),     primary_key=True)
    site_id: Mapped[int] = mapped_column('CenterID', ForeignKey('psc.CenterID'), primary_key=True)
