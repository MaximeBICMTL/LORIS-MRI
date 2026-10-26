from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.y_n_bool import YNBool


class DbSite(Base, kw_only=True, eq=False):
    __tablename__ = 'psc'

    id         : Mapped[int]         = mapped_column('CenterID', primary_key=True, init=False)
    name       : Mapped[str]         = mapped_column('Name', default='')
    area       : Mapped[str | None]  = mapped_column('PSCArea', default=None)
    address    : Mapped[str | None]  = mapped_column('Address', default=None)
    city       : Mapped[str | None]  = mapped_column('City', default=None)
    state_id   : Mapped[int | None]  = mapped_column('StateID', default=None)
    zip        : Mapped[str | None]  = mapped_column('ZIP', default=None)
    phone_1    : Mapped[str | None]  = mapped_column('Phone1', default=None)
    phone_2    : Mapped[str | None]  = mapped_column('Phone2', default=None)
    contact_1  : Mapped[str | None]  = mapped_column('Contact1', default=None)
    contact_2  : Mapped[str | None]  = mapped_column('Contact2', default=None)
    alias      : Mapped[str]         = mapped_column('Alias', default='')
    mri_alias  : Mapped[str]         = mapped_column('MRI_alias', default='')
    account    : Mapped[str | None]  = mapped_column('Account', default=None)
    study_site : Mapped[bool | None] = mapped_column('Study_site', YNBool, default=True)
