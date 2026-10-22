from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.y_n_bool import YNBool


class DbSite(Base):
    __tablename__ = 'psc'

    id         : Mapped[int]         = mapped_column('CenterID', primary_key=True)
    name       : Mapped[str]         = mapped_column('Name', String(150), default='')
    area       : Mapped[str | None]  = mapped_column('PSCArea', String(150))
    address    : Mapped[str | None]  = mapped_column('Address', String(150))
    city       : Mapped[str | None]  = mapped_column('City', String(150))
    state_id   : Mapped[int | None]  = mapped_column('StateID')
    zip        : Mapped[str | None]  = mapped_column('ZIP', String(12))
    phone_1    : Mapped[str | None]  = mapped_column('Phone1', String(12))
    phone_2    : Mapped[str | None]  = mapped_column('Phone2', String(12))
    contact_1  : Mapped[str | None]  = mapped_column('Contact1', String(150))
    contact_2  : Mapped[str | None]  = mapped_column('Contact2', String(150))
    alias      : Mapped[str]         = mapped_column('Alias', String(4), default='')
    mri_alias  : Mapped[str]         = mapped_column('MRI_alias', String(4), default='')
    account    : Mapped[str | None]  = mapped_column('Account', String(8))
    study_site : Mapped[bool | None] = mapped_column('Study_site', YNBool, default=True)
