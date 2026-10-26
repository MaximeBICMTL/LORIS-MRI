from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbProject(Base, kw_only=True, eq=False):
    __tablename__ = 'Project'

    id                    : Mapped[int]         = mapped_column('ProjectID', primary_key=True, init=False)
    name                  : Mapped[str]         = mapped_column('Name')
    alias                 : Mapped[str]         = mapped_column('Alias')
    recruitement_target   : Mapped[int | None]  = mapped_column('recruitmentTarget', default=None)
    show_summary_on_login : Mapped[bool | None] = mapped_column('showSummaryOnLogin', IntBool, default=True)
