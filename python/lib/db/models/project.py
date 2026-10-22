from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbProject(Base):
    __tablename__ = 'Project'

    id                    : Mapped[int]         = mapped_column('ProjectID', primary_key=True)
    name                  : Mapped[str]         = mapped_column('Name', String(255))
    alias                 : Mapped[str]         = mapped_column('Alias', String(4))
    recruitement_target   : Mapped[int | None]  = mapped_column('recruitmentTarget')
    show_summary_on_login : Mapped[bool | None] = mapped_column('showSummaryOnLogin', IntBool, default=True)
