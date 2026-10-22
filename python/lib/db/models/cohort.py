from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbCohort(Base):
    __tablename__ = 'cohort'

    id                 : Mapped[int]         = mapped_column('CohortID', primary_key=True)
    name               : Mapped[str]         = mapped_column('title', String(255))
    use_edc            : Mapped[bool | None] = mapped_column('useEDC', IntBool)
    window_difference  : Mapped[str | None]  = mapped_column('WindowDifference', String(7))
    recruitment_target : Mapped[int | None]  = mapped_column('RecruitmentTarget')
