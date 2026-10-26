from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbCohort(Base, kw_only=True, eq=False):
    __tablename__ = 'cohort'

    id                 : Mapped[int]         = mapped_column('CohortID', primary_key=True, init=False)
    name               : Mapped[str]         = mapped_column('title')
    use_edc            : Mapped[bool | None] = mapped_column('useEDC', IntBool, default=None)
    window_difference  : Mapped[str | None]  = mapped_column('WindowDifference', default=None)
    recruitment_target : Mapped[int | None]  = mapped_column('RecruitmentTarget', default=None)
