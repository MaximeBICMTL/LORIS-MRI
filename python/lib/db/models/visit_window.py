from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbVisitWindow(Base, kw_only=True, eq=False):
    __tablename__  = 'Visit_Windows'

    id                   : Mapped[int]        = mapped_column('ID', primary_key=True, init=False)
    visit_label          : Mapped[str | None] = mapped_column('Visit_label', default=None)
    window_min_days      : Mapped[int | None] = mapped_column('WindowMinDays', default=None)
    window_max_days      : Mapped[int | None] = mapped_column('WindowMaxDays', default=None)
    optimum_min_days     : Mapped[int | None] = mapped_column('OptimumMinDays', default=None)
    optimum_max_days     : Mapped[int | None] = mapped_column('OptimumMaxDays', default=None)
    window_midpoint_days : Mapped[int | None] = mapped_column('WindowMidpointDays', default=None)
