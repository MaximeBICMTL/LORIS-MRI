from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioModality(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_modality'

    id   : Mapped[int] = mapped_column('PhysiologicalModalityID', primary_key=True, init=False)
    name : Mapped[str] = mapped_column('PhysiologicalModality')
