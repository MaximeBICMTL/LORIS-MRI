from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioElectrodeType(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_electrode_type'

    id   : Mapped[int] = mapped_column('PhysiologicalElectrodeTypeID', primary_key=True, init=False)
    name : Mapped[str] = mapped_column('ElectrodeType')
