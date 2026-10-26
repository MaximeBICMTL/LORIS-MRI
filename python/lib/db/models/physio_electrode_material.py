from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioElectrodeMaterial(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_electrode_material'

    id   : Mapped[int] = mapped_column('PhysiologicalElectrodeMaterialID', primary_key=True, init=False)
    name : Mapped[str] = mapped_column('ElectrodeMaterial')
