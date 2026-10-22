from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioCoordSystemName(Base):
    __tablename__ = 'physiological_coord_system_name'

    id          : Mapped[int]        = mapped_column('PhysiologicalCoordSystemNameID', primary_key=True)
    name        : Mapped[str]        = mapped_column('Name', String(25))
    orientation : Mapped[str | None] = mapped_column('Orientation', String(5))
    origin      : Mapped[str | None] = mapped_column('Origin', String(50))
