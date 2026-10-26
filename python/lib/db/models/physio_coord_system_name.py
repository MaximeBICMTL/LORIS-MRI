from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioCoordSystemName(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_coord_system_name'

    id          : Mapped[int]        = mapped_column('PhysiologicalCoordSystemNameID', primary_key=True, init=False)
    name        : Mapped[str]        = mapped_column('Name')
    orientation : Mapped[str | None] = mapped_column('Orientation', default=None)
    origin      : Mapped[str | None] = mapped_column('Origin', default=None)
