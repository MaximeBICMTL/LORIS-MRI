from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPoint3D(Base, kw_only=True, eq=False):
    __tablename__ = 'point_3d'

    id : Mapped[int]   = mapped_column('Point3DID', primary_key=True, init=False)
    x  : Mapped[float | None] = mapped_column('X', default=None)
    y  : Mapped[float | None] = mapped_column('Y', default=None)
    z  : Mapped[float | None] = mapped_column('Z', default=None)
