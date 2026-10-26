from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioStatusType(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_status_type'

    id   : Mapped[int] = mapped_column('PhysiologicalStatusTypeID', primary_key=True, init=False)
    name : Mapped[str] = mapped_column('ChannelStatus')
