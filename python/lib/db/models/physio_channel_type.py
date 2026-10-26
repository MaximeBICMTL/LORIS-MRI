from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base


class DbPhysioChannelType(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_channel_type'

    id          : Mapped[int]        = mapped_column('PhysiologicalChannelTypeID', primary_key=True, init=False)
    name        : Mapped[str]        = mapped_column('ChannelTypeName')
    description : Mapped[str | None] = mapped_column('ChannelDescription', default=None)
