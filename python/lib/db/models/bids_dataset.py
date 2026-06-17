from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbBidsDataset(Base):
    """
    A LORIS BIDS dataset.
    """

    __tablename__ = 'bids_dataset'

    id: Mapped[int] = mapped_column('ID', primary_key=True, autoincrement=True)
    """
    The ID of this BIDS dataset.
    """

    path: Mapped[Path] = mapped_column('Path', StringPath)
    """
    The path of this BIDS dataset, relative to the LORIS data directory.
    """

    insert_time: Mapped[datetime] = mapped_column('InsertTime')
    """
    The time at which this BIDS dataset was created in LORIS.
    """
