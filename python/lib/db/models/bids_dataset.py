from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Mapped, mapped_column

from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbBidsDataset(Base, kw_only=True, eq=False):
    """
    A LORIS BIDS dataset.
    """

    __tablename__ = 'bids_dataset'

    id: Mapped[int] = mapped_column('ID', primary_key=True, autoincrement=True, init=False)
    """
    The ID of this BIDS dataset.
    """

    path: Mapped[Path] = mapped_column('Path', StringPath, unique=True)
    """
    The path of this BIDS dataset, relative to the LORIS data directory.
    """

    insert_time: Mapped[datetime] = mapped_column('InsertTime', insert_default=datetime.now, default_factory=datetime.now)
    """
    The time at which this BIDS dataset was created in LORIS.
    """

    update_time: Mapped[datetime] = mapped_column('UpdateTime', insert_default=datetime.now, default_factory=datetime.now)
    """
    The last time at which this BIDS dataset was updated in LORIS.
    """
