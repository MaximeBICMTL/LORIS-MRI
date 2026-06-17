from datetime import datetime
from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.bids_dataset as db_bids_dataset
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbBidsFile(Base):
    """
    A file within a LORIS BIDS dataset.
    """

    __tablename__ = 'bids_file'

    id: Mapped[int] = mapped_column('ID', primary_key=True, autoincrement=True)
    """
    The ID of this BIDS file.
    """

    dataset_id: Mapped[int] = mapped_column('DatasetID', ForeignKey('bids_dataset.ID'))
    """
    The ID of the BIDS dataset to which this file belongs.
    """

    path: Mapped[Path] = mapped_column('Path', StringPath)
    """
    The path of this file relative to its LORIS BIDS dataset.
    """

    source_path: Mapped[Path | None] = mapped_column('SourcePath', StringPath)
    """
    The source path of this file relative to the BIDS dataset from which it was imported.
    """

    insert_time: Mapped[datetime] = mapped_column('InsertTime')
    """
    The time at which this BIDS dataset was created in LORIS.
    """

    blake2b_hash: Mapped[str] = mapped_column('Blake2bHash')
    """
    The BLAKE2b hash of this file.
    """

    dataset: Mapped['db_bids_dataset.DbBidsDataset'] = relationship('DbBidsDataset')
    """
    The BIDS dataset to which this file belongs.
    """
