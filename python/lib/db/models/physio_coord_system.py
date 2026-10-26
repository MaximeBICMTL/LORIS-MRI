from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.bids_file as db_bids_file
import lib.db.models.physio_coord_system_name as db_physio_coord_system_name
import lib.db.models.physio_coord_system_type as db_physio_coord_system_type
import lib.db.models.physio_coord_system_unit as db_physio_coord_system_unit
import lib.db.models.physio_modality as db_physio_modality
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbPhysioCoordSystem(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_coord_system'

    id          : Mapped[int]         = mapped_column('PhysiologicalCoordSystemID', primary_key=True, init=False)
    name_id     : Mapped[int]         = mapped_column('NameID', ForeignKey('physiological_coord_system_name.PhysiologicalCoordSystemNameID'))
    type_id     : Mapped[int]         = mapped_column('TypeID', ForeignKey('physiological_coord_system_type.PhysiologicalCoordSystemTypeID'))
    unit_id     : Mapped[int]         = mapped_column('UnitID', ForeignKey('physiological_coord_system_unit.PhysiologicalCoordSystemUnitID'))
    modality_id : Mapped[int]         = mapped_column('ModalityID', ForeignKey('physiological_modality.PhysiologicalModalityID'))
    file_path   : Mapped[Path | None] = mapped_column('FilePath', StringPath, default=None)
    bids_file_id: Mapped[int | None]  = mapped_column('BidsFileID', ForeignKey('bids_file.ID', ondelete='SET NULL'), default=None)

    name     : Mapped['db_physio_coord_system_name.DbPhysioCoordSystemName'] = relationship('DbPhysioCoordSystemName', init=False)
    type     : Mapped['db_physio_coord_system_type.DbPhysioCoordSystemType'] = relationship('DbPhysioCoordSystemType', init=False)
    unit     : Mapped['db_physio_coord_system_unit.DbPhysioCoordSystemUnit'] = relationship('DbPhysioCoordSystemUnit', init=False)
    modality : Mapped['db_physio_modality.DbPhysioModality']                 = relationship('DbPhysioModality', init=False)
    bids_file: Mapped['db_bids_file.DbBidsFile | None']                      = relationship('DbBidsFile', init=False)
