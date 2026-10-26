from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.physio_electrode_material as db_physio_electrode_material
import lib.db.models.physio_electrode_type as db_physio_electrode_type
import lib.db.models.point_3d as db_point_3d
from lib.db.base import Base
from lib.db.decorators.string_path import StringPath


class DbPhysioElectrode(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_electrode'

    id          : Mapped[int]         = mapped_column('PhysiologicalElectrodeID', primary_key=True, init=False)
    type_id     : Mapped[int | None]  = mapped_column('PhysiologicalElectrodeTypeID', ForeignKey('physiological_electrode_type.PhysiologicalElectrodeTypeID'), default=None)
    material_id : Mapped[int | None]  = mapped_column('PhysiologicalElectrodeMaterialID', ForeignKey('physiological_electrode_material.PhysiologicalElectrodeMaterialID'), default=None)
    name        : Mapped[str]         = mapped_column('Name')
    point_3d_id : Mapped[int]         = mapped_column('Point3DID', ForeignKey('point_3d.Point3DID'))
    impedance   : Mapped[int | None]  = mapped_column('Impedance', default=None)
    file_path   : Mapped[Path | None] = mapped_column('FilePath', StringPath, default=None)

    type     : Mapped['db_physio_electrode_type.DbPhysioElectrodeType | None']         = relationship('DbPhysioElectrodeType', init=False)
    material : Mapped['db_physio_electrode_material.DbPhysioElectrodeMaterial | None'] = relationship('DbPhysioElectrodeMaterial', init=False)
    point_3d : Mapped['db_point_3d.DbPoint3D']                                         = relationship('DbPoint3D', init=False)
