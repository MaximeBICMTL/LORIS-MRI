from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.parameter_type as db_parameter_type
import lib.db.models.physio_file as db_physio_file
import lib.db.models.project as db_project
from lib.db.base import Base


class DbPhysioFileParameter(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_parameter_file'

    id          : Mapped[int]        = mapped_column('PhysiologicalParameterFileID', primary_key=True, init=False)
    file_id     : Mapped[int | None] = mapped_column('PhysiologicalFileID', ForeignKey('physiological_file.PhysiologicalFileID'), default=None)
    project_id  : Mapped[int | None] = mapped_column('ProjectID', ForeignKey('Project.ProjectID'), default=None)
    type_id     : Mapped[int]        = mapped_column('ParameterTypeID', ForeignKey('parameter_type.ParameterTypeID'))
    insert_time : Mapped[datetime]   = mapped_column('InsertTime', insert_default=datetime.now, default_factory=datetime.now)
    value       : Mapped[str | None] = mapped_column('Value', default=None)

    file    : Mapped['db_physio_file.DbPhysioFile']       = relationship('DbPhysioFile', back_populates='parameters', init=False)
    project : Mapped['db_project.DbProject']              = relationship('DbProject', init=False)
    type    : Mapped['db_parameter_type.DbParameterType'] = relationship('DbParameterType', init=False)
