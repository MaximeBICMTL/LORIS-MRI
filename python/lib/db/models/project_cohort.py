from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.project as db_project
from lib.db.base import Base


class DbProjectCohort(Base, kw_only=True, eq=False):
    __tablename__ = 'project_cohort_rel'

    id         : Mapped[int] = mapped_column('ProjectCohortRelID', primary_key=True, init=False)
    project_id : Mapped[int] = mapped_column('ProjectID', ForeignKey('Project.ProjectID'))
    cohort_id  : Mapped[int] = mapped_column('CohortID')

    project : Mapped['db_project.DbProject'] = relationship('DbProject', init=False)
