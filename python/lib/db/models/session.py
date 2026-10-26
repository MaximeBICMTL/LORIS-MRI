from datetime import date, datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
import lib.db.models.file as db_file
import lib.db.models.project as db_project
import lib.db.models.site as db_site
from lib.db.base import Base
from lib.db.decorators.true_false_bool import TrueFalseBool
from lib.db.decorators.y_n_bool import YNBool


class DbSession(Base, kw_only=True, eq=False):
    __tablename__ = 'session'

    id                       : Mapped[int]             = mapped_column('ID', primary_key=True, init=False)
    candidate_id             : Mapped[int]             = mapped_column('CandidateID', ForeignKey('candidate.ID'))
    site_id                  : Mapped[int]             = mapped_column('CenterID', ForeignKey('psc.CenterID'))
    project_id               : Mapped[int]             = mapped_column('ProjectID', ForeignKey('Project.ProjectID'))
    visit_number             : Mapped[int | None]      = mapped_column('VisitNo', default=None)
    visit_label              : Mapped[str]             = mapped_column('Visit_label')
    cohort_id                : Mapped[int | None]      = mapped_column('CohortID', default=None)
    submitted                : Mapped[bool]            = mapped_column('Submitted', YNBool, default=False)
    current_stage            : Mapped[str]             = mapped_column('Current_stage', default='Not Started')
    date_stage_change        : Mapped[date | None]     = mapped_column('Date_stage_change', default=None)
    screening                : Mapped[str | None]      = mapped_column('Screening', default=None)
    date_screening           : Mapped[date | None]     = mapped_column('Date_screening', default=None)
    visit                    : Mapped[str | None]      = mapped_column('Visit', default=None)
    date_visit               : Mapped[date | None]     = mapped_column('Date_visit', default=None)
    date_status_change       : Mapped[date | None]     = mapped_column('Date_status_change', default=None)
    approval                 : Mapped[str | None]      = mapped_column('Approval', default=None)
    date_approval            : Mapped[date | None]     = mapped_column('Date_approval', default=None)
    active                   : Mapped[bool]            = mapped_column('Active', YNBool, default=True)
    date_active              : Mapped[date | None]     = mapped_column('Date_active', default=None)
    registered_by            : Mapped[str | None]      = mapped_column('RegisteredBy', default=None)
    user_id                  : Mapped[str]             = mapped_column('UserID', default='')
    date_registered          : Mapped[date | None]     = mapped_column('Date_registered', default=None)
    test_date                : Mapped[datetime]        = mapped_column('Testdate', insert_default=datetime.now, default_factory=datetime.now)
    hardcopy_request         : Mapped[str]             = mapped_column('Hardcopy_request', default='-')
    bvl_qc_status            : Mapped[str | None]      = mapped_column('BVLQCStatus', default=None)
    bvl_qc_type              : Mapped[str | None]      = mapped_column('BVLQCType', default=None)
    bvl_qc_exclusion         : Mapped[str | None]      = mapped_column('BVLQCExclusion', default=None)
    qcd                      : Mapped[str | None]      = mapped_column('QCd', default=None)
    mri_qc_status            : Mapped[str]             = mapped_column('MRIQCStatus', default='')
    mri_qc_pending           : Mapped[bool]            = mapped_column('MRIQCPending', YNBool, default=False)
    mri_qc_first_change_time : Mapped[datetime | None] = mapped_column('MRIQCFirstChangeTime', default=None)
    mri_qc_last_change_time  : Mapped[datetime | None] = mapped_column('MRIQCLastChangeTime', default=None)
    mri_caveat               : Mapped[bool]            = mapped_column('MRICaveat', TrueFalseBool, default=False)
    language_id              : Mapped[int | None]      = mapped_column('languageID', default=None)

    candidate : Mapped['db_candidate.DbCandidate'] = relationship('DbCandidate', back_populates='sessions', init=False)
    files     : Mapped[list['db_file.DbFile']]     = relationship('DbFile', back_populates='session', init=False)
    project   : Mapped['db_project.DbProject']     = relationship('DbProject', init=False)
    site      : Mapped['db_site.DbSite']           = relationship('DbSite', init=False)
