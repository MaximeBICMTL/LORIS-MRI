from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
from lib.db.base import Base


class DbMriScanner(Base, kw_only=True, eq=False):
    __tablename__ = 'mri_scanner'

    id               : Mapped[int]        = mapped_column('ID', primary_key=True, init=False)
    manufacturer     : Mapped[str | None] = mapped_column('Manufacturer', default=None)
    model            : Mapped[str | None] = mapped_column('Model', default=None)
    serial_number    : Mapped[str | None] = mapped_column('Serial_number', default=None)
    software_version : Mapped[str | None] = mapped_column('Software', default=None)
    candidate_id     : Mapped[int | None] = mapped_column('CandidateID', ForeignKey('candidate.ID'), default=None)

    candidate : Mapped['db_candidate.DbCandidate'] = relationship('DbCandidate', init=False)
