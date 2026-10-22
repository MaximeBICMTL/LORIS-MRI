from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.candidate as db_candidate
from lib.db.base import Base


class DbMriScanner(Base):
    __tablename__ = 'mri_scanner'

    id               : Mapped[int]        = mapped_column('ID', primary_key=True)
    manufacturer     : Mapped[str | None] = mapped_column('Manufacturer', String(255))
    model            : Mapped[str | None] = mapped_column('Model', String(255))
    serial_number    : Mapped[str | None] = mapped_column('Serial_number', String(255))
    software_version : Mapped[str | None] = mapped_column('Software', String(255))
    candidate_id     : Mapped[int | None] = mapped_column('CandidateID', ForeignKey('candidate.ID'))

    candidate : Mapped['db_candidate.DbCandidate'] = relationship('DbCandidate')
