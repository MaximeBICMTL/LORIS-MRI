from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.mri_protocol_check as db_mri_protocol_check
import lib.db.models.mri_violation_log as db_mri_violation_log
from lib.db.base import Base


class DbMriScanType(Base, kw_only=True, eq=False):
    __tablename__ = 'mri_scan_type'

    id   : Mapped[int] = mapped_column('MriScanTypeID', primary_key=True, init=False)
    name : Mapped[str] = mapped_column('MriScanTypeName')

    protocol_checks : Mapped[list['db_mri_protocol_check.DbMriProtocolCheck']] \
        = relationship('DbMriProtocolCheck', back_populates='scan_type', init=False)
    violations_log  : Mapped[list['db_mri_violation_log.DbMriViolationLog']] \
        = relationship('DbMriViolationLog', back_populates='scan_type', init=False)
