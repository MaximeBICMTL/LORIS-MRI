from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.project as db_project
import lib.db.models.user as db_user
from lib.db.base import Base
from lib.db.decorators.int_bool import IntBool


class DbBidsEventDatasetMapping(Base):
    __tablename__ = 'bids_event_dataset_mapping'

    id                 : Mapped[int]         = mapped_column('ID', primary_key=True)
    project_id         : Mapped[int]         = mapped_column('ProjectID', ForeignKey('Project.ProjectID'))
    property_name      : Mapped[str]         = mapped_column('PropertyName', String(50))
    property_value     : Mapped[str]         = mapped_column('PropertyValue', String(255))
    hed_tag_id         : Mapped[int | None]  = mapped_column('HEDTagID', ForeignKey('hed_schema_nodes.ID'))
    tag_value          : Mapped[str | None]  = mapped_column('TagValue', Text)
    description        : Mapped[str | None]  = mapped_column('Description', Text)
    has_pairing        : Mapped[bool | None] = mapped_column('HasPairing', IntBool, default=False)
    pair_rel_id        : Mapped[int | None]  = mapped_column('PairRelID')
    additional_members : Mapped[int | None]  = mapped_column('AdditionalMembers', default=0)
    tagger_id          : Mapped[int | None]  = mapped_column('TaggedBy', ForeignKey('users.ID'))

    project : Mapped['db_project.DbProject'] = relationship('DbProject')
    tagger  : Mapped['db_user.DbUser | None'] = relationship('DbUser')
