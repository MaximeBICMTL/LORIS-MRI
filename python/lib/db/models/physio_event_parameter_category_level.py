from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

import lib.db.models.physio_event_parameter as db_physio_event_parameter
from lib.db.base import Base


class DbPhysioEventParameterCategoryLevel(Base, kw_only=True, eq=False):
    __tablename__ = 'physiological_event_parameter_category_level'

    id                 : Mapped[int]        = mapped_column('CategoricalLevelID', primary_key=True, init=False)
    event_parameter_id : Mapped[int]        = mapped_column('EventParameterID', ForeignKey('physiological_event_parameter.EventParameterID'))
    level_name         : Mapped[str]        = mapped_column('LevelName')
    description        : Mapped[str | None] = mapped_column('Description', default=None)
    hed                : Mapped[str | None] = mapped_column('HED', default=None)

    event_parameter: Mapped['db_physio_event_parameter.DbPhysioEventParameter'] = relationship('DbPhysioEventParameter', back_populates='category_levels', init=False)
