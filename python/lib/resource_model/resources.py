"""Physical resources bound to logical LORIS objects."""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import TypeAlias, cast

from sqlalchemy import Table, and_, inspect
from sqlalchemy.sql.elements import ColumnElement

from lib.db.base import Base

DatabaseValue: TypeAlias = object
DatabaseKey: TypeAlias = tuple[tuple[str, DatabaseValue], ...]


@dataclass(frozen=True, slots=True)
class DatabaseRow:
    """Schema-aware identity of one physical database row.

    A deletion backup would later attach a typed snapshot of the row.  The PoC
    intentionally records identity only.
    """

    table: Table
    identity: tuple[DatabaseValue, ...]

    def __post_init__(self) -> None:
        primary_key_size = len(self.table.primary_key.columns)
        if len(self.identity) != primary_key_size:
            raise ValueError(
                f"Table {self.table.fullname!r} has {primary_key_size} primary-key columns, "
                f"but the row identity has {len(self.identity)} values"
            )

    @classmethod
    def from_orm(cls, instance: Base) -> "DatabaseRow":
        """Create a row identity from a persistent, single-table ORM instance."""

        state = inspect(instance)
        if state.identity is None:
            raise ValueError("An ORM instance must be persistent before it can become a database resource")

        mapper_tables = tuple(state.mapper.tables)
        if len(mapper_tables) != 1 or not isinstance(mapper_tables[0], Table):
            raise ValueError(f"ORM model {type(instance).__name__!r} does not map to exactly one physical table")

        table = cast(Table, mapper_tables[0])
        return cls(table=table, identity=tuple(state.identity))

    @property
    def key(self) -> DatabaseKey:
        """Return primary-key column names paired with their values."""

        return tuple(
            (column.name, value) for column, value in zip(self.table.primary_key.columns, self.identity, strict=True)
        )

    def predicate(self) -> ColumnElement[bool]:
        """Build a SQLAlchemy predicate selecting exactly this row."""

        comparisons = (
            column == value for column, value in zip(self.table.primary_key.columns, self.identity, strict=True)
        )
        return and_(*comparisons)


class LocalPathType(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"


@dataclass(frozen=True, slots=True)
class LocalPath:
    """A path relative to a configured LORIS storage root."""

    storage_root: str
    relative_path: PurePosixPath
    path_type: LocalPathType

    def __post_init__(self) -> None:
        if self.relative_path.is_absolute() or ".." in self.relative_path.parts:
            raise ValueError("A resource path must remain within its storage root")


Resource = DatabaseRow | LocalPath
