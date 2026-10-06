"""Concrete database and filesystem objects in the LORIS resource graph."""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any, Generic, Protocol, TypeAlias, TypeVar, cast

from sqlalchemy import Table, and_, inspect
from sqlalchemy.sql.elements import ColumnElement

from lib.db.base import Base

DatabaseValue: TypeAlias = object
DatabaseKey: TypeAlias = tuple[tuple[str, DatabaseValue], ...]
ModelT = TypeVar("ModelT", bound=Base)


@dataclass(frozen=True, slots=True, order=True)
class ObjectRef:
    """Stable identity of one concrete semantic-object instance."""

    kind: str
    key: str

    def __str__(self) -> str:
        return f"{self.kind}:{self.key}"


@dataclass(frozen=True, slots=True)
class DatabaseRowRef:
    """Session-independent, schema-aware identity of one database row."""

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
    def from_orm(cls, instance: Base) -> "DatabaseRowRef":
        """Create a reference from a persistent, single-table ORM instance."""

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

    def __str__(self) -> str:
        key = ",".join(f"{name}={value}" for name, value in self.key)
        return f"database-row:{self.table.fullname}:{key}"


@dataclass(frozen=True, slots=True)
class DatabaseRowObject(Generic[ModelT]):
    """Resolved database-row object backed by an ORM instance in the current session."""

    orm: ModelT

    @property
    def ref(self) -> DatabaseRowRef:
        return DatabaseRowRef.from_orm(self.orm)


class LocalPathType(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"


@dataclass(frozen=True, slots=True)
class LocalPathRef:
    """Stable identity of a path relative to a configured storage root."""

    storage_root: str
    relative_path: PurePosixPath

    def __post_init__(self) -> None:
        if self.relative_path.is_absolute() or ".." in self.relative_path.parts:
            raise ValueError("A resource path must remain within its storage root")

    def __str__(self) -> str:
        return f"local-path:{self.storage_root}:{self.relative_path}"


@dataclass(frozen=True, slots=True)
class LocalPathObject:
    """Resolved local path with the filesystem type expected by its provider."""

    storage_root: str
    relative_path: PurePosixPath
    expected_type: LocalPathType

    def __post_init__(self) -> None:
        LocalPathRef(self.storage_root, self.relative_path)

    @property
    def ref(self) -> LocalPathRef:
        return LocalPathRef(self.storage_root, self.relative_path)


PhysicalObjectRef: TypeAlias = DatabaseRowRef | LocalPathRef
ResourceRef: TypeAlias = ObjectRef | PhysicalObjectRef
PhysicalObject: TypeAlias = DatabaseRowObject[Any] | LocalPathObject


class ResourceObject(Protocol):
    """Small common surface shared by every object stored in a resource graph."""

    @property
    def ref(self) -> ResourceRef: ...
