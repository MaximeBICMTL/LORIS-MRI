from pathlib import Path

from sqlalchemy import String, Text
from sqlalchemy.engine import Dialect
from sqlalchemy.types import TypeDecorator, TypeEngine


class StringPath(TypeDecorator[Path]):
    """
    Decorator for a database path type.
    In SQL, the type will appear as a string.
    In Python, the type will appear as a path object.
    """

    impl = Text
    cache_ok = True

    def __init__(self, length: int | None = None):
        super().__init__()
        self.length = length

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[str]:
        return dialect.type_descriptor(Text() if self.length is None else String(self.length))

    def process_bind_param(self, value: Path | None, dialect: Dialect) -> str | None:
        if value is None:
            return None

        return str(value)

    def process_result_value(self, value: str | None, dialect: Dialect) -> Path | None:
        if value is None:
            return None

        return Path(value)
