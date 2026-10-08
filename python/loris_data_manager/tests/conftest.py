from collections.abc import Iterator

import pytest
from lib.db.base import Base
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


@pytest.fixture
def db_engine() -> Iterator[Engine]:
    """Create an in-memory database containing the LORIS ORM schema."""

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db(db_engine: Engine) -> Iterator[Session]:
    """Create a transaction-scoped database session for a unit test."""

    with Session(db_engine) as session:
        yield session
        session.rollback()
