from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from lib.db.base import Base
from lib.db.models.candidate import DbCandidate
from lib.db.models.site import DbSite


@pytest.fixture(autouse=True)
def check_database_cleanup(db_server: Engine | None, shared_db_engine: Engine) -> Iterator[None]:
    """Set up before database fixtures and check cleanup after they finish tearing down."""

    if db_server is None:
        yield
        with Session(shared_db_engine) as reader:
            assert reader.scalar(select(DbSite.id)) is None
        return

    with db_server.connect() as connection:
        before = set(connection.exec_driver_sql('SHOW DATABASES').scalars())
    yield
    with db_server.connect() as connection:
        assert set(connection.exec_driver_sql('SHOW DATABASES').scalars()) == before
    with Session(shared_db_engine) as reader:
        assert reader.scalar(select(DbSite.id)) is None


@pytest.mark.fresh_db
@pytest.mark.parametrize('name', ['First test', 'Second test'])
def test_commits_and_schema_changes_are_isolated(
    db_engine: Engine,
    name: str,
):
    # Repeating the same writes must succeed with empty tables and no leftover DDL each time.
    assert not inspect(db_engine).has_table('test_isolation_marker')
    with Session(db_engine) as writer:
        assert writer.scalar(select(DbSite.id)) is None
        writer.add(DbSite(id=1, name=name, alias='TEST', mri_alias='TEST'))
        writer.commit()

    with Session(db_engine) as reader:
        assert reader.scalar(select(DbSite.name).where(DbSite.id == 1)) == name

    with db_engine.begin() as connection:
        connection.exec_driver_sql('CREATE TABLE test_isolation_marker (id INTEGER PRIMARY KEY)')


def test_missing_foreign_key_records_are_rejected(db: Session):
    db.add(DbCandidate(
        cand_id=111111,
        psc_id='DCC001',
        registration_site_id=999,
        registration_project_id=999,
    ))
    with pytest.raises(IntegrityError):
        db.flush()


@pytest.mark.fresh_db
def test_failed_schema_setup_drops_database(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    def fail_schema_setup(*args: object, **kwargs: object):
        raise RuntimeError('Schema setup failed')

    monkeypatch.setattr(Base.metadata, 'create_all', fail_schema_setup)
    with pytest.raises(RuntimeError, match='Schema setup failed'):
        request.getfixturevalue('db_engine')


@pytest.mark.parametrize('name', ['First test', 'Second test'])
def test_session_commits_and_rollbacks_do_not_leak_between_tests(db: Session, name: str):
    assert db.scalar(select(DbSite.id)) is None
    db.add(DbSite(id=1, name=name, alias='TEST', mri_alias='TEST'))
    db.commit()

    db.add(DbSite(id=2, name='Discarded', alias='TEST', mri_alias='TEST'))
    db.flush()
    db.rollback()
    assert db.scalar(select(DbSite.name).where(DbSite.id == 1)) == name
    assert db.get(DbSite, 2) is None

    # A new savepoint must still work after an application-level rollback.
    db.add(DbSite(id=3, name='After rollback', alias='TEST', mri_alias='TEST'))
    db.commit()
