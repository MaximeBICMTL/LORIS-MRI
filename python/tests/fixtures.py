import importlib
import pkgutil
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import URL, Engine, create_engine
from sqlalchemy.orm import Session

import lib.db.models
from lib.db.base import Base
from lib.db.models.candidate import DbCandidate
from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from lib.env import Env


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        '--db-backend',
        choices=('mariadb', 'sqlite'),
        default='mariadb',
        help='Unit-test database backend (SQLite is available for performance comparisons).',
    )
    parser.addoption(
        '--db-isolation',
        choices=('rollback', 'fresh'),
        default='rollback',
        help='Roll back each test in a shared schema, or create a fresh database for every test.',
    )


def _uses_fresh_database(request: pytest.FixtureRequest) -> bool:
    return (
        'fresh_db' in request.keywords
        or request.config.getoption('--db-isolation') == 'fresh'
    )


@pytest.fixture(scope='session')
def db_server(request: pytest.FixtureRequest) -> Iterator[Engine | None]:
    """
    Start one disposable MariaDB server for the test run, only when a test needs a database.
    """

    # Explicitly load every model so the schema does not depend on which tests were collected.
    for module in pkgutil.iter_modules(lib.db.models.__path__):
        importlib.import_module(f'{lib.db.models.__name__}.{module.name}')

    if request.config.getoption('--db-backend') == 'sqlite':
        yield None
        return

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import HealthcheckWaitStrategy

    container = (
        DockerContainer('mariadb:latest')
        .with_env('MARIADB_ROOT_PASSWORD', 'test')
        .with_env('MARIADB_ROOT_HOST', '%')
        .with_exposed_ports(3306)
        .with_kwargs(healthcheck={
            'test': ['CMD', 'healthcheck.sh', '--connect', '--innodb_initialized'],
            'interval': 1_000_000_000,
            'timeout': 5_000_000_000,
            'retries': 120,
        })
        .waiting_for(HealthcheckWaitStrategy().with_startup_timeout(120))
    )
    with container:
        host = container.get_container_host_ip()
        # mysqlclient treats localhost as a Unix socket and would ignore the mapped TCP port.
        if host == 'localhost':
            host = '127.0.0.1'
        url = URL.create(
            'mysql+mysqldb',
            username='root',
            password='test',
            host=host,
            port=int(container.get_exposed_port(3306)),
        )
        server = create_engine(url, isolation_level='AUTOCOMMIT')
        try:
            with server.connect() as connection:
                print(f'MariaDB test server: {connection.exec_driver_sql("SELECT VERSION()").scalar_one()}')
            yield server
        finally:
            server.dispose()


@contextmanager
def _create_database(db_server: Engine | None) -> Generator[Engine, None, None]:
    """Create a disposable ORM database for either isolation strategy."""

    if db_server is None:
        engine = create_engine('sqlite:///:memory:')
        try:
            with engine.connect() as connection:
                connection.exec_driver_sql('PRAGMA foreign_keys=ON')
            Base.metadata.create_all(engine, checkfirst=False)
            yield engine
        finally:
            engine.dispose()
        return

    # The generated name is unique across tests/workers and contains only safe SQL identifier chars.
    database = f'pytest_{uuid4().hex}'
    with db_server.connect() as connection:
        connection.exec_driver_sql(
            f'CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
        )

    engine = create_engine(db_server.url.set(database=database), isolation_level='READ COMMITTED')
    try:
        Base.metadata.create_all(engine, checkfirst=False)
        yield engine
    finally:
        engine.dispose()
        with db_server.connect() as connection:
            connection.exec_driver_sql(f'DROP DATABASE `{database}`')


@pytest.fixture(scope='session')
def shared_db_engine(db_server: Engine | None) -> Iterator[Engine]:
    """Create the ORM schema once per test session/worker."""

    with _create_database(db_server) as engine:
        yield engine


@pytest.fixture
def fresh_db_engine(db_server: Engine | None) -> Iterator[Engine]:
    """Create a disposable database for tests that need real commits or schema changes."""

    with _create_database(db_server) as engine:
        yield engine


@pytest.fixture
def db_engine(request: pytest.FixtureRequest) -> Engine:
    """Select a shared schema by default, or a fresh database for tests marked fresh_db."""

    fixture = 'fresh_db_engine' if _uses_fresh_database(request) else 'shared_db_engine'
    return cast(Engine, request.getfixturevalue(fixture))


@pytest.fixture
def db(request: pytest.FixtureRequest, db_engine: Engine) -> Iterator[Session]:
    """
    Isolate a test with a savepoint inside an outer transaction, or use its fresh database.
    """

    if _uses_fresh_database(request):
        with Session(db_engine) as db:
            yield db
        return

    with db_engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Python 3.11's sqlite3 legacy mode does not emit BEGIN before a SAVEPOINT.
            if db_engine.dialect.name == 'sqlite':
                connection.exec_driver_sql('BEGIN')
            with Session(connection, join_transaction_mode='create_savepoint') as db:
                yield db
        finally:
            transaction.rollback()


@pytest.fixture
def env(db_engine: Engine, db: Session, tmp_path: Path) -> Env:
    """
    Create a LORIS environment using the test's database isolation strategy.
    """

    return Env(
        db_engine     = db_engine,
        db            = db,
        script_name   = 'pytest',
        config_info   = SimpleNamespace(),
        tmp_dir_path  = tmp_path,
        log_file_path = tmp_path / 'test.log',
        verbose       = False,
        cleanups      = [],
    )


@pytest.fixture
def site(db: Session) -> DbSite:
    site = DbSite(name='Test site', alias='TEST', mri_alias='TEST')
    db.add(site)
    db.flush()
    return site


@pytest.fixture
def project(db: Session) -> DbProject:
    project = DbProject(name='Test project', alias='TEST')
    db.add(project)
    db.flush()
    return project


@pytest.fixture
def candidate(db: Session, site: DbSite, project: DbProject) -> DbCandidate:
    candidate = DbCandidate(
        cand_id=111111,
        psc_id='DCC001',
        registration_site=site,
        registration_project=project,
    )
    db.add(candidate)
    db.flush()
    return candidate


@pytest.fixture
def visit(db: Session, candidate: DbCandidate, site: DbSite, project: DbProject) -> DbSession:
    visit = DbSession(candidate=candidate, site=site, project=project, visit_label='V1')
    db.add(visit)
    db.flush()
    return visit
