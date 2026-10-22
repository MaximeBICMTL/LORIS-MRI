# LORIS Python tooling

## Virtual environment

LORIS-MRI uses a Python virtual environment to manage its execution context and dependencies. To activate the virtual environment, use the command `source environment` in the LORIS-MRI root directory. The dependencies of the virtual environment are listed in the `pyproject.toml` file.

## Configuration

The LORIS Python tools are configured in the `pyproject.toml` file in the LORIS-MRI root directory.

## Linting

LORIS Python uses the Ruff linter to ensure a consistent coding style that adheres to the Python community guidelines.

To run Ruff, use the following commands in the LORIS-MRI root directory:
* `ruff check` to run the linter and report style errors.
* `ruff check --fix` to automatically fix trivial linting errors such as unsorted imports.

## Type checking

LORIS Python uses type hints and the Pyright type checker [^3] to improve the robustness and maintainability of the code.

To run Pyright, use the following commands in the LORIS-MRI root directory:
* `pyright` to run the strict type checker, which type checks modern LORIS Python type-hinted modules with a strict configuration.
* `pyright --project test` to run the global type checker, which type checks all the LORIS Python modules including untyped legacy code with a (very) loose configuration.

When interacting with legacy code or untyped libraries in modern modules, you can use `# type: ignore` or `cast(type, value)` to ignore type checking errors.

[^3]: Although we use Pyright, Mypy is also an excellent alternative type checker.

## Testing

LORIS Python uses the Pytest library to handle unit and integration tests.

### Unit testing

The core LORIS Python unit tests are located in the `python/tests/unit` directory, but each Python subpackage can have its unit tests in its own `tests` directory.

To run the LORIS Python unit tests, use the command `pytest` in the root LORIS-MRI directory or in a package directory to only run that package tests.

Database tests require the `test` dependencies (`pip install --editable '.[test]'`) and a running,
accessible Docker daemon. Testcontainers starts a disposable `mariadb:latest` container when the
first database test runs. The first run may download MariaDB and Testcontainers' cleanup image.
Use `docker pull mariadb:latest` to refresh a cached MariaDB image.

One server is shared for the pytest session, with a separate server per worker if tests are run in
parallel. By default, the ORM schema is created once per session/worker. Each test's `db` session
uses a savepoint inside an outer transaction, which is rolled back after the test, including after
failures. Calling `db.commit()` releases the savepoint; calling `db.rollback()` rolls back to it.
Neither commits the outer transaction. Auto-increment counters are not reset, so fixtures should
use the IDs of inserted records rather than assuming generated IDs start at one.

Tests that need real commits visible to other connections, open their own sessions/connections,
run subprocesses, or change the database schema must use `@pytest.mark.fresh_db`. The marker gives
the usual `db_engine`, `db`, and `env` fixtures a fresh ORM-created database that is dropped after
the test. A `fresh_db_engine` fixture is also available for tests explicitly managing their own
sessions; the marker is preferable when combining it with `db` or `env`. For example:

```py
@pytest.mark.fresh_db
def test_committed_data_is_visible(db, db_engine):
    # db.commit() is a real database commit in this test.
    ...
```

Use `pytest --db-isolation=fresh` to run all database tests with a fresh database per test, or
`pytest --db-isolation=rollback` for the default strategy. Fresh databases are dropped even after
failed tests or failed schema setup. No production database, SQL schema checkout, or integration
dataset is required.

Fixtures should create the parent records required by foreign keys. Shared `site`, `project`,
`candidate`, and `visit` fixtures are available for tests that need those records.

For performance comparisons or SQLite-specific diagnostics, use `pytest --db-backend=sqlite`.
This uses an in-memory SQLite database with foreign keys enabled and supports both isolation
strategies. MariaDB remains the default backend, including in CI; SQLite is never selected
automatically when Docker fails.
Tests that do not request database fixtures do not start any containers.

To compare both backends with the same tests and record complete process wall times, run:

```sh
python test/benchmark_unit_tests.py --repeats 3 --output /tmp/unit-test-benchmark.json
```

The benchmark compares SQLite with fresh databases, MariaDB with fresh databases, and MariaDB
with a shared schema and transaction rollback. The order rotates between repeats. Each MariaDB
run includes a fresh container startup and container cleanup. Fresh mode creates and drops the
schema per database test; rollback mode creates and drops it once for the run.
Pre-pull the images for comparisons that exclude image download time. Additional pytest arguments
can be supplied after `--`, for example `-- python/tests/unit/db/query`.

Measurements from the SQLite-to-MariaDB migration are documented in the
[database benchmark report](Unit_test_database_benchmark.md).

### Integration testing

The LORIS-MRI integration tests are located in the `python/tests/integration` directory.

The LORIS-MRI integration tests require a more complex testing environment with the following:
- A (partial) copy of the main LORIS repository.
- A copy of the S3 LORIS-MRI test dataset.
- A LORIS database Docker image with the LORIS-MRI test dataset.
- A LORIS-MRI code Docker image with all the required dependencies installed.

To run the LORIS-MRI integration tests, use the command `test/run_integration_tests.sh` in the root LORIS-MRI directory.

Note that the first run of the LORIS-MRI integration test run might take some time, as the test data must be downloaded and the Docker images must be created. However, much of this setup should be cached in the Docker cache as well as the LORIS-MRI test cache directory (by default `~/.cache/loris/test`), so further test runs should be much faster. To avoid downloading data and building images locally, it is also possible to run the LORIS-MRI integration tests remotely by simply creating a pull request, which will trigger the LORIS-MRI CI workflow.
