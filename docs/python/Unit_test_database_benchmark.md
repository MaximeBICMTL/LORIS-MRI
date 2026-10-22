# Unit-test database benchmark

## Shared schema and transaction rollback

The default MariaDB fixtures now create one ORM schema per pytest session/worker. Each ordinary
database test uses a savepoint inside an outer transaction, which is rolled back at teardown.
Tests requiring real commits, independent connections, subprocesses, or DDL use `fresh_db`.
`--db-isolation=fresh` retains the previous strategy for comparison.

Three complete runs of the original 14 application tests (11 using a database) produced these
wall times on 2026-10-22. Fixture regression tests are excluded from this comparison.

| Configuration | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| SQLite, fresh databases | 2.54 s | 4.72 s | 1.72 s | 2.54 s |
| MariaDB, fresh databases | 15.87 s | 16.03 s | 14.58 s | 15.87 s |
| MariaDB, shared schema and rollback | 7.39 s | 9.44 s | 6.74 s | 7.39 s |

Rollback saves 8.48 seconds at the median, a 2.15 times speedup over fresh MariaDB databases for
this test selection. Each run still starts and removes its own server, so the fixed startup cost
is included. The benchmark rotates configuration order between repeats; no warmup runs were
discarded. The timing variation, including in SQLite, illustrates the limits of local wall-time
measurements under normal host load.

To reproduce this comparison:

```sh
python test/benchmark_unit_tests.py --repeats 3 --output /tmp/unit-test-benchmark.json -- \
    --ignore=python/tests/unit/db/test_fixtures.py
```

To examine repeated-test overhead, collect the same application tests five times in one pytest
process (70 tests, including 55 database tests), without changing the schema or application code:

```sh
test_paths=(python/tests/unit/db/query python/loris_bids_importer/tests python/loris_bids_utils/tests)
python test/benchmark_unit_tests.py --repeats 3 --output /tmp/unit-test-benchmark-70.json -- \
    --keep-duplicates "${test_paths[@]}" "${test_paths[@]}" "${test_paths[@]}" \
    "${test_paths[@]}" "${test_paths[@]}"
```

This workload measures how the isolation strategy scales with test count. It repeats existing
tests against the same 67-table schema and does not simulate more complex application logic or
a larger schema. Fresh MariaDB mode creates 737 tables for the 14-test selection and 3,685 tables
for the 70-test selection. Rollback mode creates 67 tables once in either case.

The fivefold workload produced these results:

| Configuration | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| SQLite, fresh databases | 3.47 s | 2.34 s | 2.06 s | 2.34 s |
| MariaDB, fresh databases | 53.95 s | 48.49 s | 49.56 s | 49.56 s |
| MariaDB, shared schema and rollback | 8.40 s | 8.65 s | 8.01 s | 8.40 s |

Rollback saves 41.15 seconds at the median for 70 tests, a 5.90 times speedup. Increasing the
selection from 14 to 70 tests adds 33.68 seconds in fresh mode and 1.01 seconds in rollback mode.
The large difference is consistent with creating the schema once and paying only transaction
setup, fixture data insertion, test execution, and rollback costs for additional ordinary tests.
It supports keeping rollback as the default and reserving fresh databases for tests that need
their commit/connection/schema behavior.

All images were cached, and every measured MariaDB process still starts and removes a disposable
server. Both workload sizes use Python 3.11.11, pytest 9.0.3, SQLAlchemy 2.1.3, Testcontainers 4.15.0,
mysqlclient 2.3.0, and MariaDB 13.0.2. The [raw rollback measurements](unit_test_database_rollback_benchmark.json)
retain every run and the exact test selections. The separate correctness suite has 20 test cases
and passes on both backends with either isolation setting, including the fresh-database exceptions.

## Initial fresh-database experiment

Measured on 2026-10-22 using Python 3.11.11, pytest 9.0.3, SQLAlchemy 2.1.3,
Testcontainers 4.15.0, mysqlclient 2.3.0, and `mariadb:latest` reporting
`13.0.2-MariaDB-ubu2604`.

The initial comparison uses the same original 14 tests, including 11 database tests. Four new fixture
regression cases are excluded from these measurements. Each backend was measured three times;
the table reports complete process wall times, including Python/pytest startup and collection.

| Setup | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| Original SQLite suite | 1.89 s | 0.99 s | 0.95 s | 0.99 s |
| Updated SQLite comparison backend | 1.47 s | 1.20 s | 1.91 s | 1.47 s |
| MariaDB, fresh ORM database per test | 14.85 s | 16.48 s | 15.47 s | 15.47 s |

On this host, MariaDB adds approximately 14.5 seconds to the original suite's median wall time
(15.6 times the original runtime). It takes 10.5 times the updated SQLite backend's runtime.
The updated fixtures load all 67 ORM models explicitly, create required foreign-key parent
records, and enable SQLite foreign-key enforcement. They therefore do more work than the original
SQLite fixtures.

Each MariaDB run starts one disposable server and creates and drops a separate database for each
database test. The images were already cached: downloads are excluded, while server startup,
table creation, database deletion, and container cleanup are included. The observed fixture
durations put server startup around 4–5 seconds and subsequent database setup around 0.6–1.2
seconds per test, with database deletion around 0.2–0.3 seconds per test. Test bodies themselves
take comparatively little time. No durability settings or in-memory filesystem were used to
accelerate MariaDB.

These are local measurements with normal host scheduling variation, rather than a prediction for
CI or larger suites. No warmup runs were discarded. The original suite was exported from revision
`96aa65870eca619fe576fd245ea892943c12d1f6` and run using the same interpreter and execution permissions.
Individual measurements and package/image versions are retained in the
[raw results](unit_test_database_benchmark.json).

The current benchmark command above retains both fresh-database configurations and also measures
transaction rollback. Omit its `--ignore` argument to include the fixture regression tests.
