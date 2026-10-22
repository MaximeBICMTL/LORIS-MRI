#!/usr/bin/env python3

"""Compare SQLite, fresh MariaDB databases, and a shared MariaDB schema with rollback."""

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from importlib.metadata import version
from pathlib import Path

CONFIGURATIONS = {
    'sqlite-fresh': ('sqlite', 'fresh'),
    'mariadb-fresh': ('mariadb', 'fresh'),
    'mariadb-rollback': ('mariadb', 'rollback'),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeats', type=int, default=3, help='Measured runs per backend (default: 3).')
    parser.add_argument('--output', type=Path, help='Also save the measurements as JSON.')
    parser.add_argument('pytest_args', nargs=argparse.REMAINDER, help='Arguments after -- are passed to pytest.')
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error('--repeats must be positive')

    project_dir = Path(__file__).resolve().parents[1]
    pytest_args = args.pytest_args
    if pytest_args[:1] == ['--']:
        pytest_args = pytest_args[1:]

    results: dict[str, object] = {
        'python': sys.version.split()[0],
        'packages': {name: version(name) for name in ['pytest', 'sqlalchemy', 'testcontainers', 'mysqlclient']},
        'pytest_args': pytest_args,
        'runs': [],
    }
    runs: list[dict[str, object]] = []
    elapsed_by_configuration: dict[str, list[float]] = {name: [] for name in CONFIGURATIONS}
    for repeat in range(1, args.repeats + 1):
        # Rotate the order so each configuration runs first once in a three-repeat benchmark.
        names = list(CONFIGURATIONS)
        offset = (repeat - 1) % len(names)
        for name in names[offset:] + names[:offset]:
            backend, isolation = CONFIGURATIONS[name]
            command = [
                sys.executable, '-m', 'pytest', '-p', 'tests.fixtures',
                '-q', '-s', f'--db-backend={backend}', f'--db-isolation={isolation}', *pytest_args,
            ]
            started = time.perf_counter()
            process = subprocess.run(command, cwd=project_dir, capture_output=True, text=True)
            elapsed = time.perf_counter() - started
            if process.returncode:
                print(process.stdout + process.stderr, file=sys.stderr)
                raise SystemExit(process.returncode)
            elapsed_by_configuration[name].append(elapsed)
            run: dict[str, object] = {
                'configuration': name,
                'backend': backend,
                'isolation': isolation,
                'repeat': repeat,
                'wall_seconds': elapsed,
            }
            summary = re.search(r'\d+ passed[^\n]*', process.stdout)
            if summary:
                run['pytest_summary'] = summary[0]
            mariadb_version = re.search(r'MariaDB test server: ([^\n]+)', process.stdout)
            if mariadb_version:
                results['mariadb_version'] = mariadb_version[1]
            runs.append(run)
            print(json.dumps(run), flush=True)

    medians = {name: statistics.median(timings) for name, timings in elapsed_by_configuration.items()}
    results['runs'] = runs
    results['median_wall_seconds'] = medians
    results['fresh_over_rollback'] = medians['mariadb-fresh'] / medians['mariadb-rollback']
    results['rollback_saves_seconds'] = medians['mariadb-fresh'] - medians['mariadb-rollback']
    report = json.dumps(results, indent=2) + '\n'
    print(report)
    if args.output:
        args.output.write_text(report)


if __name__ == '__main__':
    main()
