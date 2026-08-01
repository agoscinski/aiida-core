###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Benchmark 100 ArithmeticAddCalculation jobs for two daemon configurations."""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

BENCHMARK_SCRIPT: Final = Path('docs/source/howto/include/scripts/performance_benchmark_base.py')
EMAIL: Final = 'benchmark@example.com'
CASES: Final = (
    ('4-workers-25-slots', 4, 25),
    ('1-worker-100-slots', 1, 100),
)


def run_command(command: list[str], env: dict[str, str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a subprocess command and stream its output."""
    print(f"\n$ {' '.join(shlex.quote(part) for part in command)}")
    result = subprocess.run(command, env=env, text=True, capture_output=True, start_new_session=True, check=False)
    if result.stdout:
        print(result.stdout, end='')
    if result.stderr:
        print(result.stderr, end='', file=sys.stderr)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def cleanup_local_turso_storage(args: argparse.Namespace) -> None:
    """Remove local Turso test storage artifacts if they already exist."""
    if args.storage_backend != 'core.turso_dos' or args.database_url is None:
        return

    parsed = urlparse(args.database_url)

    if parsed.scheme not in ('sqlite', 'sqlite+turso', 'sqlite+turso_sync'):
        return

    filepath_database = Path(parsed.path)
    if str(filepath_database) in {'', '/'}:
        return

    if filepath_database.is_file():
        filepath_database.unlink()

    if args.repository_uri is None:
        return

    filepath_repository = Path(urlparse(args.repository_uri).path)
    if filepath_repository.exists():
        import shutil

        shutil.rmtree(filepath_repository)


def ensure_profile(args: argparse.Namespace, env: dict[str, str]) -> None:
    """Create the benchmark profile if it does not yet exist."""
    result = run_command(['verdi', 'profile', 'list'], env=env, check=False)
    if args.profile_name in result.stdout:
        return

    cleanup_local_turso_storage(args)

    command = [
        'verdi',
        'profile',
        'setup',
        args.storage_backend,
        '-n',
        '--profile-name',
        args.profile_name,
        '--email',
        EMAIL,
        '--broker',
        'core.zeromq',
    ]

    if args.storage_backend == 'core.sqlite_dos':
        command.extend(['--filepath', args.sqlite_filepath])
    elif args.storage_backend == 'core.turso_dos':
        command.extend(['--database-url', args.database_url, '--repository-uri', args.repository_uri])
        if args.auth_token:
            command.extend(['--auth-token', args.auth_token])
    else:
        msg = f'Unsupported storage backend: {args.storage_backend}'
        raise ValueError(msg)

    run_command(command, env=env)


def stop_daemon(env: dict[str, str]) -> None:
    """Stop the daemon, ignoring failures."""
    run_command(['verdi', 'daemon', 'stop'], env=env, check=False)


def run_case(
    args: argparse.Namespace, env: dict[str, str], label: str, workers: int, slots: int
) -> tuple[float, float]:
    """Run a single benchmark case and return elapsed time and time per process."""
    print(f'\n=== {label}: workers={workers}, worker_process_slots={slots} ===')
    stop_daemon(env)
    run_command(['verdi', 'config', 'set', 'daemon.worker_process_slots', str(slots)], env=env)
    run_command(['verdi', 'daemon', 'start', str(workers)], env=env)

    try:
        result = run_command(
            [sys.executable, str(BENCHMARK_SCRIPT), '-n', str(args.number), '--daemon'],
            env=env,
        )
    finally:
        stop_daemon(env)

    elapsed_match = re.search(r'Elapsed time: ([0-9.]+) seconds\.', result.stdout)
    performance_match = re.search(r'Performance: ([0-9.]+) s / process', result.stdout)

    if elapsed_match is None or performance_match is None:
        msg = 'Could not parse benchmark output.'
        raise RuntimeError(msg)

    return float(elapsed_match.group(1)), float(performance_match.group(1))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--number', type=int, default=100, help='Number of ArithmeticAddCalculation jobs to submit.')
    parser.add_argument('--profile-name', default='benchmark-daemon-configs', help='AiiDA profile name to use.')
    parser.add_argument(
        '--storage-backend',
        default='core.sqlite_dos',
        choices=('core.sqlite_dos', 'core.turso_dos'),
        help='AiiDA storage backend to benchmark.',
    )
    parser.add_argument(
        '--aiida-path',
        default=None,
        help='AiiDA config directory to use. Defaults to a temporary directory.',
    )
    parser.add_argument(
        '--sqlite-filepath',
        default=None,
        help='Storage directory for core.sqlite_dos. Defaults to a temporary directory.',
    )
    parser.add_argument('--database-url', default=None, help='Database URL for core.turso_dos.')
    parser.add_argument('--auth-token', default=None, help='Auth token for core.turso_dos.')
    parser.add_argument('--repository-uri', default=None, help='Repository URI for core.turso_dos.')
    parser.add_argument(
        '--workers', type=int, default=None, help='Run a single case with this number of daemon workers.'
    )
    parser.add_argument(
        '--slots',
        type=int,
        default=None,
        help='Run a single case with this daemon.worker_process_slots value.',
    )
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()

    aiida_path = args.aiida_path or tempfile.mkdtemp(prefix='aiida-benchmark-')
    env = os.environ.copy()
    env['AIIDA_PATH'] = aiida_path

    if args.storage_backend == 'core.sqlite_dos':
        if args.sqlite_filepath is None:
            args.sqlite_filepath = tempfile.mkdtemp(prefix='aiida-sqlite-dos-')
    else:
        if args.database_url is None:
            handle, filepath_database = tempfile.mkstemp(prefix='aiida-turso-', suffix='.db')
            os.close(handle)
            # NOTE: `sqlite+turso:///` would engage pyturso, but pyturso 0.7.2 takes an exclusive process-level lock
            # on local database files. This prevents the submitter, broker and daemon workers from opening the same
            # storage concurrently. Default to the pysqlite driver; pass an explicit pyturso URL to test direct runs.
            args.database_url = Path(filepath_database).as_uri().replace('file://', 'sqlite:///')
        if args.repository_uri is None:
            repository_path = tempfile.mkdtemp(prefix='aiida-turso-repo-')
            args.repository_uri = Path(repository_path).as_uri()

    ensure_profile(args, env)

    if (args.workers is None) != (args.slots is None):
        msg = '`--workers` and `--slots` must be specified together.'
        raise ValueError(msg)

    cases = (
        CASES
        if args.workers is None
        else ((f'{args.workers}-workers-{args.slots}-slots', args.workers, args.slots),)
    )

    summary: list[tuple[str, float, float]] = []
    for label, workers, slots in cases:
        elapsed, performance = run_case(args, env, label, workers, slots)
        summary.append((label, elapsed, performance))

    print('\n=== Summary ===')
    for label, elapsed, performance in summary:
        print(f'{label}: elapsed={elapsed:.2f}s performance={performance:.2f}s/process')

    print(f'AIIDA_PATH={aiida_path}')
    print(f'PROFILE={args.profile_name}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
