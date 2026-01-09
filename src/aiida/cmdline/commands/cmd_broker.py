###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`verdi broker` commands."""

from __future__ import annotations

import os
import signal
import sys
import time
import typing as t
from pathlib import Path

import click

from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.cmdline.params import options
from aiida.cmdline.utils import decorators, echo


@verdi.group('broker')
def verdi_broker():
    """Manage the named pipe message broker."""


def _create_broker(profile_name: str, config_path: Path):
    """Create a ProcessBrokerService instance.

    :param profile_name: The profile name.
    :param config_path: Path to the profile config directory.
    :return: ProcessBrokerService instance.
    """
    from aiida.brokers.process_broker_service import ProcessBrokerService

    return ProcessBrokerService(
        profile_name=profile_name,
        config_path=config_path,
    )


def _get_broker_status(profile) -> dict[str, t.Any]:
    """Get broker status for a profile.

    :param profile: The profile to check.
    :return: Dictionary with 'running' (bool) and optional 'info' (ProcessInfo).
    """
    from aiida.brokers.namedpipe import discovery

    broker_info = discovery.discover_broker(profile.name)
    if broker_info is None:
        return {'running': False}

    return {'running': True, 'info': broker_info}


def _start_broker(profile, foreground: bool = False) -> None:
    """Start the broker for a profile.

    :param profile: The profile to start broker for.
    :param foreground: Whether to run in foreground.
    :raises Exception: If broker fails to start.
    """
    from aiida.manage.configuration import get_config

    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if already running
    status = _get_broker_status(profile)
    if status['running']:
        raise RuntimeError(f'Broker is already running (PID: {status["info"]["pid"]})')

    if foreground:
        # Create broker and run in foreground
        broker = _create_broker(profile.name, config_path)
        try:
            # Run maintenance loop in foreground
            while True:
                broker.run_maintenance()
                time.sleep(1)
        except KeyboardInterrupt:
            broker.close()
    else:
        # Daemonize the process (broker created after fork)
        _daemonize_broker(profile.name, config_path)


def _stop_broker(profile) -> None:
    """Stop the broker for a profile.

    :param profile: The profile to stop broker for.
    :raises Exception: If broker fails to stop.
    """
    from aiida.brokers.namedpipe import discovery

    # Check if broker is running
    broker_info = discovery.discover_broker(profile.name)
    if broker_info is None:
        return  # Already stopped

    # Send SIGTERM to broker process
    pid = broker_info['pid']
    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for process to terminate
        for _ in range(50):  # Wait up to 5 seconds
            time.sleep(0.1)
            if not discovery._is_process_alive(pid):
                break
        else:
            # Force kill if still alive
            os.kill(pid, signal.SIGKILL)

    except ProcessLookupError:
        # Process already stopped, clean up stale discovery entry
        discovery.unregister_broker(profile.name)


@verdi_broker.command()
@click.option('--foreground', is_flag=True, help='Run in foreground.')
@decorators.with_dbenv()
def start(foreground):
    """Start the named pipe message broker.

    The broker manages task distribution and broadcast messages for
    workers using named pipes.

    Returns exit code 0 if the broker is OK, non-zero if there was an error.
    """
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo('Starting the named pipe message broker... ', nl=False)

    try:
        _start_broker(profile, foreground=foreground)
        if not foreground:
            echo.echo('OK', fg=echo.COLORS['success'], bold=True)
            echo.echo_info(f'Broker started for profile: {profile.name}')
    except RuntimeError as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(str(exc))
        sys.exit(1)
    except Exception as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_critical(f'Error starting broker: {exc}')


def _daemonize_broker(profile_name: str, config_path: Path) -> None:
    """Daemonize the broker process.

    :param profile_name: The profile name.
    :param config_path: Path to the profile config directory.
    """
    import logging

    # Fork the process
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait for grandchild to write PID file, then exit
            pid_file = config_path / 'broker' / 'broker.pid'
            # Wait for PID file to be created (up to 5 seconds)
            for _ in range(50):
                if pid_file.exists():
                    with open(pid_file, 'r') as f:
                        actual_pid = f.read().strip()
                    echo.echo_info(f'Broker started in background (PID: {actual_pid})')
                    sys.exit(0)
                time.sleep(0.1)
            echo.echo_error('Timeout waiting for broker to start')
            sys.exit(1)
    except OSError as exc:
        echo.echo_critical(f'Fork failed: {exc}')

    # Child process - become session leader
    os.setsid()

    # Second fork to prevent zombie
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as exc:
        sys.exit(1)

    # Grandchild process - this is the actual daemon
    # Save our PID now that we're the final process
    pid_file = config_path / 'broker' / 'broker.pid'
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    # Setup logging for daemon
    log_file = config_path / 'broker' / 'broker.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename=str(log_file),
    )

    # NOW create the broker in the daemon process (after all forking)
    # This ensures file descriptors, selectors, and threads are created in the correct process
    broker = _create_broker(profile_name, config_path)

    # Run maintenance loop
    try:
        while True:
            broker.run_maintenance()
            time.sleep(1)
    except Exception as exc:
        logging.exception(f'Broker error: {exc}')
        broker.close()
        sys.exit(1)


@verdi_broker.command()
@decorators.with_dbenv()
def stop():
    """Stop the named pipe message broker.

    Returns exit code 0 if the broker is stopped, non-zero if there was an error.
    """
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo('Stopping the named pipe message broker... ', nl=False)

    try:
        _stop_broker(profile)
        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info('Broker stopped.')

    except PermissionError as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(f'Permission denied: {exc}')
        sys.exit(1)

    except Exception as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_critical(f'Error stopping broker: {exc}')


@verdi_broker.command()
@decorators.with_dbenv()
def status():
    """Show the status of the named pipe message broker.

    Returns exit code 0 if the broker is running, non-zero otherwise.
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo(f'Broker status for profile: {profile.name}')

    # Check broker
    broker_info = discovery.discover_broker(profile.name)
    if broker_info is None:
        echo.echo('  Broker: ', nl=False)
        echo.echo('NOT RUNNING', fg=echo.COLORS['error'], bold=True)
        sys.exit(1)

    echo.echo('  Broker: ', nl=False)
    echo.echo('RUNNING', fg=echo.COLORS['success'], bold=True)
    echo.echo(f'    PID: {broker_info["pid"]}')
    echo.echo(f'    Task pipe: {broker_info["task_pipe"]}')
    echo.echo(f'    Broadcast pipe: {broker_info["broadcast_pipe"]}')

    # Check workers
    workers = discovery.discover_workers(profile.name, check_alive=True)
    echo.echo(f'  Workers: {len(workers)}')
    for worker in workers:
        echo.echo(f'    - {worker["process_id"]} (PID: {worker["pid"]})')

    sys.exit(0)


@verdi_broker.command()
@decorators.with_dbenv()
def restart():
    """Restart the named pipe message broker.

    Returns exit code 0 if the broker is OK, non-zero if there was an error.
    """
    from click.testing import CliRunner

    runner = CliRunner()

    # Stop broker
    result = runner.invoke(stop, catch_exceptions=False)
    if result.exit_code != 0:
        echo.echo_error('Failed to stop broker')
        sys.exit(1)

    # Wait a moment
    time.sleep(0.5)

    # Start broker
    result = runner.invoke(start, catch_exceptions=False)
    sys.exit(result.exit_code)


@verdi_broker.command('workers')
@decorators.with_dbenv()
def workers():
    """Show status of all workers managed by broker.

    Returns exit code 0 if workers are found, non-zero otherwise.
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo(f'Workers for profile: {profile.name}')
    echo.echo('')

    # Check if broker is running
    broker_info = discovery.discover_broker(profile.name)
    if not broker_info:
        echo.echo_error('Broker is not running')
        echo.echo_info('Start the broker with: verdi broker start')
        sys.exit(1)

    # Get workers
    workers = discovery.discover_workers(profile.name, check_alive=True)

    if not workers:
        echo.echo_info('No workers are currently running.')
        echo.echo('')
        echo.echo_info('Workers are automatically managed by the broker.')
        sys.exit(0)

    echo.echo(f'Total workers: {len(workers)}')
    echo.echo('')

    # Display worker table
    echo.echo(f'{"Worker ID":<20} {"PID":>8} {"Status":<12}')
    echo.echo('-' * 45)

    for worker in workers:
        worker_id = worker.get('process_id', worker.get('worker_id', 'unknown'))
        pid = worker.get('pid', 0)
        # Workers discovered are alive by definition (check_alive=True)
        status = 'running'

        echo.echo(f'{worker_id:<20} {pid:>8} {status:<12}')

    sys.exit(0)


@verdi_broker.command('scale')
@click.argument('count', type=int)
@decorators.with_dbenv()
def scale(count):
    """Scale worker count to target number.

    COUNT: Target number of workers (must be non-negative).

    Note: This command requires the broker to be running with executor support.
    Worker scaling is managed automatically by the broker.

    \\b
    Examples:
        verdi broker scale 5
        verdi broker scale 0  # Stop all workers
    """
    if count < 0:
        echo.echo_critical('Worker count must be non-negative')

    echo.echo_warning('Note: Direct worker scaling is not yet implemented.')
    echo.echo_info('Workers are currently managed automatically by the broker based on configuration.')
    echo.echo('')
    echo.echo_info('To configure worker management, edit:')
    echo.echo_info('  {profile_config}/scheduler/computer_limits.json')
    echo.echo('')
    echo.echo_info('Add these fields:')
    echo.echo_info('  "_auto_spawn_workers": true')
    echo.echo_info('  "_initial_worker_count": 2')
    echo.echo_info('  "_min_worker_count": 1')
    echo.echo_info('  "_max_worker_count": 10')
    sys.exit(1)
