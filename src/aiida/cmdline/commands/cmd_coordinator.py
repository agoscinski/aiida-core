###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`verdi coordinator` commands."""

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


@verdi.group('coordinator')
def verdi_coordinator():
    """Manage the named pipe coordinator."""


@verdi_coordinator.command()
@click.option('--foreground', is_flag=True, help='Run in foreground.')
@decorators.with_dbenv()
def start(foreground):
    """Start the named pipe coordinator.

    The coordinator manages task distribution and broadcast messages for
    workers using named pipes.

    Returns exit code 0 if the coordinator is OK, non-zero if there was an error.
    """
    from aiida.brokers.namedpipe import PipeCoordinator, discovery
    from aiida.manage.configuration import get_config, get_profile

    profile = get_profile()
    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    echo.echo('Starting the named pipe coordinator... ', nl=False)

    # Check if coordinator is already running
    existing_coordinator = discovery.discover_coordinator(config_path)
    if existing_coordinator:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(f'Coordinator is already running (PID: {existing_coordinator["pid"]})')
        sys.exit(1)

    try:
        from aiida.orm.utils import serialize
        import functools

        coordinator = PipeCoordinator(
            profile_name=profile.name,
            config_path=config_path,
            encoder=functools.partial(serialize.serialize, encoding='utf-8'),
            decoder=serialize.deserialize_unsafe,
        )

        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info(f'Coordinator started for profile: {profile.name}')

        if foreground:
            echo.echo_info('Running in foreground mode. Press Ctrl+C to stop.')
            try:
                # Run maintenance loop in foreground
                while True:
                    coordinator.run_maintenance()
                    time.sleep(1)
            except KeyboardInterrupt:
                echo.echo_info('\nShutting down coordinator...')
                coordinator.close()
                echo.echo('Coordinator stopped.', fg=echo.COLORS['success'])
        else:
            # Daemonize the process
            _daemonize_coordinator(coordinator, config_path)

    except Exception as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_critical(f'Error starting coordinator: {exc}')


def _daemonize_coordinator(coordinator: 'PipeCoordinator', config_path: Path) -> None:
    """Daemonize the coordinator process.

    :param coordinator: The coordinator instance.
    :param config_path: Path to the profile config directory.
    """
    import logging

    # Fork the process
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - save PID and exit
            pid_file = config_path / 'coordinator' / 'coordinator.pid'
            pid_file.parent.mkdir(parents=True, exist_ok=True)
            with open(pid_file, 'w') as f:
                f.write(str(pid))
            echo.echo_info(f'Coordinator started in background (PID: {pid})')
            sys.exit(0)
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

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    # Setup logging for daemon
    log_file = config_path / 'coordinator' / 'coordinator.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename=str(log_file),
    )

    # Run maintenance loop
    try:
        while True:
            coordinator.run_maintenance()
            time.sleep(1)
    except Exception as exc:
        logging.exception(f'Coordinator error: {exc}')
        coordinator.close()
        sys.exit(1)


@verdi_coordinator.command()
@decorators.with_dbenv()
def stop():
    """Stop the named pipe coordinator.

    Returns exit code 0 if the coordinator is stopped, non-zero if there was an error.
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_config, get_profile

    profile = get_profile()
    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    echo.echo('Stopping the named pipe coordinator... ', nl=False)

    # Check if coordinator is running
    coordinator_info = discovery.discover_coordinator(config_path)
    if coordinator_info is None:
        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info('Coordinator is not running.')
        return

    # Send SIGTERM to coordinator process
    pid = coordinator_info['pid']
    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for process to terminate
        for _ in range(50):  # Wait up to 5 seconds
            time.sleep(0.1)
            if not discovery._is_process_alive(pid):
                break
        else:
            # Force kill if still alive
            echo.echo_warning('Coordinator did not stop gracefully, forcing...')
            os.kill(pid, signal.SIGKILL)

        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info('Coordinator stopped.')

    except ProcessLookupError:
        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info('Coordinator process not found (already stopped).')
        # Clean up stale discovery entry
        discovery.unregister_coordinator(config_path)

    except PermissionError:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(f'Permission denied to stop coordinator (PID: {pid})')
        sys.exit(1)

    except Exception as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_critical(f'Error stopping coordinator: {exc}')


@verdi_coordinator.command()
@decorators.with_dbenv()
def status():
    """Show the status of the named pipe coordinator.

    Returns exit code 0 if the coordinator is running, non-zero otherwise.
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_config, get_profile

    profile = get_profile()
    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    echo.echo(f'Coordinator status for profile: {profile.name}')

    # Check coordinator
    coordinator_info = discovery.discover_coordinator(config_path)
    if coordinator_info is None:
        echo.echo('  Coordinator: ', nl=False)
        echo.echo('NOT RUNNING', fg=echo.COLORS['error'], bold=True)
        sys.exit(1)

    echo.echo('  Coordinator: ', nl=False)
    echo.echo('RUNNING', fg=echo.COLORS['success'], bold=True)
    echo.echo(f'    PID: {coordinator_info["pid"]}')
    echo.echo(f'    Task pipe: {coordinator_info["task_pipe"]}')
    echo.echo(f'    Broadcast pipe: {coordinator_info["broadcast_pipe"]}')

    # Check workers
    workers = discovery.discover_workers(config_path, check_alive=True)
    echo.echo(f'  Workers: {len(workers)}')
    for worker in workers:
        echo.echo(f'    - {worker["process_id"]} (PID: {worker["pid"]})')

    sys.exit(0)


@verdi_coordinator.command()
@decorators.with_dbenv()
def restart():
    """Restart the named pipe coordinator.

    Returns exit code 0 if the coordinator is OK, non-zero if there was an error.
    """
    from click.testing import CliRunner

    runner = CliRunner()

    # Stop coordinator
    result = runner.invoke(stop, catch_exceptions=False)
    if result.exit_code != 0:
        echo.echo_error('Failed to stop coordinator')
        sys.exit(1)

    # Wait a moment
    time.sleep(0.5)

    # Start coordinator
    result = runner.invoke(start, catch_exceptions=False)
    sys.exit(result.exit_code)
