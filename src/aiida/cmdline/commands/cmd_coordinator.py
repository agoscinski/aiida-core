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


def _create_coordinator(profile_name: str, config_path: Path):
    """Create a PipeCoordinator instance.

    :param profile_name: The profile name.
    :param config_path: Path to the profile config directory.
    :return: PipeCoordinator instance.
    """
    from aiida.brokers.namedpipe import PipeCoordinator
    from aiida.orm.utils import serialize
    import functools

    return PipeCoordinator(
        profile_name=profile_name,
        config_path=config_path,
        encoder=functools.partial(serialize.serialize, encoding='utf-8'),
        decoder=serialize.deserialize_unsafe,
    )


def _get_coordinator_status(profile) -> dict[str, t.Any]:
    """Get coordinator status for a profile.

    :param profile: The profile to check.
    :return: Dictionary with 'running' (bool) and optional 'info' (ProcessInfo).
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_config

    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    coordinator_info = discovery.discover_coordinator(config_path)
    if coordinator_info is None:
        return {'running': False}

    return {'running': True, 'info': coordinator_info}


def _start_coordinator(profile, foreground: bool = False) -> None:
    """Start the coordinator for a profile.

    :param profile: The profile to start coordinator for.
    :param foreground: Whether to run in foreground.
    :raises Exception: If coordinator fails to start.
    """
    from aiida.manage.configuration import get_config

    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if already running
    status = _get_coordinator_status(profile)
    if status['running']:
        raise RuntimeError(f'Coordinator is already running (PID: {status["info"]["pid"]})')

    if foreground:
        # Create coordinator and run in foreground
        coordinator = _create_coordinator(profile.name, config_path)
        try:
            # Run maintenance loop in foreground
            while True:
                coordinator.run_maintenance()
                time.sleep(1)
        except KeyboardInterrupt:
            coordinator.close()
    else:
        # Daemonize the process (coordinator created after fork)
        _daemonize_coordinator(profile.name, config_path)


def _stop_coordinator(profile) -> None:
    """Stop the coordinator for a profile.

    :param profile: The profile to stop coordinator for.
    :raises Exception: If coordinator fails to stop.
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage.configuration import get_config

    config = get_config()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if coordinator is running
    coordinator_info = discovery.discover_coordinator(config_path)
    if coordinator_info is None:
        return  # Already stopped

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
            os.kill(pid, signal.SIGKILL)

    except ProcessLookupError:
        # Process already stopped, clean up stale discovery entry
        discovery.unregister_coordinator(config_path)


@verdi_coordinator.command()
@click.option('--foreground', is_flag=True, help='Run in foreground.')
@decorators.with_dbenv()
def start(foreground):
    """Start the named pipe coordinator.

    The coordinator manages task distribution and broadcast messages for
    workers using named pipes.

    Returns exit code 0 if the coordinator is OK, non-zero if there was an error.
    """
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo('Starting the named pipe coordinator... ', nl=False)

    try:
        _start_coordinator(profile, foreground=foreground)
        if not foreground:
            echo.echo('OK', fg=echo.COLORS['success'], bold=True)
            echo.echo_info(f'Coordinator started for profile: {profile.name}')
    except RuntimeError as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(str(exc))
        sys.exit(1)
    except Exception as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_critical(f'Error starting coordinator: {exc}')


def _daemonize_coordinator(profile_name: str, config_path: Path) -> None:
    """Daemonize the coordinator process.

    :param profile_name: The profile name.
    :param config_path: Path to the profile config directory.
    """
    import logging

    # Fork the process
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait for grandchild to write PID file, then exit
            pid_file = config_path / 'coordinator' / 'coordinator.pid'
            # Wait for PID file to be created (up to 5 seconds)
            for _ in range(50):
                if pid_file.exists():
                    with open(pid_file, 'r') as f:
                        actual_pid = f.read().strip()
                    echo.echo_info(f'Coordinator started in background (PID: {actual_pid})')
                    sys.exit(0)
                time.sleep(0.1)
            echo.echo_error('Timeout waiting for coordinator to start')
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
    pid_file = config_path / 'coordinator' / 'coordinator.pid'
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))

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

    # NOW create the coordinator in the daemon process (after all forking)
    # This ensures file descriptors, selectors, and threads are created in the correct process
    coordinator = _create_coordinator(profile_name, config_path)

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
    from aiida.manage.configuration import get_profile

    profile = get_profile()

    echo.echo('Stopping the named pipe coordinator... ', nl=False)

    try:
        _stop_coordinator(profile)
        echo.echo('OK', fg=echo.COLORS['success'], bold=True)
        echo.echo_info('Coordinator stopped.')

    except PermissionError as exc:
        echo.echo('FAILED', fg=echo.COLORS['error'], bold=True)
        echo.echo_error(f'Permission denied: {exc}')
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
