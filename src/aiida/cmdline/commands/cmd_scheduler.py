###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Commands to manage the per-computer process scheduler daemon."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.cmdline.utils import decorators, echo


def _daemonize_scheduler(profile_name: str, config_path: Path) -> None:
    """Daemonize the scheduler process.

    :param profile_name: The profile name.
    :param config_path: Path to the profile config directory.
    """
    import logging
    import os

    # Fork the process
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait briefly then exit
            import time
            time.sleep(0.5)
            sys.exit(0)
    except OSError as exc:
        echo.echo_critical(f'Fork failed: {exc}')

    # Child process - become session leader
    os.setsid()

    # Second fork to prevent zombie
    try:
        pid = os.fork()
        if pid > 0:
            # First child exits
            sys.exit(0)
    except OSError as exc:
        echo.echo_critical(f'Second fork failed: {exc}')

    # Grandchild continues as daemon
    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    # Close stdin/stdout/stderr
    with open(os.devnull, 'r') as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    with open(os.devnull, 'a+') as devnull:
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())

    # Configure logging to file
    log_file = config_path / 'scheduler' / 'scheduler.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create and run scheduler (ProcessBrokerService with scheduling enabled)
    from aiida.brokers.process_broker_service import ProcessBrokerService

    scheduler = ProcessBrokerService(
        profile_name=profile_name,
        config_path=config_path,
        enable_scheduling=True,
    )

    # Run maintenance loop
    import time
    try:
        while True:
            time.sleep(1)  # More frequent for scheduling
            scheduler.run_maintenance()
    except Exception:
        logging.exception('Scheduler daemon crashed')
        scheduler.close()
        sys.exit(1)


@verdi.group('scheduler')
def verdi_scheduler():
    """Manage the per-computer process scheduler daemon.

    The scheduler enforces concurrency limits per computer and queues
    processes when limits are reached. It wraps the broker functionality.
    """


@verdi_scheduler.command('start')
@click.option('--foreground', is_flag=True, help='Run in foreground instead of daemonizing')
@decorators.with_dbenv()
def scheduler_start(foreground):
    """Start the scheduler daemon.

    The scheduler intercepts process tasks and enforces per-computer concurrency
    limits before forwarding them to the internal broker.

    \\b
    Examples:
        verdi scheduler start
        verdi scheduler start --foreground
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if already running (scheduler now registers as a broker)
    existing = discovery.discover_broker(profile.name)
    if existing:
        echo.echo_critical(f'Scheduler is already running (PID: {existing["pid"]})')

    if foreground:
        # Run in foreground
        from aiida.brokers.process_broker_service import ProcessBrokerService

        echo.echo_info('Starting scheduler in foreground mode...')
        echo.echo_info('Press Ctrl+C to stop')

        scheduler = None
        try:
            scheduler = ProcessBrokerService(
                profile_name=profile.name,
                config_path=config_path,
                enable_scheduling=True,
            )

            # Run maintenance loop
            import time
            while True:
                time.sleep(1)  # More frequent for scheduling
                scheduler.run_maintenance()

        except KeyboardInterrupt:
            echo.echo_info('\nShutting down scheduler...')
            if scheduler:
                scheduler.close()
            echo.echo_success('Scheduler stopped')
        except Exception as exc:
            echo.echo_critical(f'Scheduler failed: {exc}')
    else:
        # Daemonize
        try:
            _daemonize_scheduler(profile_name=profile.name, config_path=config_path)
            echo.echo_success('Scheduler daemon started')
            echo.echo_info('Use "verdi scheduler status" to check status')
        except Exception as exc:
            echo.echo_critical(f'Failed to start scheduler daemon: {exc}')


@verdi_scheduler.command('stop')
@decorators.with_dbenv()
def scheduler_stop():
    """Stop the scheduler daemon.

    \\b
    Examples:
        verdi scheduler stop
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if running (scheduler registers as broker)
    existing = discovery.discover_broker(profile.name)
    if not existing:
        echo.echo_error('Scheduler is not running')
        return

    # Send termination signal
    import os
    import signal
    import time

    pid = existing['pid']
    echo.echo_info(f'Stopping scheduler (PID: {pid})...')

    try:
        os.kill(pid, signal.SIGTERM)

        # Wait for process to stop
        for _ in range(30):  # Wait up to 3 seconds
            time.sleep(0.1)
            if not discovery.discover_broker(profile.name):
                echo.echo_success('Scheduler stopped')
                return

        # Still running, force kill
        echo.echo_warning('Scheduler did not stop gracefully, forcing shutdown...')
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)

        if discovery.discover_broker(profile.name):
            echo.echo_error('Failed to stop scheduler')
        else:
            echo.echo_success('Scheduler stopped (forced)')

    except ProcessLookupError:
        # Process already dead, clean up discovery file
        discovery.unregister_broker(profile.name, 'broker')
        echo.echo_success('Scheduler stopped (was not running)')
    except Exception as exc:
        echo.echo_critical(f'Failed to stop scheduler: {exc}')


@verdi_scheduler.command('restart')
@decorators.with_dbenv()
@click.pass_context
def scheduler_restart(ctx):
    """Restart the scheduler daemon.

    \\b
    Examples:
        verdi scheduler restart
    """
    echo.echo_info('Restarting scheduler...')

    # Stop if running
    from aiida.brokers.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    if discovery.discover_broker(profile.name):
        ctx.invoke(scheduler_stop)

    # Wait a moment
    import time
    time.sleep(0.5)

    # Start
    ctx.invoke(scheduler_start)


@verdi_scheduler.command('status')
@decorators.with_dbenv()
def scheduler_status():
    """Show scheduler daemon and queue status.

    Displays:
    - Whether the scheduler daemon is running
    - Per-computer limits and queue status
    - Running and queued process counts

    \\b
    Examples:
        verdi scheduler status
    """
    from aiida.brokers.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if scheduler is running (registered as broker)
    existing = discovery.discover_broker(profile.name)

    echo.echo_info('Scheduler daemon status:')
    echo.echo('')

    if existing:
        echo.echo(f'  Status: Running (PID: {existing["pid"]})')
    else:
        echo.echo('  Status: Not running')
        echo.echo('')
        echo.echo_info('Start the scheduler with: verdi scheduler start')
        return

    echo.echo('')
    echo.echo_info('Per-computer queue status:')
    echo.echo('')

    # Read queue status from broker/scheduler directory
    broker_dir = config_path / 'broker' / 'scheduler'

    # Load limits
    limits_file = config_path / 'scheduler' / 'computer_limits.json'
    limits = {}
    if limits_file.exists():
        try:
            with open(limits_file) as f:
                data = json.load(f)
                # Extract limits, skip metadata keys
                limits = {k: v for k, v in data.items() if not k.startswith('_')}
        except (json.JSONDecodeError, OSError):
            pass

    # Check for queue directories
    if not broker_dir.exists():
        echo.echo_info('No queues found.')
        return

    computer_dirs = [d for d in broker_dir.iterdir() if d.is_dir() and d.name != '__pycache__']

    if not computer_dirs and not limits:
        echo.echo_info('No queues or limits configured.')
        echo.echo('')
        echo.echo_info('Configure limits with: verdi scheduler set-limit <computer_label> <limit>')
        return

    # Display all computers (from limits or queues)
    all_computers = set(limits.keys()) | {d.name for d in computer_dirs}

    echo.echo(f'{"Computer":<30} {"Limit":>8} {"Queued":>8}')
    echo.echo('-' * 50)

    for computer_label in sorted(all_computers):
        limit = limits.get(computer_label, 10)

        # Count queued processes
        computer_dir = broker_dir / computer_label
        if computer_dir.exists():
            queue_files = list(computer_dir.glob('proc_*.json'))
            queued_count = len(queue_files)
        else:
            queued_count = 0

        echo.echo(f'{computer_label:<30} {limit:>8} {queued_count:>8}')

    echo.echo('')
    echo.echo_info('Note: "Queued" shows processes waiting to run.')
    echo.echo_info('      Running process counts are maintained by the scheduler daemon.')


@verdi_scheduler.command('set-limit')
@click.argument('computer_label', type=str)
@click.argument('limit', type=int)
@decorators.with_dbenv()
def scheduler_set_limit(computer_label, limit):
    """Set concurrency limit for a computer.

    COMPUTER_LABEL: The label of the computer to configure.
    LIMIT: Maximum number of concurrent processes (must be positive).

    The scheduler must be restarted for the new limit to take effect.

    \\b
    Examples:
        verdi scheduler set-limit localhost 20
        verdi scheduler set-limit frontier 50
    """
    if limit < 1:
        echo.echo_critical('Limit must be a positive integer')

    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Update computer_limits.json
    scheduler_dir = config_path / 'scheduler'
    scheduler_dir.mkdir(parents=True, exist_ok=True)
    limits_file = scheduler_dir / 'computer_limits.json'

    # Load existing limits
    if limits_file.exists():
        try:
            with open(limits_file) as f:
                limits = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            echo.echo_warning(f'Could not read limits file: {exc}. Creating new file.')
            limits = {}
    else:
        limits = {}

    # Update limit
    limits[computer_label] = limit

    # Save limits
    try:
        with open(limits_file, 'w') as f:
            json.dump(limits, f, indent=2)
    except OSError as exc:
        echo.echo_critical(f'Failed to write limits file: {exc}')

    echo.echo_success(f'Set concurrency limit for "{computer_label}" to {limit}')
    echo.echo_info('Restart the scheduler for changes to take effect: verdi scheduler restart')


@verdi_scheduler.command('show-limits')
@decorators.with_dbenv()
def scheduler_show_limits():
    """Show concurrency limits for all computers.

    Displays the configured concurrency limits from computer_limits.json.
    If no limits are configured, shows the default limit.

    \\b
    Examples:
        verdi scheduler show-limits
    """
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    scheduler_dir = config_path / 'scheduler'
    limits_file = scheduler_dir / 'computer_limits.json'

    if not limits_file.exists():
        echo.echo_info('No limits configured.')
        echo.echo_info('Default limit: 10 processes per computer')
        echo.echo('')
        echo.echo_info('Configure limits with: verdi scheduler set-limit <computer_label> <limit>')
        return

    try:
        with open(limits_file) as f:
            limits = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        echo.echo_critical(f'Failed to read limits file: {exc}')

    if not limits:
        echo.echo_info('No limits configured.')
        return

    echo.echo_info('Computer concurrency limits:')
    echo.echo('')
    for computer_label in sorted(limits.keys()):
        limit = limits[computer_label]
        echo.echo(f'  {computer_label:<30} {limit:>5}')
