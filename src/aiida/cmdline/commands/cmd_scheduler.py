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
from pathlib import Path

import click

from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.cmdline.utils import decorators, echo


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
    from aiida.communication.namedpipe import discovery
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
        from aiida.engine.scheduler.process_scheduler_service import ProcessSchedulerService

        echo.echo_info('Starting scheduler in foreground mode...')
        echo.echo_info('Press Ctrl+C to stop')

        scheduler = None
        try:
            scheduler = ProcessSchedulerService(
                profile_name=profile.name,
                config_path=config_path,
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
            from aiida.engine.scheduler.process_scheduler_service import start_daemon

            start_daemon(profile_name=profile.name, config_path=config_path)
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
    from aiida.communication.namedpipe import discovery
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
    from aiida.communication.namedpipe import discovery
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
    - Worker status and configuration
    - Per-computer limits and queue status

    \\b
    Examples:
        verdi scheduler status
    """
    import datetime

    from aiida.communication.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    # Check if scheduler is running (registered as broker)
    existing = discovery.discover_broker(profile.name)

    # Load config
    data = _load_scheduler_config(config_path)
    workers_config = data.get('workers', {})

    echo.echo('')
    echo.echo('=' * 60)
    echo.echo('  SCHEDULER STATUS')
    echo.echo('=' * 60)
    echo.echo('')

    if not existing:
        echo.echo('  Status:    Not running')
        echo.echo('')
        echo.echo_info('Start the scheduler with: verdi scheduler start')
        return

    # Scheduler is running
    echo.echo(f'  Status:    Running')
    echo.echo(f'  PID:       {existing["pid"]}')
    echo.echo(f'  Log:       {config_path / "scheduler" / "scheduler.log"}')

    # Show uptime if available
    if 'timestamp' in existing:
        started = datetime.datetime.fromisoformat(existing['timestamp'])
        uptime = datetime.datetime.now() - started
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        echo.echo(f'  Uptime:    {hours}h {minutes}m {seconds}s')

    echo.echo('')

    # Worker status
    echo.echo('-' * 60)
    echo.echo('  WORKERS')
    echo.echo('-' * 60)
    echo.echo('')

    from aiida.engine.daemon.client import get_daemon_client
    daemon_client = get_daemon_client()

    workers = discovery.discover_workers(profile.name)
    configured_count = workers_config.get('count', workers_config.get('initial_count', 0))

    echo.echo(f'  Configured:    {configured_count}')
    echo.echo(f'  Running:       {len(workers)}')
    echo.echo(f'  Log:           {daemon_client.daemon_log_file}')

    if workers:
        echo.echo('')
        echo.echo(f'  {"Worker ID":<24} {"PID":>8}')
        echo.echo('  ' + '-' * 34)
        for worker in workers:
            echo.echo(f'  {worker["process_id"]:<24} {worker["pid"]:>8}')

    echo.echo('')

    # Queue status (only for computers with limits)
    limits = data.get('computers', {})

    if not limits:
        echo.echo('-' * 60)
        echo.echo('  LIMITS')
        echo.echo('-' * 60)
        echo.echo('')
        echo.echo('  No computer limits configured (all unlimited).')
        echo.echo('')
        return

    echo.echo('-' * 60)
    echo.echo('  QUEUES (computers with limits)')
    echo.echo('-' * 60)
    echo.echo('')

    queue_dir = config_path / 'scheduler' / 'queue'

    echo.echo(f'  {"Computer":<24} {"Limit":>6} {"Running":>8} {"Queued":>8}')
    echo.echo('  ' + '-' * 50)

    total_running = 0
    total_queued = 0
    for computer_label in sorted(limits.keys()):
        limit = limits[computer_label]

        # Count queued and running processes
        computer_dir = queue_dir / computer_label
        running_count = 0
        queued_count = 0
        if computer_dir.exists():
            import json
            for proc_file in computer_dir.glob('proc_*.json'):
                try:
                    with open(proc_file) as f:
                        proc_data = json.load(f)
                    status = proc_data.get('status', 'pending')
                    if status == 'running':
                        running_count += 1
                    else:
                        queued_count += 1
                except (json.JSONDecodeError, OSError):
                    queued_count += 1  # Count broken files as queued

        total_running += running_count
        total_queued += queued_count
        echo.echo(f'  {computer_label:<24} {limit:>6} {running_count:>8} {queued_count:>8}')

    echo.echo('  ' + '-' * 50)
    echo.echo(f'  {"Total":<24} {"":>6} {total_running:>8} {total_queued:>8}')
    echo.echo('')


@verdi_scheduler.group('limits')
def scheduler_limits():
    """Manage per-computer concurrency limits."""


@scheduler_limits.command('set')
@click.argument('computer_label', type=str)
@click.argument('limit', type=int)
@decorators.with_dbenv()
def limits_set(computer_label, limit):
    """Set concurrency limit for a computer.

    COMPUTER_LABEL: The label of the computer to configure.
    LIMIT: Maximum number of concurrent processes (must be positive).

    The scheduler must be restarted for the new limit to take effect.

    \\b
    Examples:
        verdi scheduler limits set localhost 20
        verdi scheduler limits set frontier 50
    """
    if limit < 1:
        echo.echo_critical('Limit must be a positive integer')

    from aiida.common.exceptions import NotExistent
    from aiida.orm import Computer

    try:
        Computer.collection.get(label=computer_label)
    except NotExistent:
        echo.echo_critical(f'Computer "{computer_label}" does not exist')

    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    data = _load_scheduler_config(config_path)

    # Ensure computers section exists
    if 'computers' not in data:
        data['computers'] = {}

    # Update limit
    data['computers'][computer_label] = limit

    try:
        _save_scheduler_config(config_path, data)
    except OSError as exc:
        echo.echo_critical(f'Failed to write config file: {exc}')

    echo.echo_success(f'Set concurrency limit for "{computer_label}" to {limit}')
    echo.echo_info('Restart the scheduler for changes to take effect: verdi scheduler restart')


@scheduler_limits.command('show')
@decorators.with_dbenv()
def limits_show():
    """Show concurrency limits for all computers.

    Displays the configured concurrency limits for all computers.
    Computers without a specific limit show 'Unlimited'.

    \\b
    Examples:
        verdi scheduler limits show
    """
    from aiida.orm import Computer, QueryBuilder
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    data = _load_scheduler_config(config_path)
    configured_limits = data.get('computers', {})

    # Query all computers
    qb = QueryBuilder()
    qb.append(Computer, project=['label'])
    all_computers = sorted([row[0] for row in qb.all()])

    echo.echo_info('Computer concurrency limits:')
    echo.echo('')
    echo.echo(f'  {"Computer":<30} {"Limit":>10}')
    echo.echo('  ' + '-' * 42)

    if not all_computers:
        echo.echo('  No computers configured.')
        return

    for computer_label in all_computers:
        if computer_label in configured_limits:
            limit = str(configured_limits[computer_label])
        else:
            limit = 'Unlimited'
        echo.echo(f'  {computer_label:<30} {limit:>10}')

    echo.echo('')
    echo.echo_info('Set limits with: verdi scheduler limits set <computer> <limit>')


def _load_scheduler_config(config_path: Path) -> dict:
    """Load scheduler config from file."""
    config_file = config_path / 'scheduler' / 'config.json'
    if config_file.exists():
        try:
            with open(config_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}

# TODO should be somehow in scheduler API
def _save_scheduler_config(config_path: Path, data: dict) -> None:
    """Save scheduler config to file."""
    scheduler_dir = config_path / 'scheduler'
    scheduler_dir.mkdir(parents=True, exist_ok=True)
    config_file = scheduler_dir / 'config.json'
    with open(config_file, 'w') as f:
        json.dump(data, f, indent=2)


@verdi_scheduler.group('workers')
def scheduler_workers():
    """Manage scheduler workers."""


@scheduler_workers.command('set')
@click.argument('count', type=int)
@decorators.with_dbenv()
def workers_set(count):
    """Set the number of workers for the scheduler.

    COUNT: Number of workers to run. Dead workers are automatically respawned.

    The scheduler must be restarted for changes to take effect.

    \\b
    Examples:
        verdi scheduler workers set 4
    """
    if count < 0:
        echo.echo_critical('Worker count must be non-negative')

    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    data = _load_scheduler_config(config_path)

    # Ensure workers section exists
    if 'workers' not in data:
        data['workers'] = {}

    # Update worker count
    data['workers']['count'] = count

    try:
        _save_scheduler_config(config_path, data)
    except OSError as exc:
        echo.echo_critical(f'Failed to write config file: {exc}')

    echo.echo_success(f'Set worker count to {count}')
    echo.echo_info('Restart the scheduler for changes to take effect: verdi scheduler restart')


@scheduler_workers.command('status')
@decorators.with_dbenv()
def workers_status():
    """Show worker configuration and status.

    \\b
    Examples:
        verdi scheduler workers status
    """
    from aiida.communication.namedpipe import discovery
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    data = _load_scheduler_config(config_path)
    workers_config = data.get('workers', {})

    configured_count = workers_config.get('count', workers_config.get('initial_count', 0))

    # Show running workers
    from aiida.engine.daemon.client import get_daemon_client
    daemon_client = get_daemon_client()

    workers = discovery.discover_workers(profile.name)

    echo.echo_info('Worker status:')
    echo.echo(f'  Configured:   {configured_count}')
    echo.echo(f'  Running:      {len(workers)}')
    echo.echo(f'  Log:          {daemon_client.daemon_log_file}')

    if workers:
        echo.echo('')
        echo.echo(f'  {"Worker ID":<20} {"PID":>8}')
        echo.echo('  ' + '-' * 30)
        for worker in workers:
            echo.echo(f'  {worker["process_id"]:<20} {worker["pid"]:>8}')


@verdi_scheduler.command('logshow')
@decorators.with_dbenv()
def scheduler_logshow():
    """Show scheduler log in a pager.
    """
    from aiida.manage import manager

    mgr = manager.get_manager()
    config = mgr.get_config()
    profile = mgr.get_profile()
    config_path = Path(config.dirpath) / 'profiles' / profile.name

    log_file = config_path / 'scheduler' / 'scheduler.log'

    if not log_file.exists():
        echo.echo_warning(f'Log file not found: {log_file}')
        echo.echo_info('The scheduler may not have been started yet.')
        return

    with open(log_file) as f:
        click.echo_via_pager(f.read())
