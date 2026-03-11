###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`verdi broker` commands for managing the message broker."""

from __future__ import annotations

from typing import Optional, Union

import click

from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.cmdline.utils import decorators, echo

DEFAULT_CALCJOB_PREFETCH = 'UNLIMITED'


def parse_limit(value: Optional[str]) -> Optional[Union[int, str]]:
    """Parse a limit value from CLI input.

    :param value: The string value from CLI (integer or 'UNLIMITED')
    :return: int, 'UNLIMITED', or None if value is None
    """
    if value is None:
        return None
    if value.upper() == 'UNLIMITED':
        return 'UNLIMITED'
    try:
        return int(value)
    except ValueError:
        raise click.BadParameter(f"'{value}' is not a valid limit. Use an integer or 'UNLIMITED'.")


@verdi.group('broker')
def verdi_broker():
    """Manage the message broker for the current profile."""


@verdi_broker.group('queue')
def verdi_broker_queue():
    """Manage process queues.

    The queue controls how many processes the daemon worker can run concurrently.
    Limits can be set separately for root WorkChains and CalcJobs to prevent deadlocks.

    Before submitting processes, you must create a queue with `verdi broker queue create`.
    """


@verdi_broker_queue.command('list')
@decorators.with_dbenv()
@click.pass_context
def queue_list(ctx):
    """Show the current queue configuration."""
    from tabulate import tabulate

    from aiida.manage.configuration import get_config_option

    profile = ctx.obj.profile
    queue_config = profile.get_queue_config()

    if queue_config is None:
        echo.echo_warning('No queue configured. Create one with: verdi broker queue create')
        return

    default_prefetch = get_config_option('daemon.worker_process_slots')

    headers = ['User Queue', 'Root WorkChain Limit', 'CalcJob Limit']
    table = []
    for queue_name, config in queue_config.items():
        workchain_limit = config.get('root_workchain_prefetch', default_prefetch)
        calcjob_limit = config.get('calcjob_prefetch', DEFAULT_CALCJOB_PREFETCH)
        table.append([queue_name, workchain_limit, calcjob_limit])

    echo.echo(tabulate(table, headers=headers))


@verdi_broker_queue.command('create')
@click.argument('queue_name')
@click.argument('root_workchain_limit', type=str, required=False, default=None)
@click.argument('calcjob_limit', type=str, required=False, default=None)
@decorators.only_if_daemon_not_running(
    message='Cannot create queue while daemon is running. Stop it first with: verdi daemon stop'
)
@decorators.with_dbenv()
@click.pass_context
def queue_create(ctx, queue_name, root_workchain_limit, calcjob_limit):
    """Create a process queue for the current profile.

    \b
    QUEUE_NAME              Name of the queue to create.
    ROOT_WORKCHAIN_LIMIT    Max concurrent root WorkChains per worker (default: daemon.worker_process_slots).
    CALCJOB_LIMIT           Max concurrent CalcJobs per worker (default: UNLIMITED).

    \b
    Examples:
        verdi broker queue create default
        verdi broker queue create default 200
        verdi broker queue create hpc-gpu 50 100
        verdi broker queue create hpc-cpu 10 UNLIMITED
    """
    from aiida.manage.configuration import get_config, get_config_option

    config = get_config()
    profile = ctx.obj.profile

    queue_config = profile.get_queue_config() or {}

    if queue_name in queue_config:
        echo.echo_warning(f'Queue "{queue_name}" already exists. Use `verdi broker queue set` to modify limits.')
        return

    # Parse and apply defaults
    root_workchain_limit = parse_limit(root_workchain_limit)
    calcjob_limit = parse_limit(calcjob_limit)

    if root_workchain_limit is None:
        root_workchain_limit = get_config_option('daemon.worker_process_slots')
    if calcjob_limit is None:
        calcjob_limit = DEFAULT_CALCJOB_PREFETCH

    queue_config[queue_name] = {
        'root_workchain_prefetch': root_workchain_limit,
        'calcjob_prefetch': calcjob_limit,
    }

    profile.set_queue_config(queue_config)
    config.update_profile(profile)
    config.store()

    echo.echo_success(f'Queue "{queue_name}" created successfully.')
    echo.echo_info(f'  Root WorkChain limit: {root_workchain_limit}')
    echo.echo_info(f'  CalcJob limit: {calcjob_limit}')


@verdi_broker_queue.command('delete')
@click.argument('queue_name')
@decorators.only_if_daemon_not_running(
    message='Cannot delete queue while daemon is running. Stop it first with: verdi daemon stop'
)
@decorators.with_dbenv()
@click.option('--force', is_flag=True, help='Delete even if there are tasks in the queue.')
@click.pass_context
def queue_delete(ctx, queue_name, force):
    """Delete a process queue configuration.

    QUEUE_NAME is the name of the queue to delete.

    Note: The 'default' queue cannot be deleted as it is required when a broker is configured.

    Warning: If there are tasks waiting in the queue, they will be orphaned
    and the corresponding processes will need to be repaired with
    `verdi process repair --queue <new_queue>`.
    """
    from aiida.engine.processes.control import get_process_tasks
    from aiida.manage import get_manager
    from aiida.manage.configuration import get_config

    # Prevent deletion of the 'default' queue
    if queue_name == 'default':
        echo.echo_critical("The 'default' queue cannot be deleted. " 'It is required when a broker is configured.')
        return

    config = get_config()
    profile = ctx.obj.profile

    queue_config = profile.get_queue_config() or {}

    if queue_name not in queue_config:
        echo.echo_warning(f'Queue "{queue_name}" does not exist.')
        return

    # Check for tasks in the queues
    broker = get_manager().get_broker()
    if broker is not None:
        task_pks = get_process_tasks(broker)
        task_count = len(task_pks)

        if task_count > 0 and not force:
            echo.echo_warning(f'There are {task_count} task(s) waiting in the queues.')
            echo.echo_warning('Deleting the queue will orphan these tasks.')
            echo.echo_warning('Use `verdi process repair --queue <new_queue>` after deletion to reassign them.')
            echo.echo_info('Use --force to proceed anyway.')
            return

        if task_count > 0 and force:
            echo.echo_warning(f'Deleting queue with {task_count} orphaned task(s).')

    if not force:
        click.confirm(f'Are you sure you want to delete queue "{queue_name}"?', abort=True)

    del queue_config[queue_name]
    profile.set_queue_config(queue_config if queue_config else None)
    config.update_profile(profile)
    config.store()

    echo.echo_success(f'Queue "{queue_name}" deleted.')


@verdi_broker_queue.command('set')
@click.argument('queue_name', default='default', required=False)
@click.argument('process_type', type=click.Choice(['root-workchain', 'calcjob']))
@click.argument('limit', type=str)
@decorators.only_if_daemon_not_running(
    message='Cannot change queue limits while daemon is running. Stop it first with: verdi daemon stop'
)
@decorators.with_dbenv()
@click.pass_context
def queue_set(ctx, queue_name, process_type, limit):
    """Set the concurrent process limit for a process type.

    QUEUE_NAME is the name of the queue to modify (default: 'default').
    PROCESS_TYPE is either 'root-workchain' or 'calcjob'.
    LIMIT is the maximum number of concurrent processes (use UNLIMITED for no limit, 0 to block).

    \b
    Examples:
        verdi broker queue set root-workchain 50        # Limit default queue to 50 root workchains
        verdi broker queue set hpc-gpu calcjob 100      # Limit hpc-gpu queue to 100 calcjobs
        verdi broker queue set calcjob UNLIMITED        # Unlimited calcjobs for default queue
        verdi broker queue set maintenance calcjob 0    # Block all calcjobs on maintenance queue
    """
    from aiida.manage.configuration import get_config

    config = get_config()
    profile = ctx.obj.profile

    queue_config = profile.get_queue_config() or {}

    if queue_name not in queue_config:
        echo.echo_critical(
            f'Queue "{queue_name}" does not exist. Create it first with: verdi broker queue create {queue_name}'
        )

    # Parse the limit value
    parsed_limit = parse_limit(limit)

    # Convert hyphen to underscore for config key
    config_key = f'{process_type.replace("-", "_")}_prefetch'
    queue_config[queue_name][config_key] = parsed_limit

    profile.set_queue_config(queue_config)
    config.update_profile(profile)
    config.store()

    process_name = 'Root WorkChains' if process_type == 'root-workchain' else 'CalcJobs'
    echo.echo_success(f'{process_name} limit for queue "{queue_name}" set to {parsed_limit}.')
