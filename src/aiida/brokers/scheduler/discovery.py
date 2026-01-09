###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Discovery system for scheduler process."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from aiida.brokers.namedpipe.discovery import ProcessInfo, _is_process_alive, _read_process_info, _write_process_info, get_discovery_dir

__all__ = ('register_scheduler', 'discover_scheduler', 'unregister_scheduler')


def register_scheduler(
    profile_name: str,
    scheduler_id: str,
    task_pipe: str,
) -> None:
    """Register the scheduler in the discovery system.

    :param profile_name: Profile name.
    :param scheduler_id: Scheduler identifier (usually 'scheduler').
    :param task_pipe: Path to scheduler's task pipe.
    """
    discovery_dir = get_discovery_dir(profile_name)

    info: ProcessInfo = {
        'process_id': scheduler_id,
        'process_type': 'scheduler',
        'task_pipe': task_pipe,
        'rpc_pipe': '',  # Scheduler doesn't have RPC pipe
        'broadcast_pipe': '',  # Scheduler doesn't use broadcasts
        'reply_pipe': None,
        'pid': os.getpid(),
        'alive': True,
        'timestamp': datetime.now().isoformat(),
    }

    _write_process_info(discovery_dir, scheduler_id, info)


def discover_scheduler(profile_name: str, scheduler_id: str = 'scheduler') -> ProcessInfo | None:
    """Discover the scheduler.

    :param profile_name: Profile name.
    :param scheduler_id: Scheduler identifier (default: 'scheduler').
    :return: Scheduler process information or None if not found/dead.
    """
    discovery_dir = get_discovery_dir(profile_name)
    file_path = discovery_dir / f'{scheduler_id}.json'

    if not file_path.exists():
        return None

    info = _read_process_info(file_path)
    if info is None:
        return None

    if info['process_type'] != 'scheduler':
        return None

    # Check if scheduler is alive
    alive = _is_process_alive(info['pid'])
    info['alive'] = alive

    if not alive:
        # Clean up stale entry
        try:
            file_path.unlink()
        except OSError:
            pass
        return None

    return info


def unregister_scheduler(profile_name: str, scheduler_id: str = 'scheduler') -> None:
    """Unregister the scheduler from the discovery system.

    :param profile_name: Profile name.
    :param scheduler_id: Scheduler identifier (default: 'scheduler').
    """
    discovery_dir = get_discovery_dir(profile_name)
    file_path = discovery_dir / f'{scheduler_id}.json'

    try:
        file_path.unlink()
    except FileNotFoundError:
        pass
