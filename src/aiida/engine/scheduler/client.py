###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Client for querying the running scheduler process."""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

__all__ = ('SchedulerClient', 'get_scheduler_client')


class SchedulerClient:
    """Client for communicating with the running scheduler process.

    Provides methods to query scheduler state via IPC (named pipes).
    """

    def __init__(self, profile_name: str):
        """Initialize scheduler client.

        :param profile_name: AiiDA profile name
        """
        self._profile_name = profile_name

    def is_running(self) -> bool:
        """Check if scheduler is running.

        :return: True if scheduler is running, False otherwise
        """
        from aiida.communication.namedpipe import discovery

        existing = discovery.discover_broker(self._profile_name)
        return existing is not None

    def get_queue_status(self, pids: list[int], timeout: float = 5.0) -> dict[int, str | None]:
        """Query scheduler for queue status of given PIDs.

        :param pids: List of process IDs to query
        :param timeout: Timeout in seconds for the query
        :return: Dict mapping PID to status:
            - 'queued' - waiting in ProcessQueue (pending)
            - 'scheduled' - sent to worker via scheduler (running)
            - None - not tracked (unlimited queue or not in queue)
        :raises RuntimeError: If scheduler is not running or communication fails
        """
        from aiida.communication.namedpipe import discovery, messages, utils

        # Check if scheduler is running
        existing = discovery.discover_broker(self._profile_name)
        if not existing:
            raise RuntimeError('Scheduler is not running')

        broker_task_pipe = existing.get('task_pipe')
        if not broker_task_pipe:
            raise RuntimeError('Scheduler task pipe not found')

        # Create temporary reply pipe
        reply_pipe_path = Path(tempfile.gettempdir()) / f'aiida_query_{uuid.uuid4().hex[:8]}'

        try:
            # Create the reply pipe
            utils.create_pipe(reply_pipe_path)

            # Build query message
            query = {
                'type': 'query_queue_status',
                'pids': pids,
                'reply_pipe': str(reply_pipe_path),
            }

            # Send query to scheduler
            data = messages.serialize(query)
            utils.write_to_pipe(broker_task_pipe, data, non_blocking=False, use_lock=True)

            # Wait for response
            response = self._read_response(reply_pipe_path, timeout)

            if response.get('type') != 'queue_status_response':
                raise RuntimeError(f'Unexpected response type: {response.get("type")}')

            # Convert string keys back to integers (JSON doesn't support int keys)
            raw_status = response.get('status', {})
            return {int(k): v for k, v in raw_status.items()}

        finally:
            # Cleanup reply pipe
            utils.cleanup_pipe(reply_pipe_path)

    def _read_response(self, reply_pipe: Path, timeout: float) -> dict:
        """Read response from reply pipe with timeout.

        :param reply_pipe: Path to reply pipe
        :param timeout: Timeout in seconds
        :return: Response message dict
        :raises TimeoutError: If no response within timeout
        :raises RuntimeError: If response cannot be parsed
        """
        import select

        from aiida.communication.namedpipe import messages, utils

        # Open pipe for reading with select-based timeout
        fd = None
        try:
            fd = utils.open_pipe_read(reply_pipe, non_blocking=True)

            # Wait for data with timeout
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                raise TimeoutError(f'No response from scheduler within {timeout}s')

            # Read and parse response (blocking read after select confirms data available)
            response = messages.deserialize_from_fd(fd)
            if response is None:
                raise RuntimeError('Empty response from scheduler')

            return response

        finally:
            if fd is not None:
                os.close(fd)


def get_scheduler_client(profile_name: str | None = None) -> SchedulerClient:
    """Get scheduler client for the given or current profile.

    :param profile_name: Profile name (uses current profile if None)
    :return: SchedulerClient instance
    """
    if profile_name is None:
        from aiida.manage import get_manager

        mgr = get_manager()
        profile = mgr.get_profile()
        profile_name = profile.name

    return SchedulerClient(profile_name)
