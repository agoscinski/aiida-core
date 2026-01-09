###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Persistent per-computer process queue."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

__all__ = ('ComputerQueue',)


class ComputerQueue:
    """Persistent queue for processes targeting a specific computer.

    Each computer has its own queue stored on disk, allowing the scheduler
    to persist queued processes across restarts.
    """

    def __init__(self, computer_label: str, queue_dir: Path):
        """Initialize the computer queue.

        :param computer_label: The computer label for this queue.
        :param queue_dir: Directory for storing queue files for this computer.
        """
        self.computer_label = computer_label
        self.queue_dir = queue_dir
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def enqueue(self, message: dict) -> str:
        """Add process to queue.

        :param message: The schedule request message containing pid and other info.
        :return: Task ID for the queued process.
        """
        with self._lock:
            task_id = f'proc_{int(time.time() * 1000)}_{message["pid"]}'

            task_file = self.queue_dir / f'{task_id}.json'
            task_data = {
                'id': task_id,
                'message': message,
                'status': 'pending',
                'enqueued_at': time.time(),
            }

            with open(task_file, 'w') as f:
                json.dump(task_data, f)

            return task_id

    def get_pending(self) -> dict | None:
        """Get next pending process (FIFO order).

        :return: Task data dict or None if no pending tasks.
        """
        with self._lock:
            pending_files = sorted(self.queue_dir.glob('proc_*.json'))

            for task_file in pending_files:
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                        if task_data.get('status') == 'pending':
                            return task_data
                except (json.JSONDecodeError, OSError):
                    pass

        return None

    def mark_running(self, task_id: str):
        """Mark process as running.

        :param task_id: The task ID to mark as running.
        """
        with self._lock:
            task_file = self.queue_dir / f'{task_id}.json'
            if task_file.exists():
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)

                    task_data['status'] = 'running'
                    task_data['started_at'] = time.time()

                    with open(task_file, 'w') as f:
                        json.dump(task_data, f)
                except (json.JSONDecodeError, OSError):
                    pass

    def remove_running(self, pid: int) -> bool:
        """Remove completed process by PID.

        :param pid: Process ID to remove.
        :return: True if found and removed, False otherwise.
        """
        with self._lock:
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)

                    if task_data['message']['pid'] == pid and task_data['status'] == 'running':
                        task_file.unlink()
                        return True

                except (json.JSONDecodeError, OSError):
                    pass

        return False

    def get_running_pids(self) -> list[int]:
        """Get list of PIDs currently marked as running.

        :return: List of process IDs marked as running in this queue.
        """
        pids = []
        with self._lock:
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)

                    if task_data['status'] == 'running':
                        pids.append(task_data['message']['pid'])

                except (json.JSONDecodeError, OSError):
                    pass

        return pids

    def cleanup_old_tasks(self, max_age_seconds: int = 86400):
        """Clean up old completed tasks.

        :param max_age_seconds: Maximum age in seconds (default: 1 day).
        """
        with self._lock:
            current_time = time.time()
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    mtime = task_file.stat().st_mtime
                    if current_time - mtime > max_age_seconds:
                        task_file.unlink()
                except OSError:
                    pass
