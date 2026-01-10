"""Simple file-based task queue for persistence."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

__all__ = ('TaskQueue',)


class TaskQueue:
    """Simple file-based task queue for persistence."""

    def __init__(self, queue_dir: Path):
        """Initialize the task queue.

        :param queue_dir: Directory for storing task queue files.
        """
        self.queue_dir = queue_dir
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self._task_counter = 0
        self._lock = threading.Lock()

    def enqueue(self, task_message: dict) -> str:
        """Add a task to the queue.

        :param task_message: The task message to enqueue.
        :return: Task ID.
        """
        with self._lock:
            task_id = f'task_{int(time.time() * 1000)}_{self._task_counter}'
            self._task_counter += 1

            task_file = self.queue_dir / f'{task_id}.json'
            task_data = {'id': task_id, 'message': task_message, 'status': 'pending', 'assigned_to': None}

            with open(task_file, 'w') as f:
                json.dump(task_data, f)

            return task_id

    def get_pending_tasks(self) -> list[dict]:
        """Get all pending tasks.

        :return: List of pending task data.
        """
        pending = []
        with self._lock:
            for task_file in self.queue_dir.glob('task_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                        if task_data.get('status') == 'pending':
                            pending.append(task_data)
                except (json.JSONDecodeError, OSError):
                    pass
        return pending

    def mark_assigned(self, task_id: str, worker_id: str) -> None:
        """Mark a task as assigned to a worker.

        :param task_id: The task ID.
        :param worker_id: The worker ID.
        """
        with self._lock:
            task_file = self.queue_dir / f'{task_id}.json'
            if not task_file.exists():
                return

            try:
                with open(task_file) as f:
                    task_data = json.load(f)

                task_data['status'] = 'assigned'
                task_data['assigned_to'] = worker_id

                with open(task_file, 'w') as f:
                    json.dump(task_data, f)
            except (json.JSONDecodeError, OSError):
                pass

    def mark_completed(self, task_id: str) -> None:
        """Mark a task as completed and remove it.

        :param task_id: The task ID.
        """
        with self._lock:
            task_file = self.queue_dir / f'{task_id}.json'
            try:
                task_file.unlink()
            except FileNotFoundError:
                pass

    def cleanup_old_tasks(self, max_age_seconds: int = 86400) -> int:
        """Clean up old task files.

        :param max_age_seconds: Maximum age in seconds (default: 1 day).
        :return: Number of tasks cleaned up.
        """
        count = 0
        with self._lock:
            current_time = time.time()
            for task_file in self.queue_dir.glob('task_*.json'):
                try:
                    mtime = task_file.stat().st_mtime
                    if current_time - mtime > max_age_seconds:
                        task_file.unlink()
                        count += 1
                except OSError:
                    pass
        return count
