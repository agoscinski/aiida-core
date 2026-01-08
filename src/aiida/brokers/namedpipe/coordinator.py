"""Coordinator process for managing task queue and broadcast distribution."""

from __future__ import annotations

import atexit
import json
import logging
import os
import selectors
import threading
import time
import typing as t
import uuid
from pathlib import Path

from . import discovery, messages, utils

__all__ = ('PipeCoordinator',)

LOGGER = logging.getLogger(__name__)


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


class PipeCoordinator:
    """Coordinator for managing task distribution and broadcast fanout."""

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
        encoder: t.Callable | None = None,
        decoder: t.Callable | None = None,
    ):
        """Initialize the coordinator.

        :param profile_name: The AiiDA profile name.
        :param config_path: Path to the AiiDA config directory.
        :param encoder: Optional custom message encoder.
        :param decoder: Optional custom message decoder.
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._encoder = encoder
        self._decoder = decoder

        # Coordinator ID
        self._coordinator_id = 'coordinator'

        # Pipe directory
        self._pipe_dir = utils.get_pipe_dir(profile_name)

        # Coordinator pipes
        self._task_pipe_path = self._pipe_dir / 'coordinator_tasks'
        self._task_fd: int | None = None

        self._broadcast_pipe_path = self._pipe_dir / 'coordinator_broadcast'
        self._broadcast_fd: int | None = None

        # Task queue
        queue_dir = self._config_path / 'coordinator'
        self._task_queue = TaskQueue(queue_dir)

        # Worker selection (round-robin)
        self._worker_index = 0

        # Selector for event-driven I/O
        self._selector = selectors.DefaultSelector()
        self._selector_thread: threading.Thread | None = None
        self._selector_running = False
        self._selector_lock = threading.Lock()

        # Running flag
        self._closed = False

        # Initialize pipes
        self._init_pipes()

        # Register coordinator in discovery
        discovery.register_coordinator(
            self._config_path, self._coordinator_id, task_pipe=str(self._task_pipe_path), broadcast_pipe=str(self._broadcast_pipe_path)
        )

        # Start selector thread
        self._start_selector_thread()

        # Register cleanup
        atexit.register(self.close)

        LOGGER.info(f'PipeCoordinator started for profile: {profile_name}')

    def _init_pipes(self) -> None:
        """Initialize coordinator pipes."""
        # Task pipe
        utils.create_pipe(self._task_pipe_path)
        self._task_fd = utils.open_pipe_read(self._task_pipe_path, non_blocking=True)
        self._selector.register(self._task_fd, selectors.EVENT_READ, self._handle_task)

        # Broadcast pipe
        utils.create_pipe(self._broadcast_pipe_path)
        self._broadcast_fd = utils.open_pipe_read(self._broadcast_pipe_path, non_blocking=True)
        self._selector.register(self._broadcast_fd, selectors.EVENT_READ, self._handle_broadcast)

    def _start_selector_thread(self) -> None:
        """Start the background selector thread."""
        self._selector_running = True
        self._selector_thread = threading.Thread(target=self._selector_loop, daemon=True)
        self._selector_thread.start()

    def _selector_loop(self) -> None:
        """Main selector event loop (runs in background thread)."""
        while self._selector_running:
            try:
                with self._selector_lock:
                    if not self._selector_running:
                        break
                    events = self._selector.select(timeout=0.1)

                for key, mask in events:
                    callback = key.data
                    if callback:
                        try:
                            callback(key.fileobj)
                        except Exception as exc:
                            LOGGER.exception(f'Error in selector callback: {exc}')

            except Exception as exc:
                if self._selector_running:
                    LOGGER.exception(f'Error in selector loop: {exc}')

    def _handle_task(self, fd: int) -> None:
        """Handle incoming task message.

        :param fd: File descriptor of the task pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Persist task
            task_id = self._task_queue.enqueue(message)

            # Distribute to available worker
            self._distribute_task(task_id, message)

        except Exception as exc:
            LOGGER.exception(f'Error handling task: {exc}')

    def _distribute_task(self, task_id: str, message: dict) -> None:
        """Distribute a task to an available worker.

        :param task_id: The task ID.
        :param message: The task message.
        """
        # Get available workers
        workers = discovery.discover_workers(self._config_path, check_alive=True)

        if not workers:
            LOGGER.warning(f'No workers available for task {task_id}')
            return

        # Round-robin worker selection
        worker = workers[self._worker_index % len(workers)]
        self._worker_index += 1

        # Sanitize reply pipe - if original client is dead, update to worker's reply pipe
        self._sanitize_reply_pipe(message, worker)

        try:
            # Send task to worker
            utils.write_to_pipe(worker['task_pipe'], messages.serialize(message, encoder=self._encoder), non_blocking=True)

            # Mark as assigned
            self._task_queue.mark_assigned(task_id, worker['process_id'])

            LOGGER.debug(f'Distributed task {task_id} to worker {worker["process_id"]}')

        except (BrokenPipeError, FileNotFoundError, OSError) as exc:
            LOGGER.warning(f'Worker {worker["process_id"]} pipe unavailable ({type(exc).__name__}), requeueing task {task_id}')
            # Task remains in pending state for next distribution attempt

    def _sanitize_reply_pipe(self, message: dict, worker: dict) -> None:
        """Update reply_pipe in message if original no longer exists.

        If the original reply pipe is gone (client died), update it to point to
        the worker's reply pipe so the result can still be delivered.

        :param message: The task message to sanitize (modified in place).
        :param worker: The worker info dict that will process this task.
        """
        reply_pipe = message.get('reply_pipe')
        if reply_pipe:
            # Check if original reply pipe still exists
            from pathlib import Path

            if not Path(reply_pipe).exists():
                # Original client is dead, update to worker's reply pipe
                worker_reply_pipe = worker.get('reply_pipe')
                LOGGER.debug(
                    f'Reply pipe no longer exists: {reply_pipe}, '
                    f'updating to worker reply pipe: {worker_reply_pipe}'
                )
                message['reply_pipe'] = worker_reply_pipe

    def _handle_broadcast(self, fd: int) -> None:
        """Handle incoming broadcast message.

        :param fd: File descriptor of the broadcast pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Fanout to all workers with broadcast subscribers
            self._fanout_broadcast(message)

        except Exception as exc:
            LOGGER.exception(f'Error handling broadcast: {exc}')

    def _fanout_broadcast(self, message: dict) -> None:
        """Fanout broadcast message to all subscribers.

        :param message: The broadcast message.
        """
        # Get all workers
        workers = discovery.discover_workers(self._config_path, check_alive=True)

        data = messages.serialize(message, encoder=self._encoder)

        for worker in workers:
            try:
                # Non-blocking write to avoid blocking on one worker
                utils.write_to_pipe(worker['broadcast_pipe'], data, non_blocking=True)
            except BrokenPipeError:
                LOGGER.debug(f'Worker {worker["process_id"]} broadcast pipe unavailable')
            except Exception as exc:
                LOGGER.warning(f'Error sending broadcast to worker {worker["process_id"]}: {exc}')

    def run_maintenance(self) -> None:
        """Run maintenance tasks (cleanup old tasks, dead workers, etc.)."""
        try:
            # Cleanup old completed tasks
            cleaned = self._task_queue.cleanup_old_tasks()
            if cleaned:
                LOGGER.debug(f'Cleaned up {cleaned} old tasks')

            # Cleanup dead worker entries
            cleaned = discovery.cleanup_dead_processes(self._config_path)
            if cleaned:
                LOGGER.debug(f'Cleaned up {cleaned} dead worker entries')

            # Requeue tasks assigned to dead workers
            self._requeue_orphaned_tasks()

        except Exception as exc:
            LOGGER.exception(f'Error in maintenance: {exc}')

    def _requeue_orphaned_tasks(self) -> None:
        """Requeue tasks that were assigned to dead workers."""
        pending_tasks = self._task_queue.get_pending_tasks()
        workers = discovery.discover_workers(self._config_path, check_alive=True)
        worker_ids = {w['process_id'] for w in workers}

        for task_data in pending_tasks:
            if task_data.get('status') == 'assigned':
                assigned_to = task_data.get('assigned_to')
                if assigned_to and assigned_to not in worker_ids:
                    # Worker is dead, requeue task
                    task_id = task_data['id']
                    LOGGER.info(f'Requeueing task {task_id} from dead worker {assigned_to}')
                    self._distribute_task(task_id, task_data['message'])

    def close(self) -> None:
        """Close the coordinator and clean up resources."""
        if self._closed:
            return

        self._closed = True

        # Stop selector thread
        self._selector_running = False
        if self._selector_thread and self._selector_thread.is_alive():
            self._selector_thread.join(timeout=1.0)

        # Close and unregister all file descriptors
        with self._selector_lock:
            try:
                self._selector.close()
            except Exception as exc:
                LOGGER.warning(f'Error closing selector: {exc}')

        # Close file descriptors
        for fd in [self._task_fd, self._broadcast_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

        # Clean up pipes
        utils.cleanup_pipe(self._task_pipe_path)
        utils.cleanup_pipe(self._broadcast_pipe_path)

        # Unregister from discovery
        try:
            discovery.unregister_coordinator(self._config_path, self._coordinator_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering coordinator: {exc}')

        LOGGER.info('PipeCoordinator stopped')

    def is_closed(self) -> bool:
        """Check if coordinator is closed.

        :return: True if closed, False otherwise.
        """
        return self._closed
