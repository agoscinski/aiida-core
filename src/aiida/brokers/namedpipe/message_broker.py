"""Core message broker logic for task distribution and broadcast fanout."""

from __future__ import annotations

import logging
import os
import selectors
import threading
import typing as t
from pathlib import Path

from . import discovery, messages, utils
from .task_queue import TaskQueue

__all__ = ('MessageBroker',)

LOGGER = logging.getLogger(__name__)

class MessageBroker:
    """Core message broker logic without daemon management.

    This class contains the pure business logic for task distribution,
    broadcast fanout, and worker management. It does NOT handle discovery
    registration or daemon lifecycle management.
    """

    def __init__(
        self,
        profile_name: str,
        working_dir: Path | str,
        create_pipes: bool = True,
    ):
        """Initialize message broker core.

        :param profile_name: Profile name (used to locate pipes directory).
        :param working_dir: Working directory for broker files.
                           Pipe names are derived from this path.
        :param create_pipes: If True, create and own pipes. If False, pipes must be provided externally.
        """
        self._profile_name = profile_name
        self._working_dir = Path(working_dir)
        self._broker_id = 'broker'
        self._create_pipes = create_pipes

        # Broker pipes go in broker subdirectory
        broker_pipes_dir = utils.get_broker_pipes_dir(profile_name)
        self._task_pipe_path = broker_pipes_dir / 'broker_tasks'
        self._broadcast_pipe_path = broker_pipes_dir / 'broker_broadcast'

        # File descriptors
        self._task_fd: int | None = None
        self._broadcast_fd: int | None = None

        # Task queue uses working_dir
        self._task_queue = TaskQueue(self._working_dir)

        # Worker selection (round-robin)
        self._worker_index = 0

        # Selector for event-driven I/O
        self._selector = selectors.DefaultSelector()
        self._selector_thread: threading.Thread | None = None
        self._selector_running = False
        self._selector_lock = threading.Lock()

        # Running flag
        self._closed = False

        # Initialize pipes only if requested
        if self._create_pipes:
            self._init_pipes()

        # Start selector thread only if we created pipes
        if self._create_pipes:
            self._start_selector_thread()

        LOGGER.info(f'MessageBroker started for profile: {profile_name} (create_pipes={create_pipes})')

    def _init_pipes(self) -> None:
        """Initialize message broker pipes."""
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
        """Handle incoming task message from pipe.

        :param fd: File descriptor of the task pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd)
            if message is None:
                return

            self.handle_task_message(message)

        except Exception as exc:
            LOGGER.exception(f'Error handling task: {exc}')

    def handle_task_message(self, message: dict) -> None:
        """Public method to handle a task message (called by Scheduler or internal handler).

        :param message: Task message dict.
        """
        # Persist task
        task_id = self._task_queue.enqueue(message)

        # Distribute to available worker
        self._distribute_task(task_id, message)

    def _distribute_task(self, task_id: str, message: dict) -> None:
        """Distribute a task to an available worker.

        :param task_id: The task ID.
        :param message: The task message.
        """
        # Get available workers
        workers = discovery.discover_workers(self._profile_name, check_alive=True)

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
            utils.write_to_pipe(worker['task_pipe'], messages.serialize(message), non_blocking=True)

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
            message = messages.deserialize_from_fd(fd)
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
        workers = discovery.discover_workers(self._profile_name, check_alive=True)

        data = messages.serialize(message)

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
            cleaned = discovery.cleanup_dead_processes(self._profile_name)
            if cleaned:
                LOGGER.debug(f'Cleaned up {cleaned} dead worker entries')

            # Requeue tasks assigned to dead workers
            self._requeue_orphaned_tasks()

        except Exception as exc:
            LOGGER.exception(f'Error in maintenance: {exc}')

    def _requeue_orphaned_tasks(self) -> None:
        """Requeue tasks that were assigned to dead workers."""
        pending_tasks = self._task_queue.get_pending_tasks()
        workers = discovery.discover_workers(self._profile_name, check_alive=True)
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
        """Close the message broker core and clean up resources."""
        if self._closed:
            return

        self._closed = True

        # Stop selector thread (only if we started one)
        self._selector_running = False
        if self._selector_thread and self._selector_thread.is_alive():
            self._selector_thread.join(timeout=1.0)

        # Close selector (always, since we always create it)
        with self._selector_lock:
            try:
                self._selector.close()
            except Exception as exc:
                LOGGER.warning(f'Error closing selector: {exc}')

        # Close file descriptors and pipes only if we created them
        if self._create_pipes:
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

        LOGGER.info('MessageBroker stopped')

    def is_closed(self) -> bool:
        """Check if message broker core is closed.

        :return: True if closed, False otherwise.
        """
        return self._closed
