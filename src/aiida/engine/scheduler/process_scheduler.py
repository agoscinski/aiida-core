###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Unified process broker with optional per-computer scheduling."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from aiida.communication.namedpipe import discovery
from aiida.engine.scheduler.task_queue import TaskQueue

__all__ = ('ProcessScheduler', 'ProcessSchedulerConfig', 'ComputerQueue')

LOGGER = logging.getLogger(__name__)


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
        self._lock = __import__('threading').Lock()

    def enqueue(self, message: dict) -> str:
        """Add process to queue.

        :param message: The schedule request message containing pid and other info.
        :return: Task ID for the queued process.
        """
        import time

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
        import time

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
        import time

        with self._lock:
            current_time = time.time()
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    mtime = task_file.stat().st_mtime
                    if current_time - mtime > max_age_seconds:
                        task_file.unlink()
                except OSError:
                    pass


class ProcessSchedulerConfig:
    """Configuration for ProcessScheduler.

    Encapsulates all configuration options in a single object.
    """

    def __init__(
        self,
        scheduling_enabled: bool = False,
        computer_limits: dict[str, int] | None = None,
        default_limit: int = 10,
        # Worker management options
        auto_spawn_workers: bool = False,
        initial_worker_count: int = 0,
        auto_respawn_workers: bool = False,
        min_worker_count: int = 0,
        max_worker_count: int = 10,
    ):
        """Initialize broker configuration.

        :param scheduling_enabled: Enable per-computer scheduling
        :param computer_limits: Dict mapping computer labels to concurrency limits
        :param default_limit: Default limit for computers without explicit config
        :param auto_spawn_workers: Auto-spawn workers when needed
        :param initial_worker_count: Number of workers to spawn at startup
        :param auto_respawn_workers: Auto-respawn dead workers
        :param min_worker_count: Minimum number of workers to maintain
        :param max_worker_count: Maximum number of workers allowed
        """
        self.scheduling_enabled = scheduling_enabled
        self.computer_limits = computer_limits or {}
        self.default_limit = default_limit

        # Worker management
        self.auto_spawn_workers = auto_spawn_workers
        self.initial_worker_count = initial_worker_count
        self.auto_respawn_workers = auto_respawn_workers
        self.min_worker_count = min_worker_count
        self.max_worker_count = max_worker_count

    @classmethod
    def from_file(cls, config_path: Path) -> 'ProcessSchedulerConfig':
        """Load configuration from file.

        :param config_path: Path to config directory (contains scheduler/)
        :return: ProcessSchedulerConfig instance
        """
        config_file = config_path / 'scheduler' / 'computer_limits.json'

        # If file doesn't exist, scheduling is disabled
        if not config_file.exists():
            return cls(scheduling_enabled=False)

        try:
            with open(config_file) as f:
                data = json.load(f)

            # Extract metadata fields (keys starting with _)
            scheduling_enabled = data.pop('_scheduling_enabled', True)
            default_limit = data.pop('_default_limit', 10)

            # Worker management options
            auto_spawn_workers = data.pop('_auto_spawn_workers', False)
            initial_worker_count = data.pop('_initial_worker_count', 0)
            auto_respawn_workers = data.pop('_auto_respawn_workers', False)
            min_worker_count = data.pop('_min_worker_count', 0)
            max_worker_count = data.pop('_max_worker_count', 10)

            # Remaining keys are computer limits
            return cls(
                scheduling_enabled=scheduling_enabled,
                computer_limits=data,
                default_limit=default_limit,
                auto_spawn_workers=auto_spawn_workers,
                initial_worker_count=initial_worker_count,
                auto_respawn_workers=auto_respawn_workers,
                min_worker_count=min_worker_count,
                max_worker_count=max_worker_count,
            )
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning(f'Failed to load config: {exc}, using defaults')
            return cls(scheduling_enabled=False)


class ProcessScheduler:
    """Unified scheduler for task distribution and worker management.

    This class handles:
    - Task queue persistence
    - Worker discovery and selection
    - Task distribution
    - Broadcast fanout
    - Optional per-computer scheduling

    Transport operations are delegated to BrokerCommunicator (dependency injection).
    This class contains NO pipe operations - pure business logic.
    """

    def __init__(
        self,
        communicator: 'BrokerCommunicator',  # noqa: F821
        profile_name: str,
        working_dir: Path | str,
        config: ProcessSchedulerConfig | None = None,
        executor: 'WorkerExecutor | None' = None,  # noqa: F821
    ):
        """Initialize the process broker.

        :param communicator: Transport abstraction for broker communications
        :param profile_name: AiiDA profile name
        :param working_dir: Working directory for queue files
        :param config: Broker configuration (None = scheduling disabled)
        :param executor: Optional worker executor for lifecycle management
        """
        self._communicator = communicator
        self._profile_name = profile_name
        self._working_dir = Path(working_dir)
        self._config = config or ProcessSchedulerConfig(scheduling_enabled=False)
        self._executor = executor

        # Task queue for persistence
        self._task_queue = TaskQueue(self._working_dir / 'tasks')

        # Worker selection (round-robin)
        self._worker_index = 0

        # Scheduling state (only used if scheduling enabled)
        self._queues: dict[str, ComputerQueue] = {}
        self._running_counts: dict[str, int] = {}

        # Register callbacks with communicator
        self._communicator.add_task_subscriber(self._on_task_received, 'process_broker')
        self._communicator.add_broadcast_subscriber(self._on_broadcast_received, 'process_broker')

        # Start communicator
        self._communicator.start()

        LOGGER.info(
            f'ProcessScheduler started for profile: {profile_name} '
            f'(scheduling={"enabled" if self._config.scheduling_enabled else "disabled"})'
        )

    def _on_task_received(self, message: dict) -> None:
        """Callback invoked when task message received from communicator.

        This is the entry point for all incoming task messages. Determines
        whether to apply scheduling or handle immediately.

        :param message: Task message dict
        """
        if self._config.scheduling_enabled:
            # Check if this is a continue_process task requiring scheduling
            task_type = message.get('body', {}).get('task')
            if task_type == 'continue':
                pid = message['body']['process_id']
                self._schedule_process(pid, message)
                return

        # Non-scheduled tasks or scheduling disabled: handle directly
        self._handle_task_message(message)

    def _on_broadcast_received(self, message: dict) -> None:
        """Callback invoked when broadcast message received from communicator.

        :param message: Broadcast message dict
        """
        self._fanout_broadcast(message)

    def _handle_task_message(self, message: dict) -> None:
        """Handle a task message (persist and distribute).

        :param message: Task message dict
        """
        # Persist task
        task_id = self._task_queue.enqueue(message)

        # Distribute to available worker
        self._distribute_task(task_id, message)

    def _distribute_task(self, task_id: str, message: dict) -> None:
        """Distribute a task to an available worker.

        Uses round-robin worker selection and delegates to communicator for sending.

        :param task_id: Task ID
        :param message: Task message
        """
        # Get available workers (from executor if available, otherwise discovery)
        if self._executor:
            workers = self._executor.get_workers()
        else:
            workers = discovery.discover_workers(self._profile_name, check_alive=True)

        if not workers:
            LOGGER.warning(f'No workers available for task {task_id}')
            return

        # Round-robin worker selection
        worker = workers[self._worker_index % len(workers)]
        self._worker_index += 1

        # Get worker identifier (worker_id for executor, process_id for discovery)
        worker_id = worker.get('worker_id', worker.get('process_id', 'unknown'))

        # Sanitize reply pipe
        self._sanitize_reply_pipe(message, worker)

        try:
            # Send task via communicator
            self._communicator.task_send(worker['task_pipe'], message, non_blocking=True)

            # Mark as assigned
            self._task_queue.mark_assigned(task_id, worker_id)

            LOGGER.debug(f'Distributed task {task_id} to worker {worker_id}')

        except (BrokenPipeError, FileNotFoundError, OSError) as exc:
            LOGGER.warning(
                f'Worker {worker_id} pipe unavailable ({type(exc).__name__}), ' f'requeueing task {task_id}'
            )

    def _sanitize_reply_pipe(self, message: dict, worker: dict) -> None:
        """Update reply_pipe in message if original no longer exists.

        If the original reply pipe is gone (client died), update it to point to
        the worker's reply pipe so the result can still be delivered.

        :param message: Task message (modified in place)
        :param worker: Worker info dict
        """
        reply_pipe = message.get('reply_pipe')
        if reply_pipe:
            if not Path(reply_pipe).exists():
                worker_reply_pipe = worker.get('reply_pipe')
                LOGGER.debug(
                    f'Reply pipe no longer exists: {reply_pipe}, '
                    f'updating to worker reply pipe: {worker_reply_pipe}'
                )
                message['reply_pipe'] = worker_reply_pipe

    def _fanout_broadcast(self, message: dict) -> None:
        """Fanout broadcast message to all workers.

        :param message: Broadcast message
        """
        # Get all workers
        workers = discovery.discover_workers(self._profile_name, check_alive=True)

        # Extract worker broadcast pipes
        worker_pipes = [w['broadcast_pipe'] for w in workers]

        # Send via communicator
        success_count = self._communicator.broadcast_send(worker_pipes, message, non_blocking=True)

        LOGGER.debug(f'Broadcast sent to {success_count}/{len(workers)} workers')

    def _schedule_process(self, pid: int, message: dict) -> None:
        """Apply per-computer scheduling logic.

        Checks concurrency limits and either executes immediately or queues.

        :param pid: Process ID
        :param message: Task message
        """
        from aiida.orm import load_node

        try:
            node = load_node(pid)
            computer_label = self._get_computer_label(node)
        except Exception as exc:
            LOGGER.error(f'Failed to load node {pid}: {exc}')
            # Execute anyway - don't block if we can't determine computer
            self._handle_task_message(message)
            return

        # Get or create queue for this computer
        if computer_label not in self._queues:
            queue_dir = self._working_dir / 'scheduler' / computer_label
            self._queues[computer_label] = ComputerQueue(computer_label, queue_dir)
            LOGGER.info(f'Created queue for computer: {computer_label}')

        # Check limit
        current = self._running_counts.get(computer_label, 0)
        limit = self._config.computer_limits.get(computer_label, self._config.default_limit)

        if current < limit:
            # Under limit - execute immediately
            self._handle_task_message(message)
            self._running_counts[computer_label] = current + 1
            LOGGER.info(f'Executed process {pid} on {computer_label} ({current + 1}/{limit})')
        else:
            # At limit - enqueue
            self._queues[computer_label].enqueue({'pid': pid, 'message': message})
            LOGGER.info(f'Queued process {pid} for {computer_label} (at limit {current}/{limit})')

    def _get_computer_label(self, node) -> str:
        """Get computer label from node.

        :param node: ProcessNode instance
        :return: Computer label
        """
        if hasattr(node, 'computer') and node.computer is not None:
            return node.computer.label
        return 'localhost'

    def on_completion(self, pid: int) -> None:
        """Handle process completion (decrement count, try submit next).

        This method should be called externally when a process completes.

        :param pid: Process ID that completed
        """
        if not self._config.scheduling_enabled:
            return

        from aiida.orm import load_node

        try:
            node = load_node(pid)
            computer_label = self._get_computer_label(node)
        except Exception as exc:
            LOGGER.warning(f'Could not determine computer for {pid}: {exc}')
            return

        # Decrement count
        current = self._running_counts.get(computer_label, 0)
        if current > 0:
            self._running_counts[computer_label] = current - 1
            LOGGER.info(f'Process {pid} completed on {computer_label}. New count: {current - 1}')

        # Try to submit next queued process
        self._try_submit_next(computer_label)

    def _try_submit_next(self, computer_label: str) -> None:
        """Try to submit next queued process.

        :param computer_label: Computer label
        """
        queue = self._queues.get(computer_label)
        if not queue:
            return

        current = self._running_counts.get(computer_label, 0)
        limit = self._config.computer_limits.get(computer_label, self._config.default_limit)

        if current >= limit:
            LOGGER.debug(f'Computer {computer_label} still at limit ({current}/{limit})')
            return

        # Get next pending process
        pending = queue.get_pending()
        if not pending:
            return

        # Submit via broker
        try:
            pid = pending['message']['pid']
            message = pending['message']['message']
            self._handle_task_message(message)

            # Mark as running
            queue.mark_running(pending['id'])
            self._running_counts[computer_label] = current + 1

            LOGGER.info(f'Submitted queued process {pid} to {computer_label} ({current + 1}/{limit})')

        except Exception as exc:
            LOGGER.error(f'Failed to submit process: {exc}')

    def run_maintenance(self) -> None:
        """Run periodic maintenance tasks.

        This should be called periodically (e.g., every second) to perform:
        - Cleanup of old completed tasks
        - Cleanup of dead worker entries (or executor health checks)
        - Requeue tasks assigned to dead workers
        - Poll for missed completions (if scheduling enabled)
        """
        try:
            # Cleanup old completed tasks
            cleaned = self._task_queue.cleanup_old_tasks()
            if cleaned:
                LOGGER.debug(f'Cleaned up {cleaned} old tasks')

            # Worker health checks
            if self._executor:
                # Use executor health checks if available
                dead_workers = self._executor.check_workers_health()
                if dead_workers:
                    LOGGER.warning(f'Executor cleaned up {len(dead_workers)} dead/hung workers')
            else:
                # Fallback to discovery-based cleanup
                cleaned = discovery.cleanup_dead_processes(self._profile_name)
                if cleaned:
                    LOGGER.debug(f'Cleaned up {cleaned} dead worker entries')

            # Requeue tasks assigned to dead workers
            self._requeue_orphaned_tasks()

            # Poll for missed completions (if scheduling enabled)
            if self._config.scheduling_enabled:
                self._poll_running_processes()

        except Exception as exc:
            LOGGER.exception(f'Error in maintenance: {exc}')

    def _requeue_orphaned_tasks(self) -> None:
        """Requeue tasks assigned to dead workers."""
        pending_tasks = self._task_queue.get_pending_tasks()

        # Get worker IDs from executor or discovery
        if self._executor:
            workers = self._executor.get_workers()
            worker_ids = {w['worker_id'] for w in workers}
        else:
            workers = discovery.discover_workers(self._profile_name, check_alive=True)
            worker_ids = {w['process_id'] for w in workers}

        for task_data in pending_tasks:
            if task_data.get('status') == 'assigned':
                assigned_to = task_data.get('assigned_to')
                if assigned_to and assigned_to not in worker_ids:
                    task_id = task_data['id']
                    LOGGER.info(f'Requeueing task {task_id} from dead worker {assigned_to}')
                    self._distribute_task(task_id, task_data['message'])

    def _poll_running_processes(self) -> None:
        """Poll database for missed completions.

        Queries the database to find processes that are marked as completed
        in the database but were not properly handled by the scheduler.
        """
        from aiida.orm import ProcessNode, QueryBuilder

        for computer_label, queue in self._queues.items():
            pids = queue.get_running_pids()
            if not pids:
                continue

            # Query database for these processes
            qb = QueryBuilder()
            qb.append(ProcessNode, filters={'id': {'in': pids}}, project=['id', 'attributes.process_state'])

            for pid, state in qb.all():
                if state in ['finished', 'failed', 'killed', 'excepted']:
                    LOGGER.warning(f'Missed completion event for {pid}, handling now')
                    self.on_completion(pid)

    def get_status(self) -> dict:
        """Get broker status for CLI display.

        :return: Status dict with scheduling info if enabled
        """
        # Get worker count from executor or discovery
        if self._executor:
            worker_count = self._executor.get_worker_count()
        else:
            worker_count = len(discovery.discover_workers(self._profile_name, check_alive=True))

        status = {
            'scheduling_enabled': self._config.scheduling_enabled,
            'worker_count': worker_count,
        }

        if self._config.scheduling_enabled:
            computers = {}
            all_computer_labels = set(list(self._queues.keys()) + list(self._config.computer_limits.keys()))

            for computer_label in all_computer_labels:
                running = self._running_counts.get(computer_label, 0)
                limit = self._config.computer_limits.get(computer_label, self._config.default_limit)
                queue = self._queues.get(computer_label)

                if queue:
                    queued = len(list(queue.queue_dir.glob('proc_*.json')))
                else:
                    queued = 0

                computers[computer_label] = {'running': running, 'limit': limit, 'queued': queued}

            status['computers'] = computers

        return status

    def close(self) -> None:
        """Close the broker and cleanup resources."""
        self._communicator.close()
        LOGGER.info('ProcessScheduler stopped')

    def is_closed(self) -> bool:
        """Check if broker is closed.

        :return: True if closed, False otherwise
        """
        return self._communicator.is_closed()
