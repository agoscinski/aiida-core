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

import copy
import json
import logging
from pathlib import Path

from aiida.communication.namedpipe import discovery

__all__ = ('ProcessScheduler', 'ProcessSchedulerConfig', 'ProcessQueue')

LOGGER = logging.getLogger(__name__)


class ProcessQueue:
    """Persistent queue for processes with a specific queue identifier.

    Each queue_id has its own queue stored on disk, allowing the scheduler
    to persist queued processes across restarts.

    Queue identifiers follow the format:
    - 'COMPUTER__<computer_label>' for CalcJobs (e.g., 'COMPUTER__localhost')
    - 'LOCAL' for WorkChains/CalcFunctions
    """

    def __init__(self, queue_id: str, queue_dir: Path):
        """Initialize the process queue.

        :param queue_id: The queue identifier (e.g., 'COMPUTER__localhost' or 'LOCAL').
        :param queue_dir: Directory for storing queue files for this queue.
        """
        self.queue_id = queue_id
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

    def get_all_pids(self) -> list[int]:
        """Get list of all PIDs in the queue (pending and running).

        :return: List of all process IDs in this queue.
        """
        pids = []
        with self._lock:
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                    pids.append(task_data['message']['pid'])
                except (json.JSONDecodeError, OSError, KeyError):
                    pass
        return pids

    def remove_by_pid(self, pid: int) -> bool:
        """Remove queue entry by PID (any status).

        :param pid: Process ID to remove.
        :return: True if removed, False if not found.
        """
        with self._lock:
            for task_file in self.queue_dir.glob('proc_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                    if task_data['message']['pid'] == pid:
                        task_file.unlink()
                        return True
                except (json.JSONDecodeError, OSError, KeyError):
                    pass
        return False

    def cleanup_old_tasks(self, max_age_seconds: int = 86400) -> int:
        """Clean up old completed tasks.

        :param max_age_seconds: Maximum age in seconds (default: 1 day).
        :return: Number of tasks cleaned up.
        """
        import time

        count = 0
        with self._lock:
            current_time = time.time()
            for task_file in self._glob_task_files():
                try:
                    mtime = task_file.stat().st_mtime
                    if current_time - mtime > max_age_seconds:
                        task_file.unlink()
                        count += 1
                except OSError:
                    pass
        return count

    def _glob_task_files(self):
        """Get all task files in queue directory.

        Supports both 'proc_*.json' (throttling queues) and 'task_*.json' (general task queue).
        """
        yield from self.queue_dir.glob('proc_*.json')
        yield from self.queue_dir.glob('task_*.json')

    # --- Methods below are for general task distribution ---

    def enqueue_task(self, task_message: dict) -> str:
        """Add a task to the queue (for general task distribution).

        Unlike enqueue() which expects {'pid': ..., 'message': ...}, this method
        stores the task message directly with worker assignment tracking.

        :param task_message: The task message to enqueue.
        :return: Task ID.
        """
        import time

        with self._lock:
            task_id = f'task_{int(time.time() * 1000)}_{id(task_message) % 10000}'

            task_file = self.queue_dir / f'{task_id}.json'
            task_data = {
                'id': task_id,
                'message': task_message,
                'status': 'pending',
                'assigned_to': None,
                'enqueued_at': time.time(),
            }

            with open(task_file, 'w') as f:
                json.dump(task_data, f)

            return task_id

    def get_pending_tasks(self) -> list[dict]:
        """Get all pending tasks.

        :return: List of pending task data dicts.
        """
        pending = []
        with self._lock:
            for task_file in self._glob_task_files():
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                        if task_data.get('status') == 'pending':
                            pending.append(task_data)
                except (json.JSONDecodeError, OSError):
                    pass
        return pending

    def get_all_tasks(self) -> list[dict]:
        """Get all tasks (pending and assigned).

        :return: List of all task data dicts.
        """
        tasks = []
        with self._lock:
            for task_file in self._glob_task_files():
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)
                        tasks.append(task_data)
                except (json.JSONDecodeError, OSError):
                    pass
        return tasks

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

    def reset_to_pending(self, task_id: str) -> None:
        """Reset an assigned task back to pending status.

        :param task_id: The task ID to reset.
        """
        with self._lock:
            task_file = self.queue_dir / f'{task_id}.json'
            if not task_file.exists():
                return

            try:
                with open(task_file) as f:
                    task_data = json.load(f)

                task_data['status'] = 'pending'
                task_data['assigned_to'] = None

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


class ProcessSchedulerConfig:
    """Configuration for ProcessScheduler.

    Encapsulates all configuration options in a single object.
    """

    def __init__(
        self,
        computer_limits: dict[str, int] | None = None,
        worker_count: int = 0,
    ):
        """Initialize scheduler configuration.

        :param computer_limits: Dict mapping computer labels to concurrency limits.
                                Computers not listed have unlimited concurrency.
        :param worker_count: Number of workers to spawn at startup
        """
        self.computer_limits = computer_limits or {}
        self.worker_count = worker_count

    @classmethod
    def from_file(cls, config_path: Path) -> 'ProcessSchedulerConfig':
        """Load configuration from file.

        Config file format (scheduler/config.json):
        {
            "workers": {
                "count": 4
            },
            "computers": {
                "localhost": 20,
                "frontier": 50
            }
        }

        Computers not listed have unlimited concurrency.

        :param config_path: Path to config directory (contains scheduler/)
        :return: ProcessSchedulerConfig instance
        """
        config_file = config_path / 'scheduler' / 'config.json'

        if not config_file.exists():
            return cls()

        try:
            with open(config_file) as f:
                data = json.load(f)

            # Worker count (support both 'count' and legacy 'initial_count')
            workers = data.get('workers', {})
            worker_count = workers.get('count', workers.get('initial_count', 0))

            # Computer limits
            computer_limits = data.get('computers', {})

            return cls(
                computer_limits=computer_limits,
                worker_count=worker_count,
            )
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning(f'Failed to load config: {exc}, using defaults')
            return cls()


class ProcessScheduler:
    """Unified scheduler for task distribution and worker management.

    This class handles:
    - Task queue persistence
    - Worker discovery and selection
    - Task distribution
    - Broadcast fanout
    - Per-computer scheduling with concurrency limits

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
        """Initialize the process scheduler.

        :param communicator: Transport abstraction for broker communications
        :param profile_name: AiiDA profile name
        :param working_dir: Working directory for queue files
        :param config: Scheduler configuration
        :param executor: Optional worker executor for lifecycle management
        """
        self._communicator = communicator
        self._profile_name = profile_name
        self._working_dir = Path(working_dir)
        self._config = config or ProcessSchedulerConfig()
        self._executor = executor

        # Task queue for persistence (using ProcessQueue with queue_id='tasks')
        self._task_queue = ProcessQueue('tasks', self._working_dir / 'queue')

        # Worker selection (round-robin)
        self._worker_index = 0

        # Scheduling state
        self._queues: dict[str, ProcessQueue] = {}
        self._running_counts: dict[str, int] = {}
        self._running_pids: dict[str, set[int]] = {}  # Track running PIDs in memory for polling
        self._pid_to_queue_id: dict[int, str] = {}  # Map PID to queue_id for completion handling

        # Load existing ProcessQueues from disk (for restart scenarios)
        self._load_existing_queues()

        # Register callbacks with communicator
        self._communicator.add_task_subscriber(self._on_task_received, 'process_broker')
        self._communicator.add_broadcast_subscriber(self._on_broadcast_received, 'process_broker')

        # Start communicator
        self._communicator.start()

        LOGGER.info(f'ProcessScheduler started for profile: {profile_name}')

    def _load_existing_queues(self) -> None:
        """Load existing ProcessQueues from disk on startup.

        For computers with configured limits, checks if queue directories exist
        on disk and loads them into memory.

        All "running" entries in ProcessQueue are cleaned up on startup since
        they're stale (workers from before restart are gone). Tasks will be
        re-added to ProcessQueue if needed when _requeue_orphaned_tasks runs.
        """
        # Load ProcessQueues for pending tasks
        # Config uses computer_label, but queue dirs use full queue_id format
        for computer_label in self._config.computer_limits:
            queue_id = f'COMPUTER__{computer_label}'
            queue_dir = self._working_dir / 'queue' / queue_id
            if queue_dir.exists():
                # Create queue instance (loads existing files)
                queue = ProcessQueue(queue_id, queue_dir)
                self._queues[queue_id] = queue

                # Clean up stale "running" entries from previous scheduler run
                # These are always stale after restart since old workers are gone
                running_pids = queue.get_running_pids()
                for pid in running_pids:
                    queue.remove_running(pid)
                    LOGGER.debug(f'Cleaned up stale running entry for pid={pid} in {queue_id}')

                if running_pids:
                    LOGGER.info(f'Cleaned up {len(running_pids)} stale running entries for {queue_id}')

                # Count remaining pending entries
                pending_count = sum(1 for _ in queue_dir.glob('proc_*.json'))
                LOGGER.info(f'Loaded queue for {queue_id}: {pending_count} pending')

    def _on_task_received(self, message: dict) -> None:
        """Callback invoked when task message received from communicator.

        This is the entry point for all incoming task messages. Determines
        whether to apply scheduling or handle immediately.

        :param message: Task message dict
        """
        # Check for scheduler query messages
        msg_type = message.get('type')
        if msg_type == 'query_queue_status':
            self._handle_queue_status_query(message)
            return

        # Check if this is a continue_process task requiring scheduling
        task_type = message.get('body', {}).get('task')
        if task_type == 'continue':
            # plumpy sends: {'task': 'continue', 'args': {'pid': 123, ...}}
            pid = message['body']['args']['pid']
            self._schedule_process(pid, message)
            return

        # Non-continue tasks: handle directly
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
        task_id = self._task_queue.enqueue_task(message)

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

        # Strip scheduler-specific metadata from args before sending to worker
        # The queue_id is used by the scheduler for routing but ProcessLauncher doesn't expect it
        worker_message = copy.deepcopy(message)
        if 'body' in worker_message and 'args' in worker_message['body']:
            worker_message['body']['args'].pop('queue_id', None)

        # Round-robin worker selection
        worker = workers[self._worker_index % len(workers)]
        self._worker_index += 1

        # Get worker identifier (worker_id for executor, process_id for discovery)
        worker_id = worker.get('worker_id', worker.get('process_id', 'unknown'))

        # Sanitize reply pipe
        self._sanitize_reply_pipe(worker_message, worker)

        try:
            # Send task via communicator (use worker_message with queue_id stripped)
            self._communicator.task_send(worker['task_pipe'], worker_message, non_blocking=True)

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

    def _get_computer_label_from_queue_id(self, queue_id: str) -> str | None:
        """Extract computer_label from queue_id if it's a computer queue.

        :param queue_id: Queue identifier (e.g., 'COMPUTER__localhost' or 'LOCAL')
        :return: Computer label if queue_id is for a computer, None otherwise
        """
        if queue_id.startswith('COMPUTER__'):
            return queue_id[len('COMPUTER__'):]
        return None

    def _schedule_process(self, pid: int, message: dict) -> None:
        """Apply queue-based scheduling logic.

        Checks concurrency limits and either executes immediately or queues.
        Queues without configured limits have unlimited concurrency.

        :param pid: Process ID
        :param message: Task message
        """
        # Get queue_id from message metadata (set by launch.py/runners.py via plumpy metadata)
        # Format: 'COMPUTER__<label>' for CalcJobs, 'LOCAL' for WorkChains/CalcFunctions
        queue_id = message.get('body', {}).get('args', {}).get('queue_id')

        if queue_id is None:
            # No queue_id (legacy message or error) - execute immediately
            self._handle_task_message(message)
            LOGGER.debug(f'Executed process {pid} (no queue_id)')
            return

        # Extract computer_label from queue_id to check limits
        # Config uses computer_label, queue uses full queue_id
        computer_label = self._get_computer_label_from_queue_id(queue_id)

        if computer_label is None:
            # Not a computer queue (e.g., 'LOCAL') - execute immediately without scheduling
            self._handle_task_message(message)
            LOGGER.debug(f'Executed process {pid} (queue_id={queue_id})')
            return

        # Check if this computer has a limit configured
        limit = self._config.computer_limits.get(computer_label)

        if limit is None:
            # No limit configured - execute immediately (unlimited)
            self._handle_task_message(message)
            LOGGER.debug(f'Executed process {pid} on {queue_id} (unlimited)')
            return

        # Get or create queue for this queue_id (only for limited queues)
        if queue_id not in self._queues:
            queue_dir = self._working_dir / 'queue' / queue_id
            self._queues[queue_id] = ProcessQueue(queue_id, queue_dir)
            LOGGER.info(f'Created queue: {queue_id}')

        # Check limit
        current = self._running_counts.get(queue_id, 0)

        # Store pid -> queue_id mapping for completion handling
        self._pid_to_queue_id[pid] = queue_id

        if current < limit:
            # Under limit - execute immediately
            self._handle_task_message(message)
            # Track running PID in memory for completion polling
            if queue_id not in self._running_pids:
                self._running_pids[queue_id] = set()
            self._running_pids[queue_id].add(pid)
            self._running_counts[queue_id] = current + 1
            LOGGER.info(f'Executed process {pid} on {queue_id} ({current + 1}/{limit})')
        else:
            # At limit - enqueue
            self._queues[queue_id].enqueue({'pid': pid, 'message': message})
            LOGGER.info(f'Queued process {pid} for {queue_id} (at limit {current}/{limit})')

    def on_completion(self, pid: int, queue_id: str | None = None) -> None:
        """Handle process completion (decrement count, try submit next).

        This method should be called externally when a process completes.

        :param pid: Process ID that completed
        :param queue_id: Optional queue identifier (if known from context)
        """
        # Get queue_id from parameter or from our mapping (set when process was scheduled)
        if queue_id is None:
            queue_id = self._pid_to_queue_id.pop(pid, None)
        else:
            # Clear from mapping if it was there
            self._pid_to_queue_id.pop(pid, None)

        if queue_id is None:
            # Process wasn't tracked (unlimited queue or LOCAL)
            return

        # Extract computer_label to check if this queue has a limit
        computer_label = self._get_computer_label_from_queue_id(queue_id)
        if computer_label is None or computer_label not in self._config.computer_limits:
            return

        # Remove from in-memory running set
        if queue_id in self._running_pids:
            self._running_pids[queue_id].discard(pid)

        # Remove from queue if present (for processes that were queued then executed)
        queue = self._queues.get(queue_id)
        if queue:
            queue.remove_running(pid)

        # Decrement count
        current = self._running_counts.get(queue_id, 0)
        if current > 0:
            self._running_counts[queue_id] = current - 1
            LOGGER.info(f'Process {pid} completed on {queue_id}. New count: {current - 1}')

        # Try to submit next queued process
        self._try_submit_next(queue_id)

    def _try_submit_next(self, queue_id: str) -> None:
        """Try to submit queued processes up to the limit.

        :param queue_id: Queue identifier
        """
        queue = self._queues.get(queue_id)
        if not queue:
            LOGGER.debug(f'No queue for {queue_id}')
            return

        # Extract computer_label to get limit from config
        computer_label = self._get_computer_label_from_queue_id(queue_id)
        if computer_label is None:
            LOGGER.debug(f'Not a computer queue: {queue_id}')
            return

        limit = self._config.computer_limits.get(computer_label)
        if limit is None:
            # No limit - shouldn't have a queue, but handle gracefully
            LOGGER.debug(f'No limit for {queue_id}')
            return

        current = self._running_counts.get(queue_id, 0)

        if current >= limit:
            LOGGER.debug(f'Queue {queue_id} still at limit ({current}/{limit})')
            return

        # Submit processes until we reach the limit or run out of pending
        while current < limit:
            # Get next pending process
            pending = queue.get_pending()
            if not pending:
                LOGGER.debug(f'No pending process for {queue_id}')
                break

            # Submit via broker
            try:
                pid = pending['message']['pid']
                message = pending['message']['message']
                self._handle_task_message(message)

                # Mark as running
                queue.mark_running(pending['id'])
                current += 1
                self._running_counts[queue_id] = current

                LOGGER.info(f'Submitted queued process {pid} to {queue_id} ({current}/{limit})')

            except Exception as exc:
                LOGGER.error(f'Failed to submit process: {exc}')
                break

    def run_maintenance(self) -> None:
        """Run periodic maintenance tasks.

        This should be called periodically (e.g., every second) to perform:
        - Cleanup of old completed tasks
        - Cleanup of dead worker entries (or executor health checks)
        - Requeue tasks assigned to dead workers
        - Poll for missed completions
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

            # Process pending tasks that were never assigned (e.g., from before restart)
            self._process_pending_tasks()

            # Clean up tasks for processes that have completed
            self._cleanup_completed_tasks()

            # Poll for missed completions (only for computers with limits)
            if self._queues:
                self._poll_running_processes()

            # Try to schedule pending processes from ProcessQueues
            for queue_id in self._queues:
                self._try_submit_next(queue_id)

        except Exception as exc:
            LOGGER.exception(f'Error in maintenance: {exc}')

    def _requeue_orphaned_tasks(self) -> None:
        """Requeue tasks assigned to dead workers.

        For CalcJob tasks with computer limits, decrements the running count
        and re-routes through _schedule_process to respect limits.
        """
        all_tasks = self._task_queue.get_all_tasks()

        # Get worker IDs from executor or discovery
        if self._executor:
            workers = self._executor.get_workers()
            worker_ids = {w['worker_id'] for w in workers}
        else:
            workers = discovery.discover_workers(self._profile_name, check_alive=True)
            worker_ids = {w['process_id'] for w in workers}

        for task_data in all_tasks:
            if task_data.get('status') == 'assigned':
                assigned_to = task_data.get('assigned_to')
                if assigned_to and assigned_to not in worker_ids:
                    task_id = task_data['id']
                    message = task_data['message']

                    # Check if this is a process with a queue that has limits
                    queue_id = message.get('body', {}).get('args', {}).get('queue_id')
                    pid = message.get('body', {}).get('args', {}).get('pid')

                    # Extract computer_label from queue_id to check limits
                    computer_label = self._get_computer_label_from_queue_id(queue_id) if queue_id else None

                    if computer_label and computer_label in self._config.computer_limits:
                        # CalcJob with limit - remove from task queue and re-schedule
                        # This decrements the effective running count and respects limits
                        LOGGER.info(f'Requeueing CalcJob task {task_id} (pid={pid}) through scheduler')
                        self._task_queue.mark_completed(task_id)
                        # Remove from pid_to_queue_id tracking (will be re-added if scheduled)
                        self._pid_to_queue_id.pop(pid, None)
                        # Also clean up any stale ProcessQueue entry
                        queue = self._queues.get(queue_id)
                        if queue:
                            queue.remove_running(pid)
                        # Re-route through scheduling logic
                        self._schedule_process(pid, message)
                    else:
                        # WorkChain or unlimited - redistribute directly
                        LOGGER.info(f'Requeueing task {task_id} from dead worker {assigned_to}')
                        self._task_queue.reset_to_pending(task_id)
                        self._distribute_task(task_id, message)

    def _process_pending_tasks(self) -> None:
        """Process pending tasks that were never assigned.

        This handles tasks that were queued before a scheduler restart but never
        got assigned to a worker. Routes CalcJobs through _schedule_process for
        proper throttling.
        """
        pending_tasks = self._task_queue.get_pending_tasks()

        for task_data in pending_tasks:
            # Only process tasks that have never been assigned
            if task_data.get('assigned_to') is not None:
                continue

            task_id = task_data['id']
            message = task_data['message']

            # Check if this is a process with a queue that has limits
            queue_id = message.get('body', {}).get('args', {}).get('queue_id')
            pid = message.get('body', {}).get('args', {}).get('pid')

            # Extract computer_label from queue_id to check limits
            computer_label = self._get_computer_label_from_queue_id(queue_id) if queue_id else None

            if computer_label and computer_label in self._config.computer_limits:
                # CalcJob with limit - route through scheduler
                # First remove from task queue (will be tracked in ProcessQueue)
                self._task_queue.mark_completed(task_id)
                LOGGER.info(f'Processing pending CalcJob task (pid={pid}) through scheduler')
                self._schedule_process(pid, message)
            else:
                # WorkChain or unlimited - distribute directly to worker
                LOGGER.info(f'Distributing pending task {task_id} (pid={pid})')
                self._distribute_task(task_id, message)

    def _cleanup_completed_tasks(self) -> None:
        """Clean up tasks for processes that have reached terminal state.

        Queries the database to check if processes referenced by tasks have
        completed (finished/failed/killed/excepted). If so, removes the task
        from the queue since it's no longer needed.
        """
        from aiida.orm import ProcessNode, QueryBuilder

        all_tasks = self._task_queue.get_all_tasks()
        if not all_tasks:
            return

        # Extract PIDs from tasks
        task_pids = {}
        for task_data in all_tasks:
            task_message = task_data.get('message', {})
            task_body = task_message.get('body', {})
            task_pid = task_body.get('args', {}).get('pid')
            if task_pid is not None:
                task_pids[int(task_pid)] = task_data['id']

        if not task_pids:
            return

        # Query database for terminal processes
        qb = QueryBuilder()
        qb.append(
            ProcessNode,
            filters={
                'id': {'in': list(task_pids.keys())},
                'attributes.process_state': {'in': ['finished', 'failed', 'killed', 'excepted']},
            },
            project=['id'],
        )

        # Remove tasks for completed processes
        for (pid,) in qb.all():
            task_id = task_pids.get(pid)
            if task_id:
                LOGGER.debug(f'Removing task {task_id} for completed process {pid}')
                self._task_queue.mark_completed(task_id)

    def _poll_running_processes(self) -> None:
        """Poll database for missed completions and clean up stale queue entries.

        Queries the database to find processes that are marked as completed
        in the database but were not properly handled by the scheduler.
        Also removes pending queue entries for already-completed processes.
        """
        from aiida.orm import ProcessNode, QueryBuilder

        # Process each queue with limits
        for computer_label in self._config.computer_limits:
            queue_id = f'COMPUTER__{computer_label}'
            queue = self._queues.get(queue_id)
            if not queue:
                continue

            # Get PIDs from in-memory set (immediately executed - running)
            memory_pids = self._running_pids.get(queue_id, set())

            # Get PIDs from queue files - both running and pending
            running_pids = set(queue.get_running_pids())
            all_queue_pids = set(queue.get_all_pids())
            pending_pids = all_queue_pids - running_pids

            # Check running processes for missed completions
            all_running_pids = list(memory_pids | running_pids)
            if all_running_pids:
                qb = QueryBuilder()
                qb.append(ProcessNode, filters={'id': {'in': all_running_pids}}, project=['id', 'attributes.process_state'])

                for pid, state in qb.all():
                    if state in ['finished', 'failed', 'killed', 'excepted']:
                        LOGGER.warning(f'Missed completion event for {pid}, handling now')
                        self.on_completion(pid, queue_id)

            # Check pending processes - remove stale entries (no completion handling needed)
            if pending_pids:
                qb = QueryBuilder()
                qb.append(ProcessNode, filters={'id': {'in': list(pending_pids)}}, project=['id', 'attributes.process_state'])

                for pid, state in qb.all():
                    if state in ['finished', 'failed', 'killed', 'excepted']:
                        LOGGER.debug(f'Removing stale queue entry for completed process {pid}')
                        queue.remove_by_pid(pid)

            # Synchronize running count with actual tracked PIDs
            # This handles cases where count got out of sync (e.g., process counted but not tracked)
            # Re-fetch after processing completions above
            current_memory_pids = self._running_pids.get(queue_id, set())
            current_running_pids = set(queue.get_running_pids())
            actual_running = len(current_memory_pids | current_running_pids)
            current_count = self._running_counts.get(queue_id, 0)
            if current_count > actual_running:
                LOGGER.warning(
                    f'Running count mismatch for {queue_id}: count={current_count}, '
                    f'actual tracked={actual_running}. Correcting count.'
                )
                self._running_counts[queue_id] = actual_running

    def get_status(self) -> dict:
        """Get scheduler status for CLI display.

        :return: Status dict with worker count and computer limits
        """
        # Get worker count from executor or discovery
        if self._executor:
            worker_count = self._executor.get_worker_count()
        else:
            worker_count = len(discovery.discover_workers(self._profile_name, check_alive=True))

        status = {
            'worker_count': worker_count,
        }

        # Only include computers with limits
        if self._config.computer_limits:
            computers = {}
            for computer_label, limit in self._config.computer_limits.items():
                queue_id = f'COMPUTER__{computer_label}'
                running = self._running_counts.get(queue_id, 0)
                queue = self._queues.get(queue_id)

                if queue:
                    queued = len(list(queue.queue_dir.glob('proc_*.json')))
                else:
                    queued = 0

                computers[computer_label] = {'running': running, 'limit': limit, 'queued': queued}

            status['computers'] = computers

        return status

    def get_queue_status(self, pids: list[int]) -> dict[int, str | None]:
        """Get scheduler queue status for given PIDs.

        :param pids: List of process IDs to query
        :return: Dict mapping PID to status:
            - 'queued' - waiting in queue (pending)
            - 'running' - assigned to a worker
            - None - not tracked by scheduler
        """
        result: dict[int, str | None] = {pid: None for pid in pids}
        pids_set = set(pids)

        # Check task queue (general task queue for all processes)
        for task_file in self._task_queue.queue_dir.glob('task_*.json'):
            try:
                with open(task_file) as f:
                    task_data = json.load(f)

                # Extract PID from the task message
                # Format: {'body': {'task': 'continue', 'args': {'pid': 123}}}
                task_message = task_data.get('message', {})
                task_body = task_message.get('body', {})
                task_pid = task_body.get('args', {}).get('pid')

                # Convert to int (PIDs may be stored as strings)
                if task_pid is not None:
                    task_pid = int(task_pid)

                if task_pid and task_pid in pids_set:
                    task_status = task_data.get('status')
                    if task_status == 'pending':
                        result[task_pid] = 'queued'
                    elif task_status == 'assigned':
                        result[task_pid] = 'running'

            except (json.JSONDecodeError, OSError, KeyError, ValueError, TypeError):
                pass

        # Check ProcessQueues (for limited queues)
        for queue_id, queue in self._queues.items():
            for task_file in queue.queue_dir.glob('proc_*.json'):
                try:
                    with open(task_file) as f:
                        task_data = json.load(f)

                    task_pid = task_data['message']['pid']
                    if task_pid in pids_set:
                        status = task_data.get('status', 'pending')
                        if status == 'pending':
                            result[task_pid] = 'queued'
                        elif status == 'running':
                            result[task_pid] = 'running'

                except (json.JSONDecodeError, OSError, KeyError):
                    pass

        return result

    def _handle_queue_status_query(self, message: dict) -> None:
        """Handle IPC query for queue status.

        Receives query message and sends response back via reply pipe.

        Message format:
            Request: {'type': 'query_queue_status', 'pids': [1, 2, 3], 'reply_pipe': '/path'}
            Response: {'type': 'queue_status_response', 'status': {1: 'queued', 2: None, ...}}

        :param message: Query message dict
        """
        from aiida.communication.namedpipe import messages, utils

        reply_pipe = message.get('reply_pipe')
        if not reply_pipe:
            LOGGER.warning('Queue status query missing reply_pipe, ignoring')
            return

        pids = message.get('pids', [])

        # Get queue status
        status = self.get_queue_status(pids)

        # Build response
        response = {
            'type': 'queue_status_response',
            'status': status,
        }

        # Send response to reply pipe
        try:
            data = messages.serialize(response)
            utils.write_to_pipe(reply_pipe, data, non_blocking=False)
            LOGGER.debug(f'Sent queue status response for {len(pids)} PIDs')
        except (BrokenPipeError, FileNotFoundError, OSError) as exc:
            LOGGER.warning(f'Failed to send queue status response: {exc}')

    def close(self) -> None:
        """Close the broker and cleanup resources."""
        self._communicator.close()
        LOGGER.info('ProcessScheduler stopped')

    def is_closed(self) -> bool:
        """Check if broker is closed.

        :return: True if closed, False otherwise
        """
        return self._communicator.is_closed()
