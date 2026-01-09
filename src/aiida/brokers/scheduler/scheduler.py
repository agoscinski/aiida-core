###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Scheduler daemon for per-computer process queue management."""

from __future__ import annotations

import atexit
import json
import logging
import os
import selectors
import threading
import typing as t
from pathlib import Path

from aiida.brokers.namedpipe import messages, utils
from aiida.brokers.namedpipe.message_broker import MessageBroker

from . import discovery
from .queue import ComputerQueue

__all__ = ('PipeScheduler',)

LOGGER = logging.getLogger(__name__)


class PipeScheduler:
    """Top-level scheduler daemon that owns MessageBroker.

    Intercepts broker messages and enforces per-computer concurrency limits.
    The scheduler reads from the broker_tasks pipe and forwards approved
    tasks to the internal broker instance.
    """

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
    ):
        """Initialize the scheduler daemon.

        :param profile_name: The AiiDA profile name.
        :param config_path: Path to the AiiDA config directory.
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)

        # Scheduler ID
        self._scheduler_id = 'scheduler'

        # Scheduler intercepts the broker_tasks pipe (in broker pipes directory)
        broker_pipes_dir = utils.get_broker_pipes_dir(profile_name)
        self._task_pipe_path = broker_pipes_dir / 'broker_tasks'
        self._task_fd: int | None = None

        # Per-computer queues
        self._queue_dir = self._config_path / 'scheduler'
        self._queue_dir.mkdir(parents=True, exist_ok=True)
        self._queues: dict[str, ComputerQueue] = {}

        # Computer limits and running counts
        self._computer_limits = self._load_computer_limits()
        self._running_counts: dict[str, int] = {}

        # Own MessageBroker instance (uses working directory in scheduler namespace)
        # Scheduler owns the pipes, so MessageBroker should not create them
        broker_working_dir = self._config_path / 'scheduler' / 'broker'
        self._broker = MessageBroker(
            profile_name=profile_name,
            working_dir=broker_working_dir,
            create_pipes=False,  # Scheduler owns the pipes!
        )
        # Scheduler owns broker_tasks pipe, broker handles distribution

        # Selector for event-driven I/O
        self._selector = selectors.DefaultSelector()
        self._selector_thread: threading.Thread | None = None
        self._selector_running = False
        self._selector_lock = threading.Lock()

        # Running flag
        self._closed = False

        # Initialize scheduler pipes
        self._init_pipes()

        # Start selector thread
        self._start_selector_thread()

        # Register scheduler in discovery
        discovery.register_scheduler(
            self._profile_name,
            scheduler_id=self._scheduler_id,
            task_pipe=str(self._task_pipe_path),
        )

        # Register cleanup
        atexit.register(self.close)

        LOGGER.info(f'PipeScheduler started for profile: {profile_name}')

    def _init_pipes(self) -> None:
        """Initialize scheduler pipes."""
        # Task pipe (intercepts broker_tasks)
        utils.create_pipe(self._task_pipe_path)
        self._task_fd = utils.open_pipe_read(self._task_pipe_path, non_blocking=True)
        self._selector.register(self._task_fd, selectors.EVENT_READ, self._handle_task)

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
        """Intercept task message - apply scheduling logic.

        :param fd: File descriptor of the task pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd)
            if message is None:
                return

            # Check if this is a continue_process task
            task_type = message.get('body', {}).get('task')
            if task_type == 'continue':
                pid = message['body']['process_id']
                self._schedule_process(pid, message)
            else:
                # Non-continue tasks go directly to broker
                self._broker.handle_task_message(message)

        except Exception as exc:
            LOGGER.exception(f'Error handling task: {exc}')

    def _schedule_process(self, pid: int, message: dict) -> None:
        """Apply per-computer scheduling.

        :param pid: Process ID to schedule.
        :param message: Original task message.
        """
        # Get computer label from node
        from aiida.orm import load_node

        try:
            node = load_node(pid)
            computer_label = self._get_computer_label(node)
        except Exception as exc:
            LOGGER.error(f'Failed to load node {pid}: {exc}')
            # Execute anyway - don't block processes if we can't determine computer
            self._broker.handle_task_message(message)
            return

        # Get or create queue for this computer
        if computer_label not in self._queues:
            self._queues[computer_label] = ComputerQueue(
                computer_label,
                self._queue_dir / computer_label
            )
            LOGGER.info(f'Created queue for computer: {computer_label}')

        # Check limit
        current = self._running_counts.get(computer_label, 0)
        limit = self._computer_limits.get(computer_label, 10)

        if current < limit:
            # Under limit - execute immediately
            self._broker.handle_task_message(message)
            self._running_counts[computer_label] = current + 1
            LOGGER.info(f'Executed process {pid} on {computer_label} ({current + 1}/{limit})')
        else:
            # At limit - enqueue
            self._queues[computer_label].enqueue({'pid': pid, 'message': message})
            LOGGER.info(f'Queued process {pid} for {computer_label} (at limit {current}/{limit})')

    def _on_completion(self, pid: int) -> None:
        """Handle process completion - decrement and submit next.

        :param pid: Process ID that completed.
        """
        from aiida.orm import load_node

        # Determine computer label
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

        :param computer_label: The computer label.
        """
        queue = self._queues.get(computer_label)
        if not queue:
            return

        current = self._running_counts.get(computer_label, 0)
        limit = self._computer_limits.get(computer_label, 10)

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
            self._broker.handle_task_message(message)

            # Mark as running
            queue.mark_running(pending['id'])
            self._running_counts[computer_label] = current + 1

            LOGGER.info(f'Submitted queued process {pid} to {computer_label} ({current + 1}/{limit})')

        except Exception as exc:
            LOGGER.error(f'Failed to submit process: {exc}')

    def _get_computer_label(self, node) -> str:
        """Get computer label from node.

        :param node: ProcessNode instance.
        :return: Computer label string.
        """
        # CalcJobNode has .computer attribute
        if hasattr(node, 'computer') and node.computer is not None:
            return node.computer.label

        # WorkChains/WorkFunctions don't have a computer - use localhost
        return 'localhost'

    def _load_computer_limits(self) -> dict[str, int]:
        """Load computer limits from config file.

        :return: Dictionary mapping computer labels to limits.
        """
        limits = {'localhost': 10}

        config_file = self._queue_dir / 'computer_limits.json'
        if config_file.exists():
            try:
                with open(config_file) as f:
                    limits.update(json.load(f))
                    LOGGER.info(f'Loaded computer limits: {limits}')
            except (json.JSONDecodeError, OSError) as exc:
                LOGGER.warning(f'Failed to load limits: {exc}')

        return limits

    def run_maintenance(self) -> None:
        """Run periodic maintenance tasks."""
        try:
            # Poll for missed completions
            self._poll_running_processes()

            # Run broker maintenance
            self._broker.run_maintenance()

        except Exception as exc:
            LOGGER.exception(f'Error in maintenance: {exc}')

    def _poll_running_processes(self) -> None:
        """Poll database for missed completions."""
        from aiida.orm import ProcessNode, QueryBuilder

        for computer_label, queue in self._queues.items():
            pids = queue.get_running_pids()
            if not pids:
                continue

            # Query database for these processes
            qb = QueryBuilder()
            qb.append(
                ProcessNode,
                filters={'id': {'in': pids}},
                project=['id', 'attributes.process_state']
            )

            for pid, state in qb.all():
                if state in ['finished', 'failed', 'killed', 'excepted']:
                    LOGGER.warning(f'Missed completion event for {pid}, handling now')
                    self._on_completion(pid)

    def get_status(self) -> dict[str, dict[str, int]]:
        """Get scheduler status for CLI display.

        :return: Dict with computer labels, limits, running counts, queued counts.
        """
        status = {}
        all_computers = set(list(self._queues.keys()) + list(self._computer_limits.keys()))

        for computer_label in all_computers:
            running = self._running_counts.get(computer_label, 0)
            limit = self._computer_limits.get(computer_label, 10)
            queue = self._queues.get(computer_label)

            # Count queued processes
            if queue:
                queued = len(list(queue.queue_dir.glob('proc_*.json')))
            else:
                queued = 0

            status[computer_label] = {
                'running': running,
                'limit': limit,
                'queued': queued
            }

        return status

    def close(self) -> None:
        """Close the scheduler and clean up resources."""
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

        # Close file descriptor
        if self._task_fd is not None:
            try:
                os.close(self._task_fd)
            except OSError:
                pass

        # Close broker
        self._broker.close()

        # Clean up scheduler's task pipe (broker cleans up its own pipes)
        utils.cleanup_pipe(self._task_pipe_path)

        # Unregister from discovery
        try:
            discovery.unregister_scheduler(self._profile_name, self._scheduler_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering scheduler: {exc}')

        LOGGER.info('PipeScheduler stopped')

    def is_closed(self) -> bool:
        """Check if scheduler is closed.

        :return: True if closed, False otherwise.
        """
        return self._closed
