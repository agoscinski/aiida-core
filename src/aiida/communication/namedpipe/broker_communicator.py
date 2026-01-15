###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Named pipe-based implementation of BrokerCommunicator protocol."""

from __future__ import annotations

import functools
import logging
import os
import selectors
import threading
import typing as t
import uuid
from pathlib import Path

from aiida.orm.utils import serialize

from . import discovery, messages, utils

__all__ = ('PipeBrokerCommunicator',)

LOGGER = logging.getLogger(__name__)


class PipeBrokerCommunicator:
    """Named pipe-based implementation of BrokerCommunicator.

    This class owns all pipe operations:
    - Creates and manages broker's task and broadcast pipes
    - Runs selector thread for event-driven I/O
    - Handles message serialization/deserialization
    - Manages file descriptor lifecycle
    - Invokes registered callbacks when messages arrive

    This is pure transport layer - no business logic.
    """

    def __init__(
        self,
        profile_name: str,
        broker_id: str = 'broker',
    ):
        """Initialize pipe-based broker communicator.

        :param profile_name: AiiDA profile name
        :param broker_id: Broker identifier for pipe naming
        """
        self._profile_name = profile_name
        self._broker_id = broker_id
        self._closed = False

        # Encoder/decoder for YAML serialization (must match PipeCommunicator)
        self._encoder = functools.partial(serialize.serialize, encoding='utf-8')
        # Decoder wrapper: deserialize_unsafe expects string, but we receive bytes
        self._decoder = lambda data: serialize.deserialize_unsafe(data.decode('utf-8'))

        # Pipe paths (in broker pipes directory)
        broker_pipes_dir = utils.get_broker_pipes_dir(profile_name)
        self._task_pipe_path = broker_pipes_dir / 'broker_tasks'
        self._broadcast_pipe_path = broker_pipes_dir / 'broker_broadcast'

        # File descriptors
        self._task_fd: int | None = None
        self._broadcast_fd: int | None = None

        # Subscribers (callbacks)
        self._task_subscribers: dict[str, t.Callable[[dict], None]] = {}
        self._broadcast_subscribers: dict[str, t.Callable[[dict], None]] = {}

        # Selector for event-driven I/O
        self._selector = selectors.DefaultSelector()
        self._selector_thread: threading.Thread | None = None
        self._selector_running = False
        self._selector_lock = threading.Lock()

    def start(self) -> None:
        """Initialize pipes and start selector thread."""
        if self._closed:
            raise RuntimeError('Cannot start closed communicator')

        # Create task pipe
        utils.create_pipe(self._task_pipe_path)
        self._task_fd = utils.open_pipe_read(self._task_pipe_path, non_blocking=True)
        self._selector.register(self._task_fd, selectors.EVENT_READ, self._handle_task)

        # Create broadcast pipe
        utils.create_pipe(self._broadcast_pipe_path)
        self._broadcast_fd = utils.open_pipe_read(self._broadcast_pipe_path, non_blocking=True)
        self._selector.register(self._broadcast_fd, selectors.EVENT_READ, self._handle_broadcast)

        # Start selector thread
        self._selector_running = True
        self._selector_thread = threading.Thread(target=self._selector_loop, daemon=True)
        self._selector_thread.start()

        LOGGER.info(f'PipeBrokerCommunicator started for profile: {self._profile_name}')

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

        Reads message from pipe and invokes all registered task subscribers.

        :param fd: File descriptor of the task pipe
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Fire all task subscribers
            for subscriber in list(self._task_subscribers.values()):
                try:
                    subscriber(message)
                except Exception as exc:
                    LOGGER.exception(f'Error in task subscriber: {exc}')

        except Exception as exc:
            LOGGER.exception(f'Error handling task: {exc}')

    def _handle_broadcast(self, fd: int) -> None:
        """Handle incoming broadcast message from pipe.

        Reads message from pipe and invokes all registered broadcast subscribers.

        :param fd: File descriptor of the broadcast pipe
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Fire all broadcast subscribers
            for subscriber in list(self._broadcast_subscribers.values()):
                try:
                    subscriber(message)
                except Exception as exc:
                    LOGGER.exception(f'Error in broadcast subscriber: {exc}')

        except Exception as exc:
            LOGGER.exception(f'Error handling broadcast: {exc}')

    def add_task_subscriber(
        self,
        callback: t.Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming task messages.

        :param callback: Function to call with task message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier
        """
        if identifier is None:
            identifier = str(uuid.uuid4())
        self._task_subscribers[identifier] = callback
        return identifier

    def add_broadcast_subscriber(
        self,
        callback: t.Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming broadcast messages.

        :param callback: Function to call with broadcast message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier
        """
        if identifier is None:
            identifier = str(uuid.uuid4())
        self._broadcast_subscribers[identifier] = callback
        return identifier

    def remove_task_subscriber(self, identifier: str) -> None:
        """Remove a task subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        self._task_subscribers.pop(identifier, None)

    def remove_broadcast_subscriber(self, identifier: str) -> None:
        """Remove a broadcast subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        self._broadcast_subscribers.pop(identifier, None)

    def task_send(self, message: dict) -> None:
        """Send a task message to a specific worker.

        Extracts target worker from message['_routing']['target_worker'],
        looks up the worker's pipe path via discovery, and sends the message.

        :param message: Task message dict with '_routing' header
        :raises KeyError: If routing information is missing
        :raises ValueError: If target worker not found in discovery
        :raises BrokenPipeError: If worker pipe unavailable
        :raises OSError: If send fails
        """
        # Extract routing information
        routing = message.get('_routing', {})
        target_worker = routing.get('target_worker')
        if not target_worker:
            raise KeyError("Message missing '_routing.target_worker'")

        # Look up worker in discovery
        workers = discovery.discover_workers(self._profile_name, check_alive=False)
        worker_info = None
        for worker in workers:
            if worker.get('process_id') == target_worker or worker.get('worker_id') == target_worker:
                worker_info = worker
                break

        if worker_info is None:
            raise ValueError(f'Worker not found in discovery: {target_worker}')

        # Send to worker's task pipe
        task_pipe = worker_info.get('task_pipe')
        if not task_pipe:
            raise ValueError(f'Worker {target_worker} has no task_pipe')

        data = messages.serialize(message, encoder=self._encoder)
        utils.write_to_pipe(task_pipe, data, non_blocking=True)

    def broadcast_send(self, message: dict) -> int:
        """Send broadcast message to all registered workers.

        Discovers all active workers and sends the message to each.

        :param message: Broadcast message dict to send
        :return: Number of successful sends
        """
        # Discover all workers
        workers = discovery.discover_workers(self._profile_name, check_alive=True)

        if not workers:
            LOGGER.debug('No workers found for broadcast')
            return 0

        data = messages.serialize(message, encoder=self._encoder)
        success_count = 0

        for worker in workers:
            broadcast_pipe = worker.get('broadcast_pipe')
            if not broadcast_pipe:
                continue

            try:
                utils.write_to_pipe(broadcast_pipe, data, non_blocking=True)
                success_count += 1
            except (BrokenPipeError, FileNotFoundError, OSError) as exc:
                worker_id = worker.get('process_id', 'unknown')
                LOGGER.debug(f'Worker {worker_id} broadcast pipe unavailable ({type(exc).__name__})')

        return success_count

    def get_broker_pipes(self) -> dict[str, str]:
        """Get broker pipe paths for discovery registration.

        :return: Dict with 'task_pipe' and 'broadcast_pipe' keys
        """
        return {
            'task_pipe': str(self._task_pipe_path),
            'broadcast_pipe': str(self._broadcast_pipe_path),
        }

    def register_in_discovery(self) -> None:
        """Register this communicator in the discovery system.

        Registers the broker's pipe paths so workers can discover how to connect.
        """
        discovery.register_broker(
            profile_name=self._profile_name,
            broker_id=self._broker_id,
            task_pipe=str(self._task_pipe_path),
            broadcast_pipe=str(self._broadcast_pipe_path),
        )
        LOGGER.info(f'Registered broker {self._broker_id} in discovery')

    def unregister_from_discovery(self) -> None:
        """Unregister this communicator from the discovery system.

        Cleans up the broker's discovery entry during shutdown.
        """
        discovery.unregister_broker(
            profile_name=self._profile_name,
            broker_id=self._broker_id,
        )
        LOGGER.info(f'Unregistered broker {self._broker_id} from discovery')

    def close(self) -> None:
        """Close the communicator and cleanup resources."""
        if self._closed:
            return

        self._closed = True

        # Stop selector thread
        self._selector_running = False
        if self._selector_thread and self._selector_thread.is_alive():
            self._selector_thread.join(timeout=1.0)

        # Close selector
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

        LOGGER.info('PipeBrokerCommunicator stopped')

    def is_closed(self) -> bool:
        """Check if communicator is closed.

        :return: True if closed, False otherwise
        """
        return self._closed
