"""Implementation of the message broker interface using named pipes."""

from __future__ import annotations

import functools
import typing as t
from typing import Callable

from .broker_communicator import PipeBrokerCommunicator
from .communicator import PipeCommunicator
from .discovery import discover_broker, get_discovery_dir

if t.TYPE_CHECKING:
    from pathlib import Path

    from aiida.manage.configuration.profile import Profile

__all__ = ('PipeBroker',)


class PipeBroker:
    """Implementation of the message broker interface using named pipes."""

    def __init__(self, profile: 'Profile') -> None:
        """Construct a new instance.

        :param profile: The profile.
        """
        self._profile = profile
        self._communicator: PipeCommunicator | None = None
        self._broker_communicator: PipeBrokerCommunicator | None = None

    def __str__(self):
        broker = discover_broker(self._profile.name)
        if broker:
            return f'NamedPipe Broker @ {broker["task_pipe"]}'
        return 'NamedPipe Broker <Not running>'

    def close(self):
        """Close the broker and cleanup resources."""
        if self._communicator is not None:
            self._communicator.close()
            self._communicator = None
        if self._broker_communicator is not None:
            self._broker_communicator.close()
            self._broker_communicator = None

    def iterate_tasks(self):
        """Return an iterator over the tasks in the launch queue.

        Note: For named pipe implementation, we don't have direct access to the queue.
        This would require additional broker API.
        """
        # TODO: Implement task iteration via broker API
        return iter([])

    def get_communicator(self) -> PipeCommunicator:
        """Get or create the communicator instance.

        :return: PipeCommunicator instance.
        :raises ConnectionError: If broker is not running.
        """
        if self._communicator is None:
            self._communicator = self._create_communicator()
            # Verify broker is running
            self._check_broker()

        return self._communicator

    def _create_communicator(self) -> PipeCommunicator:
        """Create a new PipeCommunicator instance.

        :return: PipeCommunicator instance.
        """
        import os

        from aiida.orm.utils import serialize

        # Read worker_id from environment if set (for executor-managed workers)
        worker_id = os.environ.get('AIIDA_WORKER_ID')

        return PipeCommunicator(
            profile_name=self._profile.name,
            config_path=self._get_config_path(),
            worker_id=worker_id,
            encoder=functools.partial(serialize.serialize, encoding='utf-8'),
            # Decoder wrapper: deserialize_unsafe expects string, but we receive bytes
            decoder=lambda data: serialize.deserialize_unsafe(data.decode('utf-8')),
        )

    def _get_config_path(self) -> 'Path':
        """Get the config path for the profile.

        :return: Path to the profile config directory.
        """
        from pathlib import Path

        from aiida.manage.configuration import get_config

        config = get_config()
        # Profile-specific config directory
        return Path(config.dirpath) / 'profiles' / self._profile.name

    def _check_broker(self) -> None:
        """Check if the broker is running.

        :raises ConnectionError: If broker is not running.
        """
        broker = discover_broker(self._profile.name)
        if broker is None:
            raise ConnectionError(
                'Named pipe broker is not running. '
                'Please start it with: verdi scheduler start'
            )

    def is_broker_running(self) -> bool:
        """Check if the broker is running.

        :return: True if broker is running, False otherwise.
        """
        try:
            broker = discover_broker(self._profile.name)
            return broker is not None
        except Exception:
            return False

    # Implementation of BrokerCommunicator methods (delegate to _broker_communicator)

    def start(self) -> None:
        """Start the broker and initialize transport resources."""
        if self._broker_communicator is None:
            self._broker_communicator = PipeBrokerCommunicator(
                profile_name=self._profile.name,
                broker_id='broker'
            )
            self._broker_communicator.start()

    def is_closed(self) -> bool:
        """Check if broker is closed.

        :return: True if closed, False otherwise.
        """
        if self._broker_communicator is None:
            return True
        return self._broker_communicator.is_closed()

    def add_task_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming task messages.

        :param callback: Function to call with task message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier
        """
        self._ensure_broker_communicator()
        return self._broker_communicator.add_task_subscriber(callback, identifier)

    def add_broadcast_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming broadcast messages.

        :param callback: Function to call with broadcast message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier
        """
        self._ensure_broker_communicator()
        return self._broker_communicator.add_broadcast_subscriber(callback, identifier)

    def remove_task_subscriber(self, identifier: str) -> None:
        """Remove a task subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        if self._broker_communicator:
            self._broker_communicator.remove_task_subscriber(identifier)

    def remove_broadcast_subscriber(self, identifier: str) -> None:
        """Remove a broadcast subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        if self._broker_communicator:
            self._broker_communicator.remove_broadcast_subscriber(identifier)

    def task_send(
        self,
        worker_pipe: str,
        message: dict,
        non_blocking: bool = True,
    ) -> None:
        """Send a task message to a specific worker.

        :param worker_pipe: Worker's task pipe path
        :param message: Task message dict to send
        :param non_blocking: Use non-blocking write
        """
        self._ensure_broker_communicator()
        self._broker_communicator.task_send(worker_pipe, message, non_blocking)

    def broadcast_send(
        self,
        worker_pipes: list[str],
        message: dict,
        non_blocking: bool = True,
    ) -> int:
        """Send broadcast message to multiple workers.

        :param worker_pipes: List of worker broadcast pipe paths
        :param message: Broadcast message dict to send
        :param non_blocking: Use non-blocking write
        :return: Number of successful sends
        """
        self._ensure_broker_communicator()
        return self._broker_communicator.broadcast_send(worker_pipes, message, non_blocking)

    def get_broker_pipes(self) -> dict[str, str]:
        """Get broker pipe paths for discovery registration.

        :return: Dict with 'task_pipe' and 'broadcast_pipe' keys
        """
        self._ensure_broker_communicator()
        return self._broker_communicator.get_broker_pipes()

    def _ensure_broker_communicator(self) -> None:
        """Ensure broker communicator is started.

        :raises RuntimeError: If broker not started.
        """
        if self._broker_communicator is None or self._broker_communicator.is_closed():
            raise RuntimeError(
                'Broker not started. Call start() first or use verdi scheduler start to start the broker daemon.'
            )
