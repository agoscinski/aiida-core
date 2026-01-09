"""Implementation of the message broker interface using named pipes."""

from __future__ import annotations

import functools
import typing as t

from aiida.brokers.broker import Broker

from .communicator import PipeCommunicator
from .discovery import discover_broker, get_discovery_dir

if t.TYPE_CHECKING:
    from pathlib import Path

    from aiida.manage.configuration.profile import Profile

__all__ = ('PipeBroker',)


class PipeBroker(Broker):
    """Implementation of the message broker interface using named pipes."""

    def __init__(self, profile: 'Profile') -> None:
        """Construct a new instance.

        :param profile: The profile.
        """
        super().__init__(profile)
        self._communicator: PipeCommunicator | None = None

    def __str__(self):
        broker = discover_broker(self._profile.name)
        if broker:
            return f'NamedPipe Broker @ {broker["task_pipe"]}'
        return 'NamedPipe Broker <Not running>'

    def close(self):
        """Close the broker."""
        if self._communicator is not None:
            self._communicator.close()
            self._communicator = None

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
        from aiida.orm.utils import serialize

        return PipeCommunicator(
            profile_name=self._profile.name,
            config_path=self._get_config_path(),
            encoder=functools.partial(serialize.serialize, encoding='utf-8'),
            decoder=serialize.deserialize_unsafe,
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
                'Please start it with: verdi broker start'
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
