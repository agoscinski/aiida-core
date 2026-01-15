"""Implementation of the message broker interface using RabbitMQ through ``kiwipy``."""

from __future__ import annotations

import functools
import typing as t
from typing import Callable

from aiida.common.log import AIIDA_LOGGER
from aiida.manage.configuration import get_config_option

from .utils import get_launch_queue_name, get_message_exchange_name, get_task_exchange_name

if t.TYPE_CHECKING:
    from kiwipy.rmq import RmqThreadCommunicator

    from aiida.manage.configuration.profile import Profile

LOGGER = AIIDA_LOGGER.getChild('broker.rabbitmq')

__all__ = ('RabbitmqBroker',)


class RabbitmqBroker:
    """Implementation of the message broker interface using RabbitMQ through ``kiwipy``."""

    def __init__(self, profile: Profile) -> None:
        """Construct a new instance.

        :param profile: The profile.
        """
        self._profile = profile
        self._communicator: 'RmqThreadCommunicator' | None = None
        self._prefix = f'aiida-{self._profile.uuid}'

    def __str__(self):
        try:
            return f'RabbitMQ v{self.get_rabbitmq_version()} @ {self.get_url()}'
        except ConnectionError:
            return f'RabbitMQ @ {self.get_url()} <Connection failed>'

    def close(self):
        """Close the broker."""
        if self._communicator is not None:
            self._communicator.close()
            self._communicator = None

    def iterate_tasks(self):
        """Return an iterator over the tasks in the launch queue."""
        for task in self.get_communicator().task_queue(get_launch_queue_name(self._prefix)):
            yield task

    def get_communicator(self) -> 'RmqThreadCommunicator':
        if self._communicator is None:
            self._communicator = self._create_communicator()
            # Check whether a compatible version of RabbitMQ is being used.
            self.check_rabbitmq_version()

        return self._communicator

    def _create_communicator(self) -> 'RmqThreadCommunicator':
        """Return an instance of :class:`kiwipy.Communicator`."""
        from kiwipy.rmq import RmqThreadCommunicator

        from aiida.orm.utils import serialize

        self._communicator = RmqThreadCommunicator.connect(
            connection_params={'url': self.get_url()},
            message_exchange=get_message_exchange_name(self._prefix),
            encoder=functools.partial(serialize.serialize, encoding='utf-8'),
            decoder=serialize.deserialize_unsafe,
            task_exchange=get_task_exchange_name(self._prefix),
            task_queue=get_launch_queue_name(self._prefix),
            task_prefetch_count=get_config_option('daemon.worker_process_slots'),
            async_task_timeout=get_config_option('rmq.task_timeout'),
            # This is needed because the verdi commands will call this function and when called in unit tests the
            # testing_mode cannot be set.
            testing_mode=self._profile.is_test_profile,
        )

        return self._communicator

    def check_rabbitmq_version(self):
        """Check the version of RabbitMQ that is being connected to and emit warning if it is not compatible."""
        show_warning = get_config_option('warnings.rabbitmq_version')
        version = self.get_rabbitmq_version()

        if show_warning and not self.is_rabbitmq_version_supported():
            LOGGER.warning(f'RabbitMQ v{version} is not supported and will cause unexpected problems!')
            LOGGER.warning('It can cause long-running workflows to crash and jobs to be submitted multiple times.')
            LOGGER.warning('See https://github.com/aiidateam/aiida-core/wiki/RabbitMQ-version-to-use for details.')
            return version, False

        return version, True

    def get_url(self) -> str:
        """Return the RMQ url for this profile."""
        from .utils import get_rmq_url

        kwargs = {
            key[7:]: val for key, val in self._profile.process_control_config.items() if key.startswith('broker_')
        }
        additional_kwargs = kwargs.pop('parameters', {})
        return get_rmq_url(**kwargs, **additional_kwargs)

    def is_rabbitmq_version_supported(self) -> bool:
        """Return whether the version of RabbitMQ configured for the current profile is supported.

        Versions 3.5 and below are not supported at all, whereas versions 3.8.15 and above are not compatible with a
        default configuration of the RabbitMQ server.

        :return: boolean whether the current RabbitMQ version is supported.
        """
        from packaging.version import parse

        return parse('3.6.0') <= self.get_rabbitmq_version() < parse('3.8.15')

    def get_rabbitmq_version(self):
        """Return the version of the RabbitMQ server that the current profile connects to.

        :return: :class:`packaging.version.Version`
        """
        from packaging.version import parse

        return parse(self.get_communicator().server_properties['version'])

    # Stub implementations for BrokerCommunicator methods
    # RabbitMQ broker uses kiwipy.RmqThreadCommunicator which has different architecture

    def start(self) -> None:
        """Start the broker (not applicable for RabbitMQ)."""
        # RabbitMQ broker doesn't need explicit start - connection happens in get_communicator()
        pass

    def is_closed(self) -> bool:
        """Check if broker is closed.

        :return: True if closed, False otherwise.
        """
        return self._communicator is None or self._communicator.is_closed()

    def add_task_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming task messages.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's add_task_subscriber.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().add_task_subscriber() instead.'
        )

    def add_broadcast_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming broadcast messages.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's add_broadcast_subscriber.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().add_broadcast_subscriber() instead.'
        )

    def remove_task_subscriber(self, identifier: str) -> None:
        """Remove a task subscriber by identifier.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's remove_task_subscriber.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().remove_task_subscriber() instead.'
        )

    def remove_broadcast_subscriber(self, identifier: str) -> None:
        """Remove a broadcast subscriber by identifier.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's remove_broadcast_subscriber.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().remove_broadcast_subscriber() instead.'
        )

    def task_send(self, message: dict) -> None:
        """Send a task message to a specific worker.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's task_send.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().task_send() instead.'
        )

    def broadcast_send(self, message: dict) -> int:
        """Send broadcast message to all workers.

        :raises NotImplementedError: RabbitMQ broker uses kiwipy's broadcast_send.
        """
        raise NotImplementedError(
            'RabbitMQ broker does not support this method directly. '
            'Use get_communicator().broadcast_send() instead.'
        )

    def register_in_discovery(self) -> None:
        """Register this communicator in the discovery system.

        RabbitMQ does not use file-based discovery - it uses AMQP message routing.
        This is a no-op for RabbitMQ.
        """
        pass

    def unregister_from_discovery(self) -> None:
        """Unregister this communicator from the discovery system.

        RabbitMQ does not use file-based discovery - it uses AMQP message routing.
        This is a no-op for RabbitMQ.
        """
        pass
