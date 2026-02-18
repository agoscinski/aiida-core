"""Interface for a message broker that facilitates communication with and between process runners."""

import abc
import typing as t

if t.TYPE_CHECKING:
    from aiida.manage.configuration.profile import Profile

__all__ = ('Broker',)


class Broker:
    """Interface for a message broker that facilitates communication with and between process runners."""

    def __init__(self, profile: 'Profile') -> None:
        """Construct a new instance.

        :param profile: The profile.
        """
        self._profile = profile

    @abc.abstractmethod
    def get_communicator(self):
        """Return an instance of :class:`kiwipy.Communicator`."""

    @abc.abstractmethod
    def iterate_tasks(self):
        """Return an iterator over the tasks in the launch queue."""

    @abc.abstractmethod
    def close(self):
        """Close the broker."""

    @abc.abstractmethod
    def get_full_queue_name(self, user_queue: str, queue_type: str) -> str:
        """Get the full queue name for routing.

        :param user_queue: The user-defined queue name (e.g., 'default').
        :param queue_type: The queue type (e.g., 'root-workchain', 'calcjob').
        :return: The full queue name for routing.
        """

    @abc.abstractmethod
    def get_queue_types(self) -> list[str]:
        """Get the list of queue types.

        :return: List of queue type names.
        """

    @abc.abstractmethod
    def get_task_queue(self, queue_type: str, user_queue: str):
        """Get a task queue by type and user queue name.

        :param queue_type: The queue type.
        :param user_queue: The user-defined queue name.
        :return: The task queue instance.
        """
