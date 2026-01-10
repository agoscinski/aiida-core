###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Protocol definitions for broker communication abstraction."""

from __future__ import annotations

from typing import Callable, Protocol

__all__ = ('BrokerCommunicator',)


class BrokerCommunicator(Protocol):
    """Transport abstraction for broker communications.

    This protocol defines the interface for receiving messages via callbacks
    and sending messages to workers. Implementations handle the underlying
    transport mechanism (pipes, sockets, etc.).

    The callback-based pattern follows PipeCommunicator: subscribers register
    callbacks that are invoked when messages arrive from the transport layer.
    """

    # Lifecycle management
    def start(self) -> None:
        """Start the communicator and initialize transport resources.

        This may include creating pipes, opening sockets, starting event loops, etc.
        """
        ...

    def close(self) -> None:
        """Close the communicator and cleanup transport resources.

        This should gracefully shut down any background threads, close file
        descriptors, and clean up any created resources (pipes, sockets, etc.).
        """
        ...

    def is_closed(self) -> bool:
        """Check if communicator is closed.

        :return: True if closed, False otherwise.
        """
        ...

    # Callback registration (receive side)
    def add_task_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming task messages.

        The callback will be invoked with the task message dict when
        a task is received from the transport layer.

        :param callback: Function to call with task message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier (generated if not provided)
        """
        ...

    def add_broadcast_subscriber(
        self,
        callback: Callable[[dict], None],
        identifier: str | None = None,
    ) -> str:
        """Register callback for incoming broadcast messages.

        The callback will be invoked with the broadcast message dict when
        a broadcast is received from the transport layer.

        :param callback: Function to call with broadcast message dict
        :param identifier: Optional subscriber identifier
        :return: Subscriber identifier (generated if not provided)
        """
        ...

    def remove_task_subscriber(self, identifier: str) -> None:
        """Remove a task subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        ...

    def remove_broadcast_subscriber(self, identifier: str) -> None:
        """Remove a broadcast subscriber by identifier.

        :param identifier: Subscriber identifier to remove
        """
        ...

    # Sending methods (send side)
    def task_send(
        self,
        worker_pipe: str,
        message: dict,
        non_blocking: bool = True,
    ) -> None:
        """Send a task message to a specific worker.

        :param worker_pipe: Worker's task pipe path (or other transport endpoint)
        :param message: Task message dict to send
        :param non_blocking: Use non-blocking write (default: True)
        :raises BrokenPipeError: If worker pipe/endpoint unavailable
        :raises OSError: If send fails
        """
        ...

    def broadcast_send(
        self,
        worker_pipes: list[str],
        message: dict,
        non_blocking: bool = True,
    ) -> int:
        """Send broadcast message to multiple workers.

        This method attempts to send to all workers and returns the count of
        successful sends. Failures on individual workers are logged but do not
        raise exceptions.

        :param worker_pipes: List of worker broadcast pipe paths (or transport endpoints)
        :param message: Broadcast message dict to send
        :param non_blocking: Use non-blocking write (default: True)
        :return: Number of successful sends
        """
        ...

    # Discovery integration
    def get_broker_pipes(self) -> dict[str, str]:
        """Get broker pipe paths for discovery registration.

        Returns the transport endpoints where clients should send messages.
        For pipe-based transport, this includes task_pipe and broadcast_pipe paths.

        :return: Dict with transport endpoint information (keys depend on implementation)
        """
        ...

    # High-level broker methods
    def get_communicator(self):
        """Return an instance of :class:`kiwipy.Communicator`.

        This provides access to the kiwipy communicator for RPC calls,
        task sending, and other kiwipy-based operations.

        :return: Instance of kiwipy.Communicator
        """
        ...

    def iterate_tasks(self):
        """Return an iterator over the tasks in the launch queue.

        This allows inspection of queued tasks for debugging and monitoring.

        :return: Iterator over task messages
        """
        ...
