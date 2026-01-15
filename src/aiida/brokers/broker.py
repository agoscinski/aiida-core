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

    # Discovery integration
    def register_in_discovery(self) -> None:
        """Register this communicator in the discovery system.

        Implementations should register their transport endpoints so that
        workers and clients can discover how to connect.
        """
        ...

    def unregister_from_discovery(self) -> None:
        """Unregister this communicator from the discovery system.

        Should be called during shutdown to clean up discovery entries.
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
    def task_send(self, message: dict) -> None:
        """Send a task message to a specific worker.

        The message must contain routing information in the '_routing' key:
        - 'target_worker': Worker ID to send the task to

        The implementation is responsible for looking up how to reach the
        target worker (pipe path, queue name, etc.) based on its transport.

        :param message: Task message dict with '_routing' header
        :raises KeyError: If routing information is missing
        :raises BrokenPipeError: If worker endpoint unavailable
        :raises OSError: If send fails
        """
        ...

    def broadcast_send(self, message: dict) -> int:
        """Send broadcast message to all registered workers.

        The implementation discovers all active workers and sends the message
        to each. No routing information is required in the message.

        :param message: Broadcast message dict to send
        :return: Number of successful sends
        """
        ...

    def sanitize_message(self, message: dict, worker_id: str) -> None:
        """Sanitize message before sending to worker.

        This method allows the communicator to fix up any transport-specific
        parts of the message before sending. For example, updating reply
        endpoints if the original client is no longer reachable.

        :param message: Message dict to sanitize (modified in place)
        :param worker_id: Target worker ID
        """
        ...
