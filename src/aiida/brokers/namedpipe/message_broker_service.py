"""Message broker service for managing task queue and broadcast distribution."""

from __future__ import annotations

import atexit
import json
import logging
import os
import selectors
import threading
import time
import typing as t
import uuid
from pathlib import Path

from . import discovery, messages, utils
from .message_broker import MessageBroker
from .task_queue import TaskQueue

__all__ = ('MessageBrokerService', 'TaskQueue')

LOGGER = logging.getLogger(__name__)


class MessageBrokerService:
    """Standalone message broker daemon with discovery registration.

    This is a thin wrapper around MessageBroker that adds daemon management
    features like discovery registration/unregistration.
    """

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
    ):
        """Initialize standalone message broker daemon.

        :param profile_name: The AiiDA profile name.
        :param config_path: Path to the AiiDA config directory.
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._broker_id = 'broker'

        # Create core with working directory
        broker_working_dir = self._config_path / 'broker'
        self._core = MessageBroker(
            profile_name=profile_name,
            working_dir=broker_working_dir,
        )

        # Register in discovery (daemon management)
        # Use derived pipe paths for discovery registration
        broker_pipes_dir = utils.get_broker_pipes_dir(profile_name)
        task_pipe_path = broker_pipes_dir / 'broker_tasks'
        broadcast_pipe_path = broker_pipes_dir / 'broker_broadcast'
        discovery.register_broker(
            profile_name,
            self._broker_id,
            task_pipe=str(task_pipe_path),
            broadcast_pipe=str(broadcast_pipe_path)
        )

        # Register cleanup
        atexit.register(self.close)

        LOGGER.info(f'MessageBrokerService daemon started for profile: {profile_name}')

    def run_maintenance(self) -> None:
        """Delegate to core."""
        self._core.run_maintenance()

    def close(self) -> None:
        """Close message broker and unregister."""
        if self._core.is_closed():
            return

        # Close core
        self._core.close()

        # Unregister from discovery
        try:
            discovery.unregister_broker(self._profile_name, self._broker_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering message broker: {exc}')

        LOGGER.info('MessageBrokerService stopped')

    def is_closed(self) -> bool:
        """Check if closed."""
        return self._core.is_closed()
