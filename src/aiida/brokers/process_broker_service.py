###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Process broker service for daemon management."""

from __future__ import annotations

import atexit
import logging
from pathlib import Path

from aiida.brokers.namedpipe import discovery
from aiida.brokers.namedpipe.broker_communicator import PipeBrokerCommunicator
from aiida.brokers.process_broker import ProcessBroker, ProcessBrokerConfig
from aiida.brokers.subprocess_executor import SubprocessWorkerExecutor

__all__ = ('ProcessBrokerService',)

LOGGER = logging.getLogger(__name__)


class ProcessBrokerService:
    """Daemon service wrapper for ProcessBroker.

    This class provides daemon management features:
    - Creates and owns PipeBrokerCommunicator
    - Creates ProcessBroker with communicator
    - Registers in discovery system
    - Handles cleanup on shutdown
    - Provides maintenance loop interface
    """

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
        enable_scheduling: bool | None = None,
    ):
        """Initialize the process broker service.

        :param profile_name: The AiiDA profile name
        :param config_path: Path to the AiiDA config directory
        :param enable_scheduling: Override scheduling config (None = read from file)
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._broker_id = 'broker'

        # Load or override config
        if enable_scheduling is not None:
            # Explicit override (used by verdi scheduler start)
            config = ProcessBrokerConfig(scheduling_enabled=enable_scheduling)
            if enable_scheduling:
                # Load limits from file if scheduling is enabled
                file_config = ProcessBrokerConfig.from_file(self._config_path)
                config.computer_limits = file_config.computer_limits
                config.default_limit = file_config.default_limit
        else:
            # Read from config file (used by verdi scheduler start with scheduling disabled)
            config = ProcessBrokerConfig.from_file(self._config_path)

        # Create executor for worker lifecycle management
        self._executor = SubprocessWorkerExecutor(
            profile_name=profile_name,
            config_path=self._config_path,
            environment=self._get_worker_environment(),
        )
        self._executor.start()

        # Create communicator
        self._communicator = PipeBrokerCommunicator(profile_name=profile_name, broker_id=self._broker_id)

        # Create broker with working directory and executor
        broker_working_dir = self._config_path / 'broker'
        self._broker = ProcessBroker(
            communicator=self._communicator,
            profile_name=profile_name,
            working_dir=broker_working_dir,
            config=config,
            executor=self._executor,
        )

        # Start initial workers if configured
        if config.initial_worker_count > 0:
            LOGGER.info(f'Starting {config.initial_worker_count} initial workers')
            try:
                self._executor.scale_workers(config.initial_worker_count)
            except Exception as exc:
                LOGGER.error(f'Failed to start initial workers: {exc}')

        # Register in discovery
        broker_pipes = self._communicator.get_broker_pipes()
        discovery.register_broker(
            profile_name, self._broker_id, task_pipe=broker_pipes['task_pipe'], broadcast_pipe=broker_pipes['broadcast_pipe']
        )

        # Register cleanup
        atexit.register(self.close)

        LOGGER.info(f'ProcessBrokerService started for profile: {profile_name} (scheduling={config.scheduling_enabled})')

    def _get_worker_environment(self) -> dict[str, str]:
        """Get environment variables for worker processes.

        :return: Dict of environment variables
        """
        import os

        env = {}

        # Pass through important AiiDA environment variables
        if 'AIIDA_PATH' in os.environ:
            env['AIIDA_PATH'] = os.environ['AIIDA_PATH']

        return env

    def run_maintenance(self) -> None:
        """Run periodic maintenance tasks.

        Delegate to ProcessBroker's maintenance method.
        """
        self._broker.run_maintenance()

    def get_status(self) -> dict:
        """Get broker status for CLI display.

        :return: Status dict with scheduling info if enabled
        """
        return self._broker.get_status()

    def close(self) -> None:
        """Close the service and unregister from discovery."""
        if self._broker.is_closed():
            return

        # Close broker (which closes communicator)
        self._broker.close()

        # Close executor (stops all workers)
        try:
            self._executor.close()
        except Exception as exc:
            LOGGER.warning(f'Error closing executor: {exc}')

        # Unregister from discovery
        try:
            discovery.unregister_broker(self._profile_name, self._broker_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering broker: {exc}')

        LOGGER.info('ProcessBrokerService stopped')

    def is_closed(self) -> bool:
        """Check if service is closed.

        :return: True if closed, False otherwise
        """
        return self._broker.is_closed()
