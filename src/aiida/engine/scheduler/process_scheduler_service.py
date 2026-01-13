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

from aiida.communication.namedpipe import discovery
from aiida.communication.namedpipe.broker_communicator import PipeBrokerCommunicator
from aiida.engine.scheduler.process_scheduler import ProcessScheduler, ProcessSchedulerConfig
from aiida.engine.scheduler.executor import SubprocessWorkerExecutor

__all__ = ('ProcessSchedulerService', 'start_daemon')

LOGGER = logging.getLogger(__name__)


def start_daemon(profile_name: str, config_path: Path | str, enable_scheduling: bool = True) -> None:
    """Start the scheduler as a daemon process.

    This function forks and daemonizes, then runs ProcessSchedulerService
    in the child process. The parent process returns immediately.

    :param profile_name: The AiiDA profile name
    :param config_path: Path to the AiiDA config directory
    :param enable_scheduling: Whether to enable per-computer scheduling
    """
    import os
    import sys

    config_path = Path(config_path)

    # Fork the process
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait briefly then return
            import time
            time.sleep(0.5)
            return
    except OSError as exc:
        raise RuntimeError(f'Fork failed: {exc}') from exc

    # Child process - become session leader
    os.setsid()

    # Second fork to prevent zombie
    try:
        pid = os.fork()
        if pid > 0:
            # First child exits
            sys.exit(0)
    except OSError as exc:
        LOGGER.error(f'Second fork failed: {exc}')
        sys.exit(1)

    # Grandchild continues as daemon
    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    # Close stdin/stdout/stderr
    with open(os.devnull, 'r') as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    with open(os.devnull, 'a+') as devnull:
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())

    # Configure logging to file
    log_file = config_path / 'scheduler' / 'scheduler.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create and run scheduler
    scheduler = ProcessSchedulerService(
        profile_name=profile_name,
        config_path=config_path,
        enable_scheduling=enable_scheduling,
    )

    # Run maintenance loop
    import time
    try:
        while True:
            time.sleep(1)
            scheduler.run_maintenance()
    except Exception:
        logging.exception('Scheduler daemon crashed')
        scheduler.close()
        sys.exit(1)


class ProcessSchedulerService:
    """Daemon service wrapper for ProcessScheduler.

    This class provides daemon management features:
    - Creates and owns PipeBrokerCommunicator
    - Creates ProcessScheduler with communicator
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
            config = ProcessSchedulerConfig(scheduling_enabled=enable_scheduling)
            if enable_scheduling:
                # Load limits from file if scheduling is enabled
                file_config = ProcessSchedulerConfig.from_file(self._config_path)
                config.computer_limits = file_config.computer_limits
                config.default_limit = file_config.default_limit
        else:
            # Read from config file (used by verdi scheduler start with scheduling disabled)
            config = ProcessSchedulerConfig.from_file(self._config_path)

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
        self._broker = ProcessScheduler(
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

        LOGGER.info(f'ProcessSchedulerService started for profile: {profile_name} (scheduling={config.scheduling_enabled})')

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

        Delegate to ProcessScheduler's maintenance method.
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

        LOGGER.info('ProcessSchedulerService stopped')

    def is_closed(self) -> bool:
        """Check if service is closed.

        :return: True if closed, False otherwise
        """
        return self._broker.is_closed()
