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


def start_daemon(profile_name: str, config_path: Path | str) -> None:
    """Start the scheduler as a daemon process.

    This function forks and daemonizes, then runs ProcessSchedulerService
    in the child process. The parent process returns immediately.

    :param profile_name: The AiiDA profile name
    :param config_path: Path to the AiiDA config directory
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
    ):
        """Initialize the process broker service.

        :param profile_name: The AiiDA profile name
        :param config_path: Path to the AiiDA config directory
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._broker_id = 'broker'

        # Load config from file
        config = ProcessSchedulerConfig.from_file(self._config_path)

        # Clean up orphaned workers from previous runs
        self._cleanup_orphaned_workers(profile_name)

        # Create executor for worker lifecycle management
        self._executor = SubprocessWorkerExecutor(
            profile_name=profile_name,
            config_path=self._config_path,
            environment=self._get_worker_environment(),
        )
        self._executor.start()

        # Create communicator
        self._communicator = PipeBrokerCommunicator(profile_name=profile_name, broker_id=self._broker_id)

        # Create scheduler with working directory and executor
        scheduler_working_dir = self._config_path / 'scheduler'
        self._broker = ProcessScheduler(
            communicator=self._communicator,
            profile_name=profile_name,
            working_dir=scheduler_working_dir,
            config=config,
            executor=self._executor,
        )

        # Register in discovery BEFORE spawning workers
        # (workers check if broker is running via discovery)
        broker_pipes = self._communicator.get_broker_pipes()
        discovery.register_broker(
            profile_name, self._broker_id, task_pipe=broker_pipes['task_pipe'], broadcast_pipe=broker_pipes['broadcast_pipe']
        )

        # Register cleanup
        atexit.register(self.close)

        # Store worker count for auto-respawn
        self._worker_count = config.worker_count

        LOGGER.info(f'ProcessSchedulerService started for profile: {profile_name}')

        # Start initial workers if configured (after broker is registered)
        if self._worker_count > 0:
            LOGGER.info(f'Starting {self._worker_count} workers')
            try:
                self._executor.scale_workers(self._worker_count)
            except Exception as exc:
                LOGGER.error(f'Failed to start workers: {exc}')

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

    def _cleanup_orphaned_workers(self, profile_name: str) -> None:
        """Clean up workers from previous scheduler runs.

        When the scheduler starts fresh, any existing workers are orphaned
        and should be terminated.

        :param profile_name: The AiiDA profile name
        """
        import os
        import signal

        # Find all registered workers (including dead ones)
        workers = discovery.discover_workers(profile_name, check_alive=False)

        for worker in workers:
            pid = worker['pid']
            worker_id = worker['process_id']

            # Try to kill the process if it's still alive
            try:
                os.kill(pid, signal.SIGTERM)
                LOGGER.info(f'Terminated orphaned worker {worker_id} (PID: {pid})')
            except ProcessLookupError:
                pass  # Process already dead
            except PermissionError:
                LOGGER.warning(f'Cannot terminate worker {worker_id} (PID: {pid}): permission denied')

            # Remove discovery entry
            discovery.unregister_worker(profile_name, worker_id)

    def run_maintenance(self) -> None:
        """Run periodic maintenance tasks.

        - Delegates to ProcessScheduler's maintenance method
        - Auto-respawns dead workers to maintain configured count
        """
        self._broker.run_maintenance()

        # Auto-respawn: maintain configured worker count
        if self._worker_count > 0:
            current_count = self._executor.get_worker_count()
            if current_count < self._worker_count:
                LOGGER.info(f'Auto-respawning workers: {current_count} -> {self._worker_count}')
                try:
                    self._executor.scale_workers(self._worker_count)
                except Exception as exc:
                    LOGGER.error(f'Failed to respawn workers: {exc}')

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
