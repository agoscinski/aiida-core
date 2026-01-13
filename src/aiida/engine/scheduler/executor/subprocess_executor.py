###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Subprocess-based worker executor implementation."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .protocol import WorkerInfo

__all__ = ('SubprocessWorkerExecutor',)

LOGGER = logging.getLogger(__name__)


@dataclass
class WorkerProcess:
    """Internal tracking for a spawned worker process."""

    worker_id: str
    process: subprocess.Popen
    info: WorkerInfo
    started_at: float


class SubprocessWorkerExecutor:
    """Subprocess-based worker executor.

    Spawns workers as independent Python subprocesses running the
    'verdi daemon worker' command. Each worker:
    - Gets unique worker_id
    - Creates own pipes for IPC
    - Registers in discovery system
    - Runs asyncio event loop

    Features:
    - Process spawning with correct environment
    - Worker registration in discovery system
    - Health monitoring via process aliveness check
    - Graceful shutdown with fallback to force kill
    """

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
        verdi_path: str | None = None,
        python_path: str | None = None,
        environment: dict[str, str] | None = None,
    ):
        """Initialize subprocess executor.

        :param profile_name: AiiDA profile name
        :param config_path: Path to profile config directory
        :param verdi_path: Path to verdi executable (auto-detect if None)
        :param python_path: Python interpreter path (sys.executable if None)
        :param environment: Additional environment variables
        """
        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._verdi_path = verdi_path
        self._python_path = python_path or sys.executable
        self._environment = environment or {}

        # Track spawned workers
        self._workers: dict[str, WorkerProcess] = {}

        # State
        self._closed = False
        self._started = False

    def start(self) -> None:
        """Initialize executor and prepare for spawning workers."""
        if self._started:
            return

        LOGGER.info(f'Starting SubprocessWorkerExecutor for profile: {self._profile_name}')
        self._started = True
        LOGGER.info('SubprocessWorkerExecutor started successfully')

    def close(self) -> None:
        """Shutdown all workers and cleanup resources.

        Workers are given a chance to shutdown gracefully before
        being force-killed.
        """
        if self._closed:
            return

        LOGGER.info('Closing SubprocessWorkerExecutor...')

        # Stop all workers
        worker_ids = list(self._workers.keys())
        for worker_id in worker_ids:
            try:
                self.stop_worker(worker_id)
            except Exception as exc:
                LOGGER.error(f'Error stopping worker {worker_id}: {exc}')

        self._closed = True
        LOGGER.info('SubprocessWorkerExecutor closed')

    def is_closed(self) -> bool:
        """Check if executor is closed.

        :return: True if closed, False otherwise
        """
        return self._closed

    def start_worker(self, worker_id: str | None = None) -> WorkerInfo:
        """Spawn a new worker process.

        The worker is fully started and registered in the discovery
        system before this method returns.

        :param worker_id: Optional worker identifier (generated if None)
        :return: WorkerInfo with details about spawned worker
        :raises RuntimeError: If worker fails to start
        :raises OSError: If process spawning fails
        """
        if self._closed:
            raise RuntimeError('Executor is closed')

        if not self._started:
            raise RuntimeError('Executor not started - call start() first')

        # Generate worker_id if not provided
        if worker_id is None:
            worker_id = f'worker_{uuid.uuid4().hex[:8]}'

        LOGGER.info(f'Starting worker: {worker_id}')

        # Build command to spawn worker
        # Use verdi daemon worker command
        verdi_cmd = self._verdi_path or 'verdi'
        command = [
            verdi_cmd,
            'daemon',
            'worker',
            '--worker-id',
            worker_id,
        ]

        # Build environment
        env = os.environ.copy()
        env.update(self._environment)
        env['PYTHONUNBUFFERED'] = '1'  # Disable output buffering
        env['AIIDA_PROFILE'] = self._profile_name  # Set profile for worker

        # Spawn subprocess
        try:
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,  # Detach from parent session
            )
        except Exception as exc:
            LOGGER.error(f'Failed to spawn worker {worker_id}: {exc}')
            raise OSError(f'Failed to spawn worker: {exc}') from exc

        LOGGER.info(f'Worker {worker_id} spawned with PID: {process.pid}')

        # Wait for worker to register in discovery
        try:
            worker_info = self._wait_for_registration(worker_id, timeout=10.0)
        except RuntimeError:
            # Worker failed to register - capture its output for debugging
            if process.poll() is not None:
                # Process has exited
                stdout, stderr = process.communicate(timeout=1.0)
                LOGGER.error(f'Worker {worker_id} exited with code {process.returncode}')
                if stdout:
                    LOGGER.error(f'Worker stdout: {stdout.decode(errors="replace")}')
                if stderr:
                    LOGGER.error(f'Worker stderr: {stderr.decode(errors="replace")}')
            else:
                # Process still running but didn't register
                LOGGER.error(f'Worker {worker_id} running (PID: {process.pid}) but did not register')
            raise

        # Track worker
        self._workers[worker_id] = WorkerProcess(
            worker_id=worker_id,
            process=process,
            info=worker_info,
            started_at=time.time(),
        )

        LOGGER.info(f'Worker {worker_id} started successfully (PID: {process.pid})')
        return worker_info

    def stop_worker(self, worker_id: str) -> None:
        """Stop a specific worker gracefully.

        Worker is sent shutdown signal and given time to cleanup
        before being force-killed if necessary.

        :param worker_id: Worker identifier to stop
        :raises KeyError: If worker not found
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            raise KeyError(f'Worker not found: {worker_id}')

        LOGGER.info(f'Stopping worker: {worker_id}')

        # Send SIGTERM for graceful shutdown
        try:
            worker.process.terminate()

            # Wait for graceful shutdown
            try:
                worker.process.wait(timeout=5.0)
                LOGGER.info(f'Worker {worker_id} stopped gracefully')
            except subprocess.TimeoutExpired:
                # Force kill if still alive
                LOGGER.warning(f'Worker {worker_id} did not stop gracefully, force killing')
                worker.process.kill()
                worker.process.wait(timeout=1.0)
                LOGGER.info(f'Worker {worker_id} force killed')

        except Exception as exc:
            LOGGER.error(f'Error stopping worker {worker_id}: {exc}')
            # Try force kill anyway
            try:
                worker.process.kill()
                worker.process.wait(timeout=1.0)
            except Exception:
                pass

        finally:
            # Cleanup tracking
            if worker_id in self._workers:
                del self._workers[worker_id]

    def get_workers(self) -> list[WorkerInfo]:
        """Get list of all managed workers.

        :return: List of WorkerInfo for all active workers
        """
        return [worker.info for worker in self._workers.values()]

    def get_worker(self, worker_id: str) -> WorkerInfo | None:
        """Get info for specific worker.

        :param worker_id: Worker identifier
        :return: WorkerInfo if found, None otherwise
        """
        worker = self._workers.get(worker_id)
        return worker.info if worker else None

    def scale_workers(self, target_count: int) -> None:
        """Scale worker pool to target count.

        Spawns new workers or stops excess workers to reach the target count.
        Workers to stop are selected using FIFO (oldest workers first).

        :param target_count: Desired number of workers (must be >= 0)
        :raises ValueError: If target_count is negative
        """
        if target_count < 0:
            raise ValueError('target_count must be non-negative')

        current_count = len(self._workers)

        if target_count > current_count:
            # Spawn more workers
            to_spawn = target_count - current_count
            LOGGER.info(f'Scaling up: spawning {to_spawn} workers')
            for _ in range(to_spawn):
                try:
                    self.start_worker()
                except Exception as exc:
                    LOGGER.error(f'Failed to spawn worker during scaling: {exc}')

        elif target_count < current_count:
            # Stop excess workers (oldest first - FIFO)
            to_stop = current_count - target_count
            LOGGER.info(f'Scaling down: stopping {to_stop} workers')

            # Sort workers by start time, oldest first
            workers_by_age = sorted(self._workers.values(), key=lambda w: w.started_at)

            for worker in workers_by_age[:to_stop]:
                try:
                    self.stop_worker(worker.worker_id)
                except Exception as exc:
                    LOGGER.error(f'Failed to stop worker {worker.worker_id} during scaling: {exc}')

    def get_worker_count(self) -> int:
        """Get current number of managed workers.

        :return: Count of active workers
        """
        return len(self._workers)

    def check_workers_health(self) -> list[str]:
        """Check health of all workers and cleanup dead ones.

        Checks process aliveness via poll(). Dead workers are cleaned up
        and removed from the executor's tracking.

        :return: List of worker_ids that were cleaned up
        """
        dead_workers = []

        for worker_id, worker in list(self._workers.items()):
            # Check if process is still alive
            returncode = worker.process.poll()

            if returncode is not None:
                # Process died
                LOGGER.error(f'Worker {worker_id} died with exit code {returncode}')
                dead_workers.append(worker_id)
                del self._workers[worker_id]

        if dead_workers:
            LOGGER.warning(f'Cleaned up {len(dead_workers)} dead workers: {dead_workers}')

        return dead_workers

    def _wait_for_registration(self, worker_id: str, timeout: float = 10.0) -> WorkerInfo:
        """Wait for worker to register in discovery system.

        :param worker_id: Worker identifier to wait for
        :param timeout: Maximum time to wait in seconds
        :return: WorkerInfo from discovery
        :raises RuntimeError: If worker doesn't register within timeout
        """
        from aiida.communication.namedpipe import discovery

        deadline = time.time() + timeout
        poll_interval = 0.1

        while time.time() < deadline:
            # Check if worker registered
            workers = discovery.discover_workers(self._profile_name, check_alive=False)

            for worker in workers:
                if worker.get('process_id') == worker_id or worker.get('worker_id') == worker_id:
                    # Found our worker
                    LOGGER.info(f'Worker {worker_id} registered in discovery')

                    # Convert to WorkerInfo format
                    worker_info: WorkerInfo = {
                        'worker_id': worker_id,
                        'pid': worker.get('pid', 0),
                        'task_pipe': worker.get('task_pipe', ''),
                        'broadcast_pipe': worker.get('broadcast_pipe', ''),
                        'reply_pipe': worker.get('reply_pipe'),
                        'started_at': datetime.now().isoformat(),
                        'status': 'running',
                    }
                    return worker_info

            time.sleep(poll_interval)

        # Timeout
        raise RuntimeError(f'Worker {worker_id} failed to register within {timeout}s')

    def _generate_worker_id(self) -> str:
        """Generate unique worker identifier.

        :return: Unique worker ID string
        """
        return f'worker_{uuid.uuid4().hex[:8]}'

    def _find_verdi(self) -> str:
        """Find verdi executable in PATH.

        :return: Path to verdi executable
        :raises RuntimeError: If verdi not found
        """
        import shutil

        verdi_path = shutil.which('verdi')
        if verdi_path is None:
            raise RuntimeError('verdi executable not found in PATH')
        return verdi_path
