###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Protocol definitions for worker executor abstraction."""

from __future__ import annotations

from typing import Protocol, TypedDict

__all__ = ('WorkerExecutor', 'WorkerInfo')


class WorkerInfo(TypedDict):
    """Information about a worker process.

    This structure contains all metadata needed to interact with and
    monitor a worker process.
    """

    worker_id: str  # Unique worker identifier
    pid: int  # OS process ID
    task_pipe: str  # Path to worker's task pipe
    broadcast_pipe: str  # Path to worker's broadcast pipe
    reply_pipe: str | None  # Path to worker's reply pipe
    started_at: str  # ISO timestamp of worker start
    status: str  # Worker status: 'starting' | 'running' | 'stopping' | 'stopped'


class WorkerExecutor(Protocol):
    """Executor abstraction for worker lifecycle management.

    This protocol defines the interface for spawning, managing, and
    monitoring worker processes. Implementations handle the underlying
    process management mechanism (subprocess, systemd, kubernetes, etc.).

    The executor owns all worker lifecycle concerns:
    - Process spawning with correct environment
    - Worker registration in discovery system
    - Health monitoring and cleanup
    - Graceful shutdown with fallback to force kill
    """

    # Lifecycle management
    def start(self) -> None:
        """Initialize executor and prepare for spawning workers.

        This should set up any required resources (pipes, selectors, etc.)
        but does not spawn any workers yet.
        """
        ...

    def close(self) -> None:
        """Shutdown all workers and cleanup resources.

        Workers should be given a chance to shutdown gracefully before
        being force-killed. All executor resources should be cleaned up.
        """
        ...

    def is_closed(self) -> bool:
        """Check if executor is closed.

        :return: True if closed, False otherwise
        """
        ...

    # Worker management
    def start_worker(self, worker_id: str | None = None) -> WorkerInfo:
        """Spawn a new worker process.

        The worker should be fully started and registered in the discovery
        system before this method returns.

        :param worker_id: Optional worker identifier (generated if None)
        :return: WorkerInfo with details about spawned worker
        :raises RuntimeError: If worker fails to start
        :raises OSError: If process spawning fails
        """
        ...

    def stop_worker(self, worker_id: str) -> None:
        """Stop a specific worker gracefully.

        Worker should be sent shutdown signal and given time to cleanup
        before being force-killed if necessary.

        :param worker_id: Worker identifier to stop
        :raises KeyError: If worker not found
        """
        ...

    def get_workers(self) -> list[WorkerInfo]:
        """Get list of all managed workers.

        :return: List of WorkerInfo for all active workers
        """
        ...

    def get_worker(self, worker_id: str) -> WorkerInfo | None:
        """Get info for specific worker.

        :param worker_id: Worker identifier
        :return: WorkerInfo if found, None otherwise
        """
        ...

    # Scaling
    def scale_workers(self, target_count: int) -> None:
        """Scale worker pool to target count.

        Spawns new workers or stops excess workers to reach the target count.
        Workers to stop are selected using FIFO (oldest workers first).

        :param target_count: Desired number of workers (must be >= 0)
        :raises ValueError: If target_count is negative
        """
        ...

    def get_worker_count(self) -> int:
        """Get current number of managed workers.

        :return: Count of active workers
        """
        ...

    # Health monitoring
    def check_workers_health(self) -> list[str]:
        """Check health of all workers and cleanup dead/hung ones.

        This should check both process aliveness and heartbeats (if applicable).
        Dead workers should be cleaned up and removed from the executor's tracking.

        :return: List of worker_ids that were cleaned up
        """
        ...
