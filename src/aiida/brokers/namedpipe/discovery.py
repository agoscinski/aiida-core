"""Worker and coordinator discovery system using config folder."""

from __future__ import annotations

import json
import os
import time
import typing as t
from datetime import datetime
from pathlib import Path

__all__ = (
    'get_discovery_dir',
    'register_worker',
    'unregister_worker',
    'register_coordinator',
    'unregister_coordinator',
    'discover_workers',
    'discover_coordinator',
    'cleanup_dead_processes',
    'ProcessInfo',
)


class ProcessInfo(t.TypedDict):
    """Information about a registered process."""

    process_id: str
    process_type: str  # 'worker' or 'coordinator'
    task_pipe: str
    rpc_pipe: str
    broadcast_pipe: str
    reply_pipe: str | None
    pid: int
    alive: bool
    timestamp: str


def get_discovery_dir(config_path: Path | str) -> Path:
    """Get the discovery directory for a profile.

    :param config_path: Path to the AiiDA config directory.
    :return: Path to the discovery directory.
    """
    config_path = Path(config_path)
    discovery_dir = config_path / 'pipes'
    discovery_dir.mkdir(parents=True, exist_ok=True)
    return discovery_dir


def _write_process_info(discovery_dir: Path, process_id: str, info: ProcessInfo) -> None:
    """Write process information to discovery file.

    :param discovery_dir: Directory for discovery files.
    :param process_id: Process identifier.
    :param info: Process information.
    """
    file_path = discovery_dir / f'{process_id}.json'
    with open(file_path, 'w') as f:
        json.dump(info, f, indent=2)


def _read_process_info(file_path: Path) -> ProcessInfo | None:
    """Read process information from discovery file.

    :param file_path: Path to the discovery file.
    :return: Process information or None if file is invalid.
    """
    try:
        with open(file_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _is_process_alive(pid: int) -> bool:
    """Check if a process is alive.

    :param pid: Process ID.
    :return: True if process is alive, False otherwise.
    """
    try:
        # Send signal 0 to check if process exists
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def register_worker(
    config_path: Path | str,
    worker_id: str,
    task_pipe: str,
    rpc_pipe: str,
    broadcast_pipe: str,
    reply_pipe: str | None = None,
) -> None:
    """Register a worker in the discovery system.

    :param config_path: Path to the AiiDA config directory.
    :param worker_id: Worker identifier.
    :param task_pipe: Path to worker's task pipe.
    :param rpc_pipe: Path to worker's RPC pipe.
    :param broadcast_pipe: Path to worker's broadcast pipe.
    :param reply_pipe: Path to worker's reply pipe (optional).
    """
    discovery_dir = get_discovery_dir(config_path)

    info: ProcessInfo = {
        'process_id': worker_id,
        'process_type': 'worker',
        'task_pipe': task_pipe,
        'rpc_pipe': rpc_pipe,
        'broadcast_pipe': broadcast_pipe,
        'reply_pipe': reply_pipe,
        'pid': os.getpid(),
        'alive': True,
        'timestamp': datetime.now().isoformat(),
    }

    _write_process_info(discovery_dir, worker_id, info)


def unregister_worker(config_path: Path | str, worker_id: str) -> None:
    """Unregister a worker from the discovery system.

    :param config_path: Path to the AiiDA config directory.
    :param worker_id: Worker identifier.
    """
    discovery_dir = get_discovery_dir(config_path)
    file_path = discovery_dir / f'{worker_id}.json'

    try:
        file_path.unlink()
    except FileNotFoundError:
        pass


def register_coordinator(
    config_path: Path | str,
    coordinator_id: str,
    task_pipe: str,
    broadcast_pipe: str,
) -> None:
    """Register the coordinator in the discovery system.

    :param config_path: Path to the AiiDA config directory.
    :param coordinator_id: Coordinator identifier (usually 'coordinator').
    :param task_pipe: Path to coordinator's task pipe.
    :param broadcast_pipe: Path to coordinator's broadcast pipe.
    """
    discovery_dir = get_discovery_dir(config_path)

    info: ProcessInfo = {
        'process_id': coordinator_id,
        'process_type': 'coordinator',
        'task_pipe': task_pipe,
        'rpc_pipe': '',  # Coordinator doesn't have RPC pipe
        'broadcast_pipe': broadcast_pipe,
        'reply_pipe': None,
        'pid': os.getpid(),
        'alive': True,
        'timestamp': datetime.now().isoformat(),
    }

    _write_process_info(discovery_dir, coordinator_id, info)


def unregister_coordinator(config_path: Path | str, coordinator_id: str = 'coordinator') -> None:
    """Unregister the coordinator from the discovery system.

    :param config_path: Path to the AiiDA config directory.
    :param coordinator_id: Coordinator identifier (default: 'coordinator').
    """
    discovery_dir = get_discovery_dir(config_path)
    file_path = discovery_dir / f'{coordinator_id}.json'

    try:
        file_path.unlink()
    except FileNotFoundError:
        pass


def discover_workers(config_path: Path | str, check_alive: bool = True) -> list[ProcessInfo]:
    """Discover all registered workers.

    :param config_path: Path to the AiiDA config directory.
    :param check_alive: If True, verify processes are actually alive.
    :return: List of worker process information.
    """
    discovery_dir = get_discovery_dir(config_path)
    workers = []

    for file_path in discovery_dir.glob('*.json'):
        info = _read_process_info(file_path)
        if info is None:
            continue

        if info['process_type'] != 'worker':
            continue

        if check_alive:
            alive = _is_process_alive(info['pid'])
            info['alive'] = alive

            if not alive:
                # Clean up stale entry
                try:
                    file_path.unlink()
                except OSError:
                    pass
                continue

        workers.append(info)

    return workers


def discover_coordinator(config_path: Path | str, coordinator_id: str = 'coordinator') -> ProcessInfo | None:
    """Discover the coordinator.

    :param config_path: Path to the AiiDA config directory.
    :param coordinator_id: Coordinator identifier (default: 'coordinator').
    :return: Coordinator process information or None if not found/dead.
    """
    discovery_dir = get_discovery_dir(config_path)
    file_path = discovery_dir / f'{coordinator_id}.json'

    if not file_path.exists():
        return None

    info = _read_process_info(file_path)
    if info is None:
        return None

    if info['process_type'] != 'coordinator':
        return None

    # Check if coordinator is alive
    alive = _is_process_alive(info['pid'])
    info['alive'] = alive

    if not alive:
        # Clean up stale entry
        try:
            file_path.unlink()
        except OSError:
            pass
        return None

    return info


def cleanup_dead_processes(config_path: Path | str) -> int:
    """Clean up discovery entries for dead processes.

    :param config_path: Path to the AiiDA config directory.
    :return: Number of entries cleaned up.
    """
    discovery_dir = get_discovery_dir(config_path)
    count = 0

    for file_path in discovery_dir.glob('*.json'):
        info = _read_process_info(file_path)
        if info is None:
            # Invalid file, remove it
            try:
                file_path.unlink()
                count += 1
            except OSError:
                pass
            continue

        if not _is_process_alive(info['pid']):
            try:
                file_path.unlink()
                count += 1
            except OSError:
                pass

    return count
