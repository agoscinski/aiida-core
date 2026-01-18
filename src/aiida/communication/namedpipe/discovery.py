"""Worker and broker discovery system using config folder."""

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
    'register_broker',
    'unregister_broker',
    'discover_workers',
    'discover_broker',
    'cleanup_dead_processes',
    'ProcessInfo',
)


class ProcessInfo(t.TypedDict):
    """Information about a registered process."""

    process_id: str
    process_type: str  # 'worker' or 'broker'
    task_pipe: str
    rpc_pipe: str
    broadcast_pipe: str
    reply_pipe: str | None
    pid: int
    alive: bool
    timestamp: str


def get_discovery_dir(profile_name: str) -> Path:
    """Get the discovery directory for a profile.

    Discovery files are stored in the worker pipes directory.

    :param profile_name: Profile name.
    :return: Path to the discovery directory.
    """
    from . import utils
    return utils.get_worker_pipes_dir(profile_name)


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
    profile_name: str,
    worker_id: str,
    task_pipe: str,
    rpc_pipe: str,
    broadcast_pipe: str,
    reply_pipe: str | None = None,
) -> None:
    """Register a worker in the discovery system.

    :param profile_name: Profile name.
    :param worker_id: Worker identifier.
    :param task_pipe: Path to worker's task pipe.
    :param rpc_pipe: Path to worker's RPC pipe.
    :param broadcast_pipe: Path to worker's broadcast pipe.
    :param reply_pipe: Path to worker's reply pipe (optional).
    """
    discovery_dir = get_discovery_dir(profile_name)

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


def unregister_worker(profile_name: str, worker_id: str) -> None:
    """Unregister a worker from the discovery system.

    :param profile_name: Profile name.
    :param worker_id: Worker identifier.
    """
    discovery_dir = get_discovery_dir(profile_name)
    file_path = discovery_dir / f'{worker_id}.json'

    try:
        file_path.unlink()
    except FileNotFoundError:
        pass


def register_broker(
    profile_name: str,
    broker_id: str,
    task_pipe: str,
    broadcast_pipe: str,
) -> None:
    """Register the broker in the discovery system.

    :param profile_name: Profile name.
    :param broker_id: Broker identifier (usually 'broker').
    :param task_pipe: Path to broker's task pipe.
    :param broadcast_pipe: Path to broker's broadcast pipe.
    """
    discovery_dir = get_discovery_dir(profile_name)

    info: ProcessInfo = {
        'process_id': broker_id,
        'process_type': 'broker',
        'task_pipe': task_pipe,
        'rpc_pipe': '',  # Broker doesn't have RPC pipe
        'broadcast_pipe': broadcast_pipe,
        'reply_pipe': None,
        'pid': os.getpid(),
        'alive': True,
        'timestamp': datetime.now().isoformat(),
    }

    _write_process_info(discovery_dir, broker_id, info)


def unregister_broker(profile_name: str, broker_id: str = 'broker') -> None:
    """Unregister the broker from the discovery system.

    :param profile_name: Profile name.
    :param broker_id: Broker identifier (default: 'broker').
    """
    discovery_dir = get_discovery_dir(profile_name)
    file_path = discovery_dir / f'{broker_id}.json'

    try:
        file_path.unlink()
    except FileNotFoundError:
        pass


def discover_workers(profile_name: str, check_alive: bool = True) -> list[ProcessInfo]:
    """Discover all registered workers.

    :param profile_name: Profile name.
    :param check_alive: If True, verify processes are actually alive.
    :return: List of worker process information.
    """
    discovery_dir = get_discovery_dir(profile_name)
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


def discover_broker(profile_name: str, broker_id: str = 'broker') -> ProcessInfo | None:
    """Discover the broker.

    :param profile_name: Profile name.
    :param broker_id: Broker identifier (default: 'broker').
    :return: Broker process information or None if not found/dead.
    """
    discovery_dir = get_discovery_dir(profile_name)
    file_path = discovery_dir / f'{broker_id}.json'

    if not file_path.exists():
        return None

    info = _read_process_info(file_path)
    if info is None:
        return None

    if info['process_type'] != 'broker':
        return None

    # Check if broker is alive
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


def cleanup_dead_processes(profile_name: str) -> int:
    """Clean up discovery entries for dead processes.

    :param profile_name: Profile name.
    :return: Number of entries cleaned up.
    """
    discovery_dir = get_discovery_dir(profile_name)
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
