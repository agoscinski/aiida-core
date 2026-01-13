"""Utilities for named pipe management and I/O operations."""

from __future__ import annotations

import errno
import os
import typing as t
from pathlib import Path

__all__ = (
    'create_pipe',
    'cleanup_pipe',
    'write_to_pipe',
    'open_pipe_read',
    'open_pipe_write',
    'get_pipes_base_dir',
    'get_broker_pipes_dir',
    'get_worker_pipes_dir',
)


def get_pipes_base_dir(profile_name: str) -> Path:
    """Get the base pipes directory for a profile.

    :param profile_name: Profile name.
    :return: Base pipes directory: /tmp/aiida-scheduler/{profile}/pipes/
    """
    pipes_dir = Path('/tmp') / 'aiida-scheduler' / profile_name / 'pipes'
    pipes_dir.mkdir(parents=True, exist_ok=True)
    return pipes_dir


def get_broker_pipes_dir(profile_name: str) -> Path:
    """Get broker pipes directory.

    :param profile_name: Profile name.
    :return: Broker pipes directory: /tmp/aiida-scheduler/{profile}/pipes/broker/
    """
    broker_dir = get_pipes_base_dir(profile_name) / 'broker'
    broker_dir.mkdir(parents=True, exist_ok=True)
    return broker_dir


def get_worker_pipes_dir(profile_name: str) -> Path:
    """Get worker pipes directory.

    :param profile_name: Profile name.
    :return: Worker pipes directory: /tmp/aiida-scheduler/{profile}/pipes/worker/
    """
    worker_dir = get_pipes_base_dir(profile_name) / 'worker'
    worker_dir.mkdir(parents=True, exist_ok=True)
    return worker_dir


def create_pipe(pipe_path: str | Path, mode: int = 0o600) -> None:
    """Create a named pipe (FIFO).

    :param pipe_path: Path where the pipe should be created.
    :param mode: Permission mode for the pipe (default: owner read/write only).
    :raises FileExistsError: If pipe already exists.
    :raises OSError: If pipe creation fails.
    """
    pipe_path = Path(pipe_path)

    # Ensure parent directory exists
    pipe_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        os.mkfifo(str(pipe_path), mode=mode)
    except FileExistsError:
        # Check if it's actually a FIFO
        import stat

        if not stat.S_ISFIFO(pipe_path.stat().st_mode):
            raise FileExistsError(f'Path exists but is not a FIFO: {pipe_path}')
        # If it's already a FIFO, that's okay
    except OSError as exc:
        raise OSError(f'Failed to create pipe {pipe_path}: {exc}') from exc


def cleanup_pipe(pipe_path: str | Path) -> None:
    """Remove a named pipe.

    :param pipe_path: Path to the pipe to remove.
    """
    try:
        os.unlink(str(pipe_path))
    except FileNotFoundError:
        pass  # Already removed
    except OSError:
        pass  # Best effort cleanup


def open_pipe_read(pipe_path: str | Path, non_blocking: bool = True) -> int:
    """Open a named pipe for reading.

    Uses O_RDWR instead of O_RDONLY to prevent EOF when all writers disconnect.
    This keeps the pipe open and ready for new writers.

    :param pipe_path: Path to the pipe.
    :param non_blocking: If True, open in non-blocking mode (default: True).
    :return: File descriptor.
    :raises FileNotFoundError: If pipe doesn't exist.
    :raises OSError: If open fails.
    """
    # Use O_RDWR to prevent EOF when writers disconnect
    flags = os.O_RDWR
    if non_blocking:
        flags |= os.O_NONBLOCK

    try:
        return os.open(str(pipe_path), flags)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f'Pipe not found: {pipe_path}') from exc
    except OSError as exc:
        raise OSError(f'Failed to open pipe for reading {pipe_path}: {exc}') from exc


def open_pipe_write(pipe_path: str | Path, non_blocking: bool = False) -> int:
    """Open a named pipe for writing.

    :param pipe_path: Path to the pipe.
    :param non_blocking: If True, open in non-blocking mode (default: False).
    :return: File descriptor.
    :raises FileNotFoundError: If pipe doesn't exist.
    :raises OSError: If open fails (e.g., ENXIO if no reader).
    """
    flags = os.O_WRONLY
    if non_blocking:
        flags |= os.O_NONBLOCK

    try:
        return os.open(str(pipe_path), flags)
    except OSError as exc:
        if exc.errno == errno.ENXIO:
            raise BrokenPipeError(f'No reader on pipe: {pipe_path}') from exc
        raise OSError(f'Failed to open pipe for writing {pipe_path}: {exc}') from exc


def write_to_pipe(
    pipe_path: str | Path, data: bytes, non_blocking: bool = False, timeout: float | None = None, use_lock: bool = False
) -> None:
    """Write data to a named pipe.

    :param pipe_path: Path to the pipe.
    :param data: Bytes to write.
    :param non_blocking: If True, use non-blocking write (default: False).
    :param timeout: Optional timeout in seconds (only used if non_blocking=False).
    :param use_lock: If True, acquire exclusive lock before writing (for multi-writer pipes).
    :raises BrokenPipeError: If no reader is available.
    :raises TimeoutError: If write times out.
    :raises OSError: If write fails.
    """
    import fcntl

    lock_fd = None
    fd = None
    try:
        # Acquire lock if requested (for pipes with multiple writers)
        if use_lock:
            lock_path = Path(str(pipe_path) + '.lock')
            lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

        fd = open_pipe_write(pipe_path, non_blocking=non_blocking)

        # Write all data
        bytes_written = 0
        while bytes_written < len(data):
            try:
                n = os.write(fd, data[bytes_written:])
                if n == 0:
                    raise OSError('Write returned 0 bytes')
                bytes_written += n
            except BlockingIOError:
                if non_blocking:
                    raise BrokenPipeError(f'Pipe full or no reader: {pipe_path}')
                # For blocking mode, retry
                import time

                time.sleep(0.01)

    finally:
        if fd is not None:
            os.close(fd)
        if lock_fd is not None:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)


def read_from_pipe(pipe_path: str | Path, size: int = 4096) -> bytes:
    """Read data from a named pipe (blocking read).

    :param pipe_path: Path to the pipe.
    :param size: Maximum bytes to read.
    :return: Bytes read from pipe.
    :raises OSError: If read fails.
    """
    fd = None
    try:
        fd = open_pipe_read(pipe_path, non_blocking=False)
        return os.read(fd, size)
    finally:
        if fd is not None:
            os.close(fd)
