"""Message serialization and framing utilities for named pipe communication."""

from __future__ import annotations

import json
import struct
import typing as t

__all__ = ('serialize', 'deserialize', 'deserialize_from_fd')


def serialize(message: dict, encoder: t.Callable | None = None) -> bytes:
    """Serialize a message with length-prefixed framing.

    Frame format: [4 bytes: message length (big-endian)][message data]

    :param message: The message dictionary to serialize.
    :param encoder: Optional custom encoder function. If not provided, uses JSON.
    :return: Framed message bytes ready to write to a pipe.
    """
    if encoder is not None:
        data = encoder(message)
        if isinstance(data, str):
            data = data.encode('utf-8')
    else:
        data = json.dumps(message).encode('utf-8')

    # Pack length as 4-byte big-endian unsigned integer
    length = struct.pack('!I', len(data))
    return length + data


def deserialize(data: bytes, decoder: t.Callable | None = None) -> dict:
    """Deserialize a framed message.

    :param data: Raw bytes from pipe (including length prefix).
    :param decoder: Optional custom decoder function. If not provided, uses JSON.
    :return: Deserialized message dictionary.
    :raises ValueError: If message is incomplete or malformed.
    """
    if len(data) < 4:
        raise ValueError('Incomplete message: missing length prefix')

    # Unpack length
    length = struct.unpack('!I', data[:4])[0]

    if len(data) < 4 + length:
        raise ValueError(f'Incomplete message: expected {length} bytes, got {len(data) - 4}')

    message_data = data[4 : 4 + length]

    if decoder is not None:
        return decoder(message_data)

    return json.loads(message_data.decode('utf-8'))


def deserialize_from_fd(fd: int, decoder: t.Callable | None = None, timeout: float = 30.0) -> dict | None:
    """Read and deserialize a framed message from a file descriptor.

    :param fd: File descriptor to read from (should be non-blocking).
    :param decoder: Optional custom decoder function. If not provided, uses JSON.
    :param timeout: Timeout in seconds for waiting on partial data.
    :return: Deserialized message dictionary, or None if no complete message available.
    :raises ValueError: If message is malformed or timeout waiting for data.
    :raises OSError: If read operation fails.
    """
    import os
    import select

    # Read 4-byte length prefix
    length_bytes = b''
    while len(length_bytes) < 4:
        try:
            chunk = os.read(fd, 4 - len(length_bytes))
            if not chunk:
                # EOF or no data available
                if length_bytes:
                    raise ValueError('Incomplete message: EOF while reading length prefix')
                return None
            length_bytes += chunk
        except BlockingIOError:
            # No data available on non-blocking fd
            if length_bytes:
                # Partial length prefix read - wait for more data
                ready, _, _ = select.select([fd], [], [], timeout)
                if not ready:
                    raise ValueError('Incomplete message: timeout reading length prefix')
                continue
            return None

    length = struct.unpack('!I', length_bytes)[0]

    # Validate length
    if length == 0:
        raise ValueError('Invalid zero-length message')
    if length > 100 * 1024 * 1024:  # 100 MB max
        raise ValueError(f'Message too large: {length} bytes')

    # Read message data - wait for complete message since we know the length
    data = b''
    while len(data) < length:
        try:
            chunk = os.read(fd, length - len(data))
            if not chunk:
                raise ValueError('Incomplete message: EOF while reading data')
            data += chunk
        except BlockingIOError:
            # Wait for more data to arrive
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                raise ValueError(f'Incomplete message: timeout waiting for data ({len(data)}/{length} bytes)')
            continue

    if decoder is not None:
        return decoder(data)

    return json.loads(data.decode('utf-8'))
