"""Message serialization and framing utilities for named pipe communication.

Message Format
==============

Messages are dictionaries with the following standard fields:

    {
        'type': str,              # Message type: 'task', 'rpc', 'broadcast', etc.
        'correlation_id': str,    # Unique ID for request/response correlation
        'body': dict,             # The actual message payload (plumpy task data)
        'communication': dict,    # Transport-specific metadata (optional)
    }

The ``communication`` Field
---------------------------

The ``communication`` field contains implementation-specific information needed
by the communication layer. This separates transport concerns from business logic.

For named pipe communication::

    'communication': {
        'reply_pipe': str,    # Path to the reply pipe for responses
    }

Other communication implementations (e.g., RabbitMQ) may use different fields
or ignore this section entirely since AMQP handles reply routing internally.

This design allows:
- Plumpy/AiiDA to remain transport-agnostic
- Each transport to include its own routing metadata
- Clean separation between message content and delivery mechanism

Routing (``_routing``)
----------------------

The ``_routing`` field is used by the scheduler/broker to route messages::

    '_routing': {
        'target_worker': str,  # Worker ID to send the task to
    }

This is added by the ProcessScheduler before sending to workers.
"""

from __future__ import annotations

import json
import struct
import typing as t

__all__ = ('deserialize', 'deserialize_from_fd', 'serialize')


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


def deserialize_from_fd(fd: int, decoder: t.Callable | None = None) -> dict | None:
    """Read and deserialize a framed message from a file descriptor.

    Handles both blocking and non-blocking file descriptors. For non-blocking fds,
    returns None if no data is available (BlockingIOError).

    :param fd: File descriptor to read from.
    :param decoder: Optional custom decoder function. If not provided, uses JSON.
    :return: Deserialized message dictionary, or None if EOF or no data available.
    :raises ValueError: If message is malformed.
    :raises OSError: If read operation fails.
    """
    import os

    # Read 4-byte length prefix
    length_bytes = b''
    while len(length_bytes) < 4:
        try:
            chunk = os.read(fd, 4 - len(length_bytes))
        except BlockingIOError:
            # Non-blocking fd with no data available
            if length_bytes:
                raise ValueError('Incomplete message: no data while reading length prefix')
            return None
        if not chunk:
            if length_bytes:
                raise ValueError('Incomplete message: EOF while reading length prefix')
            return None
        length_bytes += chunk

    length = struct.unpack('!I', length_bytes)[0]

    # Validate length
    if length == 0:
        raise ValueError('Invalid zero-length message')
    if length > 100 * 1024 * 1024:  # 100 MB max
        raise ValueError(f'Message too large: {length} bytes')

    # Read message data
    data = b''
    while len(data) < length:
        try:
            chunk = os.read(fd, length - len(data))
        except BlockingIOError:
            raise ValueError(f'Incomplete message: no data while reading body ({len(data)}/{length} bytes)')
        if not chunk:
            raise ValueError('Incomplete message: EOF while reading data')
        data += chunk

    if decoder is not None:
        return decoder(data)

    return json.loads(data.decode('utf-8'))
