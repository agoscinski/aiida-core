"""Named pipe-based implementation of kiwipy.Communicator interface."""

from __future__ import annotations

import atexit
import logging
import os
import selectors
import threading
import typing as t
import uuid
from pathlib import Path

import kiwipy
from kiwipy import communications, futures

from . import discovery, messages, utils

__all__ = ('PipeCommunicator',)

LOGGER = logging.getLogger(__name__)


class PipeCommunicator(kiwipy.Communicator):
    """Named pipe-based communicator using selectors for event-driven I/O.

    This implementation uses named pipes for inter-process communication with
    a selector-based event loop for non-blocking message handling.
    """

    def __init__(
        self,
        profile_name: str,
        config_path: Path | str,
        worker_id: str | None = None,
        encoder: t.Callable | None = None,
        decoder: t.Callable | None = None,
    ):
        """Initialize the pipe communicator.

        :param profile_name: The AiiDA profile name.
        :param config_path: Path to the AiiDA config directory.
        :param worker_id: Optional worker identifier (auto-generated if not provided).
        :param encoder: Optional custom message encoder.
        :param decoder: Optional custom message decoder.
        """
        # Initialize subscriber dictionaries
        self._task_subscribers: dict[str, communications.TaskSubscriber] = {}
        self._rpc_subscribers: dict[str, communications.RpcSubscriber] = {}
        self._broadcast_subscribers: dict[str, communications.BroadcastSubscriber] = {}
        self._closed = False

        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._worker_id = worker_id or f'worker_{uuid.uuid4().hex[:8]}'
        self._encoder = encoder
        self._decoder = decoder

        # Worker pipes directory
        self._pipe_dir = utils.get_worker_pipes_dir(profile_name)

        # Reply pipe for receiving responses
        self._reply_pipe_path = self._pipe_dir / f'reply_{os.getpid()}_{uuid.uuid4().hex}'
        self._reply_fd: int | None = None

        # Worker pipes (created when adding subscribers)
        self._task_pipe_path = self._pipe_dir / f'task_{self._worker_id}'
        self._task_fd: int | None = None

        self._rpc_pipe_path = self._pipe_dir / f'rpc_{self._worker_id}'
        self._rpc_fd: int | None = None

        self._broadcast_pipe_path = self._pipe_dir / f'broadcast_{self._worker_id}'
        self._broadcast_fd: int | None = None

        # Selector for event-driven I/O
        self._selector = selectors.DefaultSelector()
        self._selector_thread: threading.Thread | None = None
        self._selector_running = False
        self._selector_lock = threading.Lock()

        # Pending futures (correlation_id -> Future)
        self._pending_futures: dict[str, futures.Future] = {}
        self._futures_lock = threading.Lock()

        # Broker discovery cache
        self._broker_info: discovery.ProcessInfo | None = None

        # Initialize reply pipe
        self._init_reply_pipe()

        # Start selector thread
        self._start_selector_thread()

        # Register cleanup
        atexit.register(self.close)

    def _init_reply_pipe(self) -> None:
        """Initialize the reply pipe for receiving responses."""
        utils.create_pipe(self._reply_pipe_path)
        self._reply_fd = utils.open_pipe_read(self._reply_pipe_path, non_blocking=True)
        self._selector.register(self._reply_fd, selectors.EVENT_READ, self._handle_reply)

    def _start_selector_thread(self) -> None:
        """Start the background selector thread."""
        self._selector_running = True
        self._selector_thread = threading.Thread(target=self._selector_loop, daemon=True)
        self._selector_thread.start()

    def _selector_loop(self) -> None:
        """Main selector event loop (runs in background thread)."""
        while self._selector_running:
            try:
                with self._selector_lock:
                    if not self._selector_running:
                        break
                    events = self._selector.select(timeout=0.1)

                for key, mask in events:
                    callback = key.data
                    if callback:
                        try:
                            callback(key.fileobj)
                        except Exception as exc:
                            LOGGER.exception(f'Error in selector callback: {exc}')

            except Exception as exc:
                if self._selector_running:
                    LOGGER.exception(f'Error in selector loop: {exc}')

    def _handle_reply(self, fd: int) -> None:
        """Handle incoming reply message.

        :param fd: File descriptor of the reply pipe.
        """
        try:
            response = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if response is None:
                return

            correlation_id = response.get('correlation_id')
            if not correlation_id:
                LOGGER.warning('Received reply without correlation_id')
                return

            with self._futures_lock:
                future = self._pending_futures.pop(correlation_id, None)

            if future is None:
                LOGGER.warning(f'No pending future for correlation_id: {correlation_id}')
                return

            # Set future result or exception
            if 'error' in response:
                error_info = response['error']
                # Reconstruct exception
                exception = kiwipy.RemoteException(error_info.get('message', 'Unknown error'))
                future.set_exception(exception)
            else:
                future.set_result(response.get('result'))

        except Exception as exc:
            LOGGER.exception(f'Error handling reply: {exc}')

    def _cleanup_future(self, correlation_id: str) -> None:
        """Remove a future from pending futures (called on future completion/cancellation).

        :param correlation_id: The correlation ID of the future to remove.
        """
        with self._futures_lock:
            self._pending_futures.pop(correlation_id, None)

    def _discover_broker(self) -> discovery.ProcessInfo:
        """Discover the broker process.

        :return: Broker process information.
        :raises ConnectionError: If broker is not found.
        """
        if self._broker_info is None or not discovery._is_process_alive(self._broker_info['pid']):
            self._broker_info = discovery.discover_broker(self._profile_name)

        if self._broker_info is None:
            raise ConnectionError('Broker not found. Make sure the broker is running.')

        return self._broker_info

    def _send_message(self, pipe_path: str, message: dict, use_lock: bool = False) -> None:
        """Send a message to a pipe.

        :param pipe_path: Path to the target pipe.
        :param message: Message dictionary to send.
        :param use_lock: If True, acquire exclusive lock (for multi-writer pipes like broker pipes).
        :raises BrokenPipeError: If pipe has no reader.
        """
        data = messages.serialize(message, encoder=self._encoder)
        utils.write_to_pipe(pipe_path, data, non_blocking=False, use_lock=use_lock)

    def is_closed(self) -> bool:
        """Check if the communicator is closed.

        :return: True if closed, False otherwise.
        """
        return self._closed

    def _ensure_open(self) -> None:
        """Ensure the communicator is open.

        :raises kiwipy.CommunicatorClosed: If the communicator is closed.
        """
        if self._closed:
            raise kiwipy.CommunicatorClosed('Communicator is closed')

    def close(self) -> None:
        """Close the communicator and clean up resources."""
        if self.is_closed():
            return

        # Mark as closed first
        self._closed = True

        # Stop selector thread
        self._selector_running = False
        if self._selector_thread and self._selector_thread.is_alive():
            self._selector_thread.join(timeout=1.0)

        # Close and unregister all file descriptors
        with self._selector_lock:
            try:
                self._selector.close()
            except Exception as exc:
                LOGGER.warning(f'Error closing selector: {exc}')

        # Close file descriptors
        for fd in [self._reply_fd, self._task_fd, self._rpc_fd, self._broadcast_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

        # Clean up pipes
        for pipe_path in [self._reply_pipe_path, self._task_pipe_path, self._rpc_pipe_path, self._broadcast_pipe_path]:
            utils.cleanup_pipe(pipe_path)

        # Unregister from discovery
        try:
            discovery.unregister_worker(self._profile_name, self._worker_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering worker: {exc}')

        # Cancel all pending futures
        with self._futures_lock:
            for future in self._pending_futures.values():
                if not future.done():
                    future.set_exception(kiwipy.CommunicatorClosed('Communicator closed'))
            self._pending_futures.clear()

        # Clear all subscribers
        self._task_subscribers.clear()
        self._rpc_subscribers.clear()
        self._broadcast_subscribers.clear()

    def task_send(self, task, no_reply=False) -> futures.Future | None:
        """Send a task message to the broker.

        :param task: The task message.
        :param no_reply: If True, do not expect a reply.
        :return: Future representing the task result, or None if no_reply=True.
        """
        self._ensure_open()

        correlation_id = str(uuid.uuid4())
        message = {
            'type': 'task',
            'correlation_id': correlation_id,
            'reply_pipe': str(self._reply_pipe_path) if not no_reply else None,
            'body': task,
        }

        # Discover broker and send message (use lock for multi-writer broker pipe)
        broker = self._discover_broker()
        self._send_message(broker['task_pipe'], message, use_lock=True)

        if no_reply:
            return None

        # Create and register future with auto-cleanup callback
        future = kiwipy.Future()
        future.add_done_callback(lambda _: self._cleanup_future(correlation_id))
        with self._futures_lock:
            self._pending_futures[correlation_id] = future

        return future

    def rpc_send(self, recipient_id, msg):
        """Initiate a remote procedure call on a recipient.

        :param recipient_id: The recipient identifier.
        :param msg: The message body.
        :return: Future representing the RPC result.
        """
        self._ensure_open()

        # Discover recipient's RPC pipe
        workers = discovery.discover_workers(self._profile_name)
        recipient = next((w for w in workers if w['process_id'] == recipient_id), None)

        if recipient is None:
            raise kiwipy.UnroutableError(f'Unknown RPC recipient: {recipient_id}')

        correlation_id = str(uuid.uuid4())
        message = {
            'type': 'rpc',
            'correlation_id': correlation_id,
            'reply_pipe': str(self._reply_pipe_path),
            'body': msg,
        }

        self._send_message(recipient['rpc_pipe'], message)

        # Create and register future with auto-cleanup callback
        future = futures.Future()
        future.add_done_callback(lambda _: self._cleanup_future(correlation_id))
        with self._futures_lock:
            self._pending_futures[correlation_id] = future

        return future

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None) -> bool:
        """Broadcast a message to all subscribers via broker.

        :param body: The message body.
        :param sender: Optional sender identifier.
        :param subject: Optional subject/topic.
        :param correlation_id: Optional correlation ID.
        :return: True if message was sent.
        """
        self._ensure_open()

        message = {
            'type': 'broadcast',
            'body': body,
            'sender': sender,
            'subject': subject,
            'correlation_id': correlation_id or str(uuid.uuid4()),
        }

        # Send to broker for distribution (use lock for multi-writer broker pipe)
        broker = self._discover_broker()
        self._send_message(broker['broadcast_pipe'], message, use_lock=True)

        return True

    def fire_broadcast(self, body, sender=None, subject=None, correlation_id=None):
        """Fire broadcast to all subscribers.

        Override parent implementation to call subscribers with positional arguments
        instead of keyword arguments, matching RabbitMQ communicator behavior.

        :param body: The message body.
        :param sender: Optional sender identifier.
        :param subject: Optional subject/topic.
        :param correlation_id: Optional correlation ID.
        :return: True if broadcast was fired.
        """
        self._ensure_open()
        for subscriber in self._broadcast_subscribers.values():
            # Call with positional arguments like RMQ does
            subscriber(self, body, sender, subject, correlation_id)
        return True

    def add_task_subscriber(self, subscriber: communications.TaskSubscriber, identifier=None) -> t.Any:
        """Add a task subscriber (worker becomes a task processor).

        :param subscriber: The task callback function.
        :param identifier: Optional subscriber identifier.
        :return: Subscriber identifier.
        """
        self._ensure_open()

        # Generate identifier if not provided
        if identifier is None:
            identifier = str(uuid.uuid4())

        # Register subscriber
        self._task_subscribers[identifier] = subscriber

        # Create task pipe if first subscriber
        if len(self._task_subscribers) == 1:
            self._init_task_pipe()

        return identifier

    def remove_task_subscriber(self, identifier) -> None:
        """Remove a task subscriber.

        :param identifier: The subscriber identifier to remove.
        """
        self._task_subscribers.pop(identifier, None)

    def fire_task(self, msg, no_reply=False) -> futures.Future | None:
        """Fire a task to all task subscribers.

        :param msg: The task message.
        :param no_reply: If True, do not return a future.
        :return: Future with the result, or None if no_reply=True.
        """
        self._ensure_open()

        # Task subscribers should process the message and return a result
        # For simplicity, we call the first subscriber (typically there's only one per worker)
        if not self._task_subscribers:
            raise kiwipy.UnroutableError('No task subscribers registered')

        subscriber = next(iter(self._task_subscribers.values()))

        # Call subscriber and wrap result in a future
        try:
            result = subscriber(self, msg)

            if no_reply:
                return None

            # If subscriber returns a future, return it directly
            if isinstance(result, futures.Future):
                return result

            # Otherwise, wrap result in a completed future
            future = futures.Future()
            future.set_result(result)
            return future

        except Exception as exc:
            if no_reply:
                return None
            future = futures.Future()
            future.set_exception(exc)
            return future

    def _init_task_pipe(self) -> None:
        """Initialize the task pipe for receiving task messages."""
        utils.create_pipe(self._task_pipe_path)
        self._task_fd = utils.open_pipe_read(self._task_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._task_fd, selectors.EVENT_READ, self._handle_task)

        # Register worker in discovery
        discovery.register_worker(
            self._profile_name,
            self._worker_id,
            task_pipe=str(self._task_pipe_path),
            rpc_pipe=str(self._rpc_pipe_path),
            broadcast_pipe=str(self._broadcast_pipe_path),
            reply_pipe=str(self._reply_pipe_path),
        )

    def _handle_task(self, fd: int) -> None:
        """Handle incoming task message.

        :param fd: File descriptor of the task pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Execute task using parent class method
            result_future = self.fire_task(message.get('body'), no_reply=message.get('reply_pipe') is None)

            # Send reply if requested
            reply_pipe = message.get('reply_pipe')
            if reply_pipe and result_future:
                self._send_task_reply(reply_pipe, message['correlation_id'], result_future)

        except Exception as exc:
            LOGGER.exception(f'Error handling task: {exc}')

    def _send_task_reply(self, reply_pipe: str, correlation_id: str, result_future: futures.Future) -> None:
        """Send task result back to requester.

        :param reply_pipe: Path to the reply pipe.
        :param correlation_id: Correlation ID from the request.
        :param result_future: Future containing the task result.
        """

        def send_reply(_):
            try:
                if result_future.exception():
                    exc = result_future.exception()
                    response = {
                        'correlation_id': correlation_id,
                        'error': {'message': str(exc), 'type': type(exc).__name__},
                    }
                else:
                    response = {'correlation_id': correlation_id, 'result': result_future.result()}

                self._send_message(reply_pipe, response)
            except (BrokenPipeError, FileNotFoundError, OSError) as exc:
                # Client's reply pipe is gone (likely the client died) - this is expected
                LOGGER.debug(f'Reply pipe unavailable ({type(exc).__name__}), client may have disconnected: {reply_pipe}')
            except Exception as exc:
                LOGGER.exception(f'Error sending task reply: {exc}')

        result_future.add_done_callback(send_reply)

    def add_rpc_subscriber(self, subscriber: communications.RpcSubscriber, identifier=None) -> t.Any:
        """Add an RPC subscriber.

        :param subscriber: The RPC callback function.
        :param identifier: Optional subscriber identifier.
        :return: Subscriber identifier.
        """
        self._ensure_open()

        # Generate identifier if not provided
        if identifier is None:
            identifier = str(uuid.uuid4())

        # Register subscriber
        self._rpc_subscribers[identifier] = subscriber

        # Create RPC pipe if first subscriber
        if len(self._rpc_subscribers) == 1:
            self._init_rpc_pipe()

        return identifier

    def remove_rpc_subscriber(self, identifier) -> None:
        """Remove an RPC subscriber.

        :param identifier: The subscriber identifier to remove.
        """
        self._rpc_subscribers.pop(identifier, None)

    def fire_rpc(self, recipient_id, msg) -> futures.Future:
        """Fire an RPC call to the appropriate subscriber.

        :param recipient_id: The recipient identifier (used to route to correct subscriber).
        :param msg: The RPC message.
        :return: Future with the result.
        """
        self._ensure_open()

        # RPC subscribers should process the message and return a result
        # For direct pipe communication, we typically have one RPC subscriber per worker
        if not self._rpc_subscribers:
            raise kiwipy.UnroutableError(f'No RPC subscribers registered for recipient: {recipient_id}')

        subscriber = next(iter(self._rpc_subscribers.values()))

        # Call subscriber and wrap result in a future
        try:
            result = subscriber(self, msg)

            # If subscriber returns a future, return it directly
            if isinstance(result, futures.Future):
                return result

            # Otherwise, wrap result in a completed future
            future = futures.Future()
            future.set_result(result)
            return future

        except Exception as exc:
            future = futures.Future()
            future.set_exception(exc)
            return future

    def _init_rpc_pipe(self) -> None:
        """Initialize the RPC pipe for receiving RPC messages."""
        utils.create_pipe(self._rpc_pipe_path)
        self._rpc_fd = utils.open_pipe_read(self._rpc_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._rpc_fd, selectors.EVENT_READ, self._handle_rpc)

        # Update discovery registration
        discovery.register_worker(
            self._profile_name,
            self._worker_id,
            task_pipe=str(self._task_pipe_path),
            rpc_pipe=str(self._rpc_pipe_path),
            broadcast_pipe=str(self._broadcast_pipe_path),
            reply_pipe=str(self._reply_pipe_path),
        )

    def _handle_rpc(self, fd: int) -> None:
        """Handle incoming RPC message.

        :param fd: File descriptor of the RPC pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Execute RPC using parent class method (gets the right subscriber)
            # Note: RPC messages in this implementation don't specify recipient in the message,
            # because they're sent directly to the recipient's pipe
            result_future = self.fire_rpc(self._worker_id, message.get('body'))

            # Send reply
            reply_pipe = message.get('reply_pipe')
            if reply_pipe:
                self._send_rpc_reply(reply_pipe, message['correlation_id'], result_future)

        except Exception as exc:
            LOGGER.exception(f'Error handling RPC: {exc}')

    def _send_rpc_reply(self, reply_pipe: str, correlation_id: str, result_future: futures.Future) -> None:
        """Send RPC result back to requester.

        :param reply_pipe: Path to the reply pipe.
        :param correlation_id: Correlation ID from the request.
        :param result_future: Future containing the RPC result.
        """

        def send_reply(_):
            try:
                if result_future.exception():
                    exc = result_future.exception()
                    response = {
                        'correlation_id': correlation_id,
                        'error': {'message': str(exc), 'type': type(exc).__name__},
                    }
                else:
                    response = {'correlation_id': correlation_id, 'result': result_future.result()}

                self._send_message(reply_pipe, response)
            except (BrokenPipeError, FileNotFoundError, OSError) as exc:
                # Client's reply pipe is gone (likely the client died) - this is expected
                LOGGER.debug(f'Reply pipe unavailable ({type(exc).__name__}), client may have disconnected: {reply_pipe}')
            except Exception as exc:
                LOGGER.exception(f'Error sending RPC reply: {exc}')

        result_future.add_done_callback(send_reply)

    def add_broadcast_subscriber(self, subscriber: communications.BroadcastSubscriber, identifier=None) -> t.Any:
        """Add a broadcast subscriber.

        :param subscriber: The broadcast callback function.
        :param identifier: Optional subscriber identifier.
        :return: Subscriber identifier.
        """
        self._ensure_open()

        # Generate identifier if not provided
        if identifier is None:
            identifier = str(uuid.uuid4())

        # Register subscriber
        self._broadcast_subscribers[identifier] = subscriber

        # Create broadcast pipe if first subscriber
        if len(self._broadcast_subscribers) == 1:
            self._init_broadcast_pipe()

        return identifier

    def remove_broadcast_subscriber(self, identifier) -> None:
        """Remove a broadcast subscriber.

        :param identifier: The subscriber identifier to remove.
        """
        self._broadcast_subscribers.pop(identifier, None)

    def _init_broadcast_pipe(self) -> None:
        """Initialize the broadcast pipe for receiving broadcast messages."""
        utils.create_pipe(self._broadcast_pipe_path)
        self._broadcast_fd = utils.open_pipe_read(self._broadcast_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._broadcast_fd, selectors.EVENT_READ, self._handle_broadcast)

        # Update discovery registration
        discovery.register_worker(
            self._profile_name,
            self._worker_id,
            task_pipe=str(self._task_pipe_path),
            rpc_pipe=str(self._rpc_pipe_path),
            broadcast_pipe=str(self._broadcast_pipe_path),
            reply_pipe=str(self._reply_pipe_path),
        )

    def _handle_broadcast(self, fd: int) -> None:
        """Handle incoming broadcast message.

        :param fd: File descriptor of the broadcast pipe.
        """
        try:
            message = messages.deserialize_from_fd(fd, decoder=self._decoder)
            if message is None:
                return

            # Fire broadcast to all subscribers using parent class method
            self.fire_broadcast(
                message.get('body'),
                sender=message.get('sender'),
                subject=message.get('subject'),
                correlation_id=message.get('correlation_id'),
            )

        except Exception as exc:
            LOGGER.exception(f'Error handling broadcast: {exc}')
