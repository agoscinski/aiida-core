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


class PipeCommunicator(communications.CommunicatorHelper):
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
        super().__init__()

        self._profile_name = profile_name
        self._config_path = Path(config_path)
        self._worker_id = worker_id or f'worker_{uuid.uuid4().hex[:8]}'
        self._encoder = encoder
        self._decoder = decoder

        # Pipe directory
        self._pipe_dir = utils.get_pipe_dir(profile_name)

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

        # Coordinator discovery cache
        self._coordinator_info: discovery.ProcessInfo | None = None

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

    def _discover_coordinator(self) -> discovery.ProcessInfo:
        """Discover the coordinator process.

        :return: Coordinator process information.
        :raises ConnectionError: If coordinator is not found.
        """
        if self._coordinator_info is None or not discovery._is_process_alive(self._coordinator_info['pid']):
            self._coordinator_info = discovery.discover_coordinator(self._config_path)

        if self._coordinator_info is None:
            raise ConnectionError('Coordinator not found. Make sure the coordinator is running.')

        return self._coordinator_info

    def _send_message(self, pipe_path: str, message: dict) -> None:
        """Send a message to a pipe.

        :param pipe_path: Path to the target pipe.
        :param message: Message dictionary to send.
        :raises BrokenPipeError: If pipe has no reader.
        """
        data = messages.serialize(message, encoder=self._encoder)
        utils.write_to_pipe(pipe_path, data, non_blocking=False)

    def close(self) -> None:
        """Close the communicator and clean up resources."""
        if self.is_closed():
            return

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
            discovery.unregister_worker(self._config_path, self._worker_id)
        except Exception as exc:
            LOGGER.warning(f'Error unregistering worker: {exc}')

        # Cancel all pending futures
        with self._futures_lock:
            for future in self._pending_futures.values():
                if not future.done():
                    future.set_exception(kiwipy.CommunicatorClosed('Communicator closed'))
            self._pending_futures.clear()

        super().close()

    def task_send(self, task, no_reply=False) -> futures.Future | None:
        """Send a task message to the coordinator.

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

        # Discover coordinator and send message
        coordinator = self._discover_coordinator()
        self._send_message(coordinator['task_pipe'], message)

        if no_reply:
            return None

        # Create and register future
        future = futures.Future()
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
        workers = discovery.discover_workers(self._config_path)
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

        # Create and register future
        future = futures.Future()
        with self._futures_lock:
            self._pending_futures[correlation_id] = future

        return future

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None) -> bool:
        """Broadcast a message to all subscribers via coordinator.

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

        # Send to coordinator for distribution
        coordinator = self._discover_coordinator()
        self._send_message(coordinator['broadcast_pipe'], message)

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
        # Use parent class to register subscriber
        identifier = super().add_task_subscriber(subscriber, identifier)

        # Create task pipe if first subscriber
        if len(self._task_subscribers) == 1:
            self._init_task_pipe()

        return identifier

    def _init_task_pipe(self) -> None:
        """Initialize the task pipe for receiving task messages."""
        utils.create_pipe(self._task_pipe_path)
        self._task_fd = utils.open_pipe_read(self._task_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._task_fd, selectors.EVENT_READ, self._handle_task)

        # Register worker in discovery
        discovery.register_worker(
            self._config_path,
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
            except Exception as exc:
                LOGGER.exception(f'Error sending task reply: {exc}')

        result_future.add_done_callback(send_reply)

    def add_rpc_subscriber(self, subscriber: communications.RpcSubscriber, identifier=None) -> t.Any:
        """Add an RPC subscriber.

        :param subscriber: The RPC callback function.
        :param identifier: Optional subscriber identifier.
        :return: Subscriber identifier.
        """
        # Use parent class to register subscriber
        identifier = super().add_rpc_subscriber(subscriber, identifier)

        # Create RPC pipe if first subscriber
        if len(self._rpc_subscribers) == 1:
            self._init_rpc_pipe()

        return identifier

    def _init_rpc_pipe(self) -> None:
        """Initialize the RPC pipe for receiving RPC messages."""
        utils.create_pipe(self._rpc_pipe_path)
        self._rpc_fd = utils.open_pipe_read(self._rpc_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._rpc_fd, selectors.EVENT_READ, self._handle_rpc)

        # Update discovery registration
        discovery.register_worker(
            self._config_path,
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
            except Exception as exc:
                LOGGER.exception(f'Error sending RPC reply: {exc}')

        result_future.add_done_callback(send_reply)

    def add_broadcast_subscriber(self, subscriber: communications.BroadcastSubscriber, identifier=None) -> t.Any:
        """Add a broadcast subscriber.

        :param subscriber: The broadcast callback function.
        :param identifier: Optional subscriber identifier.
        :return: Subscriber identifier.
        """
        # Use parent class to register subscriber
        identifier = super().add_broadcast_subscriber(subscriber, identifier)

        # Create broadcast pipe if first subscriber
        if len(self._broadcast_subscribers) == 1:
            self._init_broadcast_pipe()

        return identifier

    def _init_broadcast_pipe(self) -> None:
        """Initialize the broadcast pipe for receiving broadcast messages."""
        utils.create_pipe(self._broadcast_pipe_path)
        self._broadcast_fd = utils.open_pipe_read(self._broadcast_pipe_path, non_blocking=True)

        with self._selector_lock:
            self._selector.register(self._broadcast_fd, selectors.EVENT_READ, self._handle_broadcast)

        # Update discovery registration
        discovery.register_worker(
            self._config_path,
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
