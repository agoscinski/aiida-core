"""Tests for process communication helpers."""

import asyncio
from unittest.mock import Mock

import pytest

from aiida.brokers import communicator as broker_communicator
from aiida.brokers import futures as broker_futures
from aiida.common.processes import ProcessState
from aiida.engine.processes.communications import LocalProcessController, RemoteProcessThreadController
from tests.utils import processes as test_processes


async def reach_waiting_state(process):
    """Wait for a process to enter the waiting state."""
    while process.state != ProcessState.WAITING:
        await asyncio.sleep(0.01)


def test_local_process_controller_kills_process(runner):
    """The local controller should kill a process running on its event loop."""
    process = runner.instantiate_process(test_processes.WaitProcess)
    controller = LocalProcessController(process, runner.loop)

    async def kill_process():
        task = asyncio.create_task(process.step_until_terminated())
        await asyncio.wait_for(reach_waiting_state(process), timeout=5)

        assert await controller.kill_process(process.pid, msg_text='Stopped by local controller') is True
        await asyncio.wait_for(task, timeout=5)

    runner.loop.run_until_complete(kill_process())

    assert process.node.is_killed
    assert process.node.process_status == 'Stopped by local controller'


def test_local_process_controller_pauses_and_plays_process(runner):
    """The local controller should pause and resume its process on its event loop."""
    process = runner.instantiate_process(test_processes.WaitProcess)
    controller = LocalProcessController(process, runner.loop)

    assert runner.loop.run_until_complete(controller.pause_process(process.pid)) is True
    assert process.paused

    assert runner.loop.run_until_complete(controller.play_process(process.pid)) is True
    assert not process.paused


def test_local_process_controller_rejects_unknown_process():
    """The local controller should only control its process."""
    loop = asyncio.new_event_loop()
    process = Mock(pid=1)
    controller = LocalProcessController(process, loop)

    try:
        with pytest.raises(ValueError, match='is not controlled by this controller'):
            loop.run_until_complete(controller.kill_process(process.pid + 1))
    finally:
        loop.close()


def test_local_process_controller_requires_own_event_loop():
    """The local controller should reject calls from another event loop."""
    loop = asyncio.new_event_loop()
    process = Mock(pid=1)
    controller = LocalProcessController(process, loop)

    try:
        with pytest.raises(RuntimeError, match='must be called from its event loop'):
            asyncio.run(controller.kill_process(process.pid))
    finally:
        loop.close()


def test_execute_process_no_reply():
    """Test executing a process without a reply resolves the returned future."""
    create_future = broker_futures.Future()
    create_future.set_result(1)

    communicator = Mock(spec=broker_communicator.Communicator)
    communicator.task_send.side_effect = [create_future, None]

    loader = Mock()
    loader.identify_object.return_value = 'tests:DummyProcess'

    controller = RemoteProcessThreadController(communicator)
    execute_future = controller.execute_process(object, loader=loader, no_reply=True)

    assert execute_future.result() is None
    assert communicator.task_send.call_count == 2
    assert communicator.task_send.call_args_list[1].kwargs == {'no_reply': True}
