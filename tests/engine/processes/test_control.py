"""Tests for the :mod:`aiida.engine.processes.control` module."""

import pytest
from plumpy.process_comms import RemoteProcessThreadController

from aiida.engine import ProcessState
from aiida.engine.launch import submit
from aiida.engine.processes import control
from aiida.orm import Int
from tests.utils.processes import DummyProcess, WaitProcess


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
@pytest.mark.parametrize('action', (control.pause_processes, control.play_processes, control.kill_processes))
def test_processes_all_exclusivity(submit_and_await, action):
    """Test that control methods raise if both ``processes`` is specified and ``all_entries=True``."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)
    assert not node.paused

    with pytest.raises(ValueError, match='cannot specify processes when `all_entries = True`.'):
        action([node], all_entries=True)


@pytest.mark.usefixtures('aiida_profile_clean', 'stopped_daemon_client')
@pytest.mark.parametrize('action', (control.pause_processes, control.play_processes, control.kill_processes))
def test_daemon_not_running(action, caplog):
    """Test that control methods warns if the daemon is not running."""
    action(all_entries=True)
    assert 'The daemon is not running' in caplog.records[0].message


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_pause_processes(submit_and_await):
    """Test :func:`aiida.engine.processes.control.pause_processes`."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)
    assert not node.paused

    control.pause_processes([node], timeout=float('inf'))
    assert node.paused
    assert node.process_status == 'Paused through `aiida.engine.processes.control.pause_processes`'


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_pause_processes_all_entries(submit_and_await):
    """Test :func:`aiida.engine.processes.control.pause_processes` with ``all_entries=True``."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)
    assert not node.paused

    control.pause_processes(all_entries=True, timeout=float('inf'))
    assert node.paused


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_play_processes(submit_and_await):
    """Test :func:`aiida.engine.processes.control.play_processes`."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)
    assert not node.paused

    control.pause_processes([node], timeout=float('inf'))
    assert node.paused

    control.play_processes([node], timeout=float('inf'))
    assert not node.paused


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_play_processes_all_entries(submit_and_await):
    """Test :func:`aiida.engine.processes.control.play_processes` with ``all_entries=True``."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)
    assert not node.paused

    control.pause_processes([node], timeout=float('inf'))
    assert node.paused

    control.play_processes(all_entries=True, timeout=float('inf'))
    assert not node.paused


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_kill_processes(submit_and_await):
    """Test :func:`aiida.engine.processes.control.kill_processes`."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)

    control.kill_processes([node], timeout=float('inf'))
    assert node.is_terminated
    assert node.is_killed
    assert node.process_status == 'Killed through `aiida.engine.processes.control.kill_processes`'


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_kill_processes_all_entries(submit_and_await):
    """Test :func:`aiida.engine.processes.control.kill_processes` with ``all_entries=True``."""
    node = submit_and_await(WaitProcess, ProcessState.WAITING)

    control.kill_processes(all_entries=True, timeout=float('inf'))
    assert node.is_terminated
    assert node.is_killed


@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_revive(monkeypatch, aiida_code_installed, submit_and_await):
    """Test :func:`aiida.engine.processes.control.revive_processes`."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    builder = code.get_builder()
    builder.x = Int(1)
    builder.y = Int(1)
    builder.metadata.options.resources = {'num_machines': 1}

    # Temporarily patch the ``RemoteProcessThreadController.continue_process`` method to do nothing and just return a
    # completed future. This ensures that the submission creates a process node in the database but no task is sent to
    # RabbitMQ and so the daemon will not start running it.
    with monkeypatch.context() as context:
        context.setattr(RemoteProcessThreadController, 'continue_process', lambda *args, **kwargs: None)
        node = submit(builder)

    # The node should now be created in the database but "stuck"
    assert node.process_state == ProcessState.CREATED

    # Time to revive it by recreating the task and send it to RabbitMQ
    control.revive_processes([node])

    # Wait for the process to be picked up by the daemon and completed. If there is a problem with the code, this call
    # should timeout and raise an exception
    submit_and_await(node)
    assert node.is_finished_ok


@pytest.mark.requires_rmq
@pytest.mark.usefixtures('aiida_profile_clean', 'started_daemon_client')
def test_pause_releases_task_slot(aiida_profile, rabbitmq_client, submit_and_await):
    """Test that when a process is paused, it releases the RabbitMQ task slot.

    This test verifies the pause-release-task feature:
    1. Submit a WaitProcess that blocks a task slot while waiting
    2. Verify the slot is blocked (messages_unacknowledged = 1)
    3. Pause the process
    4. Verify the slot is released (messages_unacknowledged = 0)
    5. Submit another process and verify it can run

    Uses the RabbitMQ Management API to verify unacknowledged message counts.
    """
    from aiida.brokers.rabbitmq.utils import get_launch_queue_name

    # Get queue name
    prefix = f'aiida-{aiida_profile.uuid}'
    queue_name = get_launch_queue_name(prefix)

    # Check initial state - queue should be empty
    initial_stats = rabbitmq_client.get_queue_stats(queue_name)
    if initial_stats:
        assert initial_stats.messages_unacknowledged == 0, 'Queue should start with no unacknowledged messages'

    # Submit WaitProcess - it will block a task slot while waiting
    node1 = submit(WaitProcess)
    submit_and_await(node1, ProcessState.WAITING)
    assert not node1.paused, 'Process should not be paused yet'

    # Verify the slot is blocked (task is unacknowledged while process is waiting)
    stats_while_waiting = rabbitmq_client.get_queue_stats(queue_name)
    if stats_while_waiting:
        assert stats_while_waiting.messages_unacknowledged == 1, (
            'While waiting, task should be unacknowledged (slot blocked)'
        )

    # Pause the process - this should release the task slot
    control.pause_processes([node1], timeout=float('inf'))
    assert node1.paused, 'Process should be paused'

    # Verify the slot is released after pause
    stats_after_pause = rabbitmq_client.get_queue_stats(queue_name)
    if stats_after_pause:
        assert stats_after_pause.messages_unacknowledged == 0, (
            'After pause, task should be acknowledged (slot released)'
        )

    # Submit a second process - with the slot released, it should be able to run
    node2 = submit(DummyProcess)
    submit_and_await(node2, ProcessState.FINISHED)
    assert node2.is_finished_ok, 'Second process should complete successfully'

    # Final check - queue should still have no unacknowledged messages
    final_stats = rabbitmq_client.get_queue_stats(queue_name)
    if final_stats:
        assert final_stats.messages_unacknowledged == 0, 'Queue should have no unacknowledged messages at end'
