###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Atomic process transition persistence controlled by private engine policy."""

import asyncio
import io

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from aiida import orm
from aiida.common import exceptions
from aiida.common.links import LinkType
from aiida.common.processes import ProcessState
from aiida.engine import Process, WorkChain, run_get_node
from aiida.engine.processes.generic.process import Process as ProcessBase
from aiida.engine.processes.persistence import CheckpointContext, CheckpointPayload
from aiida.engine.processes.states import Wait
from aiida.engine.runners import Runner
from aiida.manage.caching import enable_caching
from aiida.storage.psql_dos.models.node import DbLink, DbNode
from tests.utils.processes import DummyProcess


class OutputCalculation(Process):
    """Emit an unstored output to exercise node insertion and CREATE links."""

    _node_class = orm.CalculationNode

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outputs.valid_type = orm.Data

    async def run(self):
        self.out('result', orm.Int(7))


@pytest.fixture
def enable_batching(runner, monkeypatch):
    """Enable the experimental engine policy without changing process inputs."""
    monkeypatch.setattr(runner, '_batch_transitions', True)


@pytest.fixture
def process(runner, enable_batching):
    """A process with a pending new output and a previously persisted state."""
    process = OutputCalculation(runner=runner)
    output = orm.Int(7)
    output.base.repository.put_object_from_filelike(io.BytesIO(b'result'), 'result.txt')
    process.out('result', output)
    process.node.set_process_state(ProcessState.RUNNING)
    yield process
    process.close()


def test_transition_commits_before_notification(process, backend, monkeypatch):
    """An independent session sees the whole batch when the superclass is called."""
    commits = []
    engine = backend.get_session().get_bind()
    notifications = []
    original = ProcessBase.on_entered

    def committed(connection):
        commits.append(connection)

    def notified(self, from_state):
        if self is process:
            assert not backend.in_transaction
            assert len(commits) == 1
            with Session(engine) as session:
                node = session.get(DbNode, process.pid)
                assert node.attributes['process_state'] == 'created'
                assert node.attributes['checkpoints']
                assert node.process_state_change_timestamp is not None
                links = session.scalars(select(DbLink).where(DbLink.input_id == process.pid)).all()
                assert [(link.label, link.type) for link in links] == [('result', 'create')]
                output = session.get(DbNode, links[0].output_id)
                assert output.attributes['value'] == 7
                assert output.repository_metadata
            notifications.append(self.pid)
        original(self, from_state)

    monkeypatch.setattr(ProcessBase, 'on_entered', notified)
    event.listen(engine, 'commit', committed)
    try:
        process.on_entered(None)
    finally:
        event.remove(engine, 'commit', committed)
    assert notifications == [process.pid]
    payload = process.runner.persister.load_checkpoint(process.pid)
    process.close()
    restored = payload.decode()
    try:
        assert restored.state is ProcessState.CREATED
        assert 'batch_transitions' not in restored.metadata
        assert restored.runner._batch_transitions is True
        assert restored.outputs['result'].pk == process.outputs['result'].pk
        assert restored.node.outputs.result.base.repository.get_object_content('result.txt', mode='rb') == b'result'
    finally:
        restored.close()


def test_batching_is_not_a_process_input(runner):
    """The user-facing builder and inputs cannot configure engine persistence."""
    assert runner._batch_transitions is False
    assert 'batch_transitions' not in OutputCalculation.spec().inputs['metadata']
    with pytest.raises(ValueError):
        OutputCalculation(runner=runner, inputs={'metadata': {'batch_transitions': True}})


def test_ordinary_orm_writes_remain_immediate(backend, enable_batching):
    """An enabled engine policy must not defer unrelated Python/CLI ORM writes."""
    node = orm.Int(5).store()
    node.base.extras.set('external', True)
    node.label = 'immediately visible'
    assert not backend.in_transaction
    with Session(backend.get_session().get_bind()) as external:
        persisted = external.get(DbNode, node.pk)
        assert persisted.attributes['value'] == 5
        assert persisted.extras['external'] is True
        assert persisted.label == 'immediately visible'


def test_checkpoint_restoration_uses_current_runner_policy(process, runner):
    """A checkpoint must not enable batching on a different worker's runner."""
    process.on_entered(None)
    payload = runner.persister.load_checkpoint(process.pid)
    worker = Runner(loop=runner.loop, persister=runner.persister)
    restored = payload.decode(CheckpointContext(runner=worker))
    try:
        assert runner._batch_transitions is True
        assert worker._batch_transitions is False
        assert restored.runner is worker
        assert 'batch_transitions' not in restored.metadata
        assert restored.node.outputs.result.value == 7
    finally:
        restored.close()
        worker.close()


@pytest.mark.parametrize('batch', [False, True])
def test_complete_process_and_sealing(batch, runner, monkeypatch):
    """Terminal cleanup and sealing retain their existing ordering and behavior."""
    monkeypatch.setattr(runner, '_batch_transitions', batch)
    result = run_get_node(OutputCalculation)
    assert result.node.is_finished_ok
    assert result.node.is_sealed
    assert result.node.checkpoint is None
    assert result.result['result'].value == 7
    with pytest.raises(exceptions.ModificationNotAllowed):
        result.result['result'].base.attributes.set('value', 8)


@pytest.mark.parametrize('failure', ['serialization', 'storage'])
def test_checkpoint_failure_retains_outputs_and_state(process, backend, monkeypatch, caplog, failure):
    """Checkpoint failure is logged, with a usable outer transaction after rollback."""
    old_checkpoint = process.node.checkpoint
    if failure == 'serialization':

        def fail(*args, **kwargs):
            raise ImportError('cannot serialize checkpoint')

        monkeypatch.setattr(CheckpointPayload, 'from_object', fail)
    else:
        duplicate_email = orm.User.collection.get_default().email

        def fail(checkpoint):
            # A real failed SQL flush, wrapped by the persister as PersistenceError.
            orm.User(email=duplicate_email).store()

        monkeypatch.setattr(process.node, 'set_checkpoint', fail)

    process.on_entered(None)
    assert process.node.process_state is ProcessState.CREATED
    assert process.node.outputs.result.value == 7
    assert process.node.checkpoint == old_checkpoint
    assert 'Exception trying to save checkpoint' in caplog.text
    assert not backend.in_transaction


def test_invalid_output_rolls_back_partial_outputs(process, backend, monkeypatch):
    """A later invalid output cannot leave the first output or its links attached."""
    output = process.outputs['result']
    invalid = orm.Int(9).store()
    other = orm.CalculationNode().store()
    invalid.base.links.add_incoming(other, LinkType.CREATE, 'existing')
    process.out('invalid', invalid)
    notifications = []
    monkeypatch.setattr(ProcessBase, 'on_entered', lambda *args: notifications.append(True))
    old_checkpoint = process.node.checkpoint
    with pytest.raises(ValueError):
        process.on_entered(None)
    assert notifications == []
    assert process.node.process_state is ProcessState.CREATED
    assert process.node.checkpoint == old_checkpoint
    assert process.node.base.links.get_outgoing().all() == []
    assert not output.is_stored
    assert output.base.repository.get_object_content('result.txt', mode='rb') == b'result'
    assert output.base.links.incoming_cache == []


def test_commit_failure_does_not_notify_or_retry(process, backend, monkeypatch):
    """Commit errors propagate, restore local outputs, and never publish success."""
    notifications = []
    commits = []
    old_attributes = process.node.base.attributes.all
    old_timestamp = process.node.backend_entity.process_state_change_timestamp
    engine = backend.get_session().get_bind()

    def fail(connection):
        commits.append(connection)
        raise RuntimeError('commit failed')

    monkeypatch.setattr(ProcessBase, 'on_entered', lambda *args: notifications.append(True))
    event.listen(engine, 'commit', fail)
    try:
        with pytest.raises(RuntimeError, match='commit failed'):
            process.on_entered(None)
    finally:
        event.remove(engine, 'commit', fail)
    assert len(commits) == 1
    assert notifications == []
    assert process.node.base.attributes.all == old_attributes
    assert process.node.backend_entity.process_state_change_timestamp == old_timestamp
    assert process.node.base.links.get_outgoing().all() == []
    assert not process.outputs['result'].is_stored


def test_interrupted_checkpoint_rolls_back_batch(process, backend, monkeypatch):
    """Interruption is not a best-effort commit of incomplete transition writes."""
    old_attributes = process.node.base.attributes.all

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(process.runner.persister, 'save_checkpoint', interrupt)
    with pytest.raises(KeyboardInterrupt):
        process.on_entered(None)
    assert process.node.base.attributes.all == old_attributes
    assert process.node.base.links.get_outgoing().all() == []
    assert not process.outputs['result'].is_stored
    assert not backend.in_transaction


def test_lost_commit_acknowledgement_does_not_replay(process, backend, monkeypatch):
    """An error after a real commit is surfaced as an unknown outcome, not retried."""
    session = backend.get_session()
    original = session.commit
    notifications = []
    commits = []

    def lose_acknowledgement():
        original()
        commits.append(True)
        raise RuntimeError('acknowledgement lost')

    monkeypatch.setattr(session, 'commit', lose_acknowledgement)
    monkeypatch.setattr(ProcessBase, 'on_entered', lambda *args: notifications.append(True))
    with pytest.raises(RuntimeError, match='acknowledgement lost'):
        process.on_entered(None)
    assert commits == [True]
    assert notifications == []
    monkeypatch.setattr(session, 'commit', original)
    # Reconcile against persisted data, not the rolled-back local node snapshot.
    persisted = orm.load_node(process.pid)
    assert persisted.process_state is ProcessState.CREATED
    assert persisted.outputs.result.value == 7
    assert persisted.checkpoint
    assert len(persisted.base.links.get_outgoing().all()) == 1


def test_excepted_path_does_not_attach_outputs(process, monkeypatch):
    """A terminal EXCEPTED transition remains robust even with invalid outputs."""

    def fail():
        raise AssertionError('must not update outputs in EXCEPTED')

    monkeypatch.setattr(process, 'update_outputs', fail)
    process.fail(RuntimeError('failed process'), None)
    assert process.node.is_excepted
    assert process.node.is_sealed
    assert process.node.checkpoint is None
    assert process.node.base.links.get_outgoing().all() == []
    assert not process.outputs['result'].is_stored


def test_explicit_caller_transaction_rejected(process, backend):
    """No notification can escape an uncommitted caller transaction."""
    with backend.transaction():
        with pytest.raises(exceptions.InvalidOperation, match='caller transaction'):
            process.on_entered(None)
    assert process.node.process_state is ProcessState.RUNNING
    assert not process.outputs['result'].is_stored


def test_authoritative_metadata_preserves_external_controls(process, backend):
    """Refreshing at the boundary preserves flags committed by a control client."""
    engine = backend.get_session().get_bind()
    with Session(engine) as external:
        node = external.get(DbNode, process.pid)
        node.attributes = {**node.attributes, 'paused': True, 'external_control': 'keep'}
        external.commit()
    process.on_entered(None)
    assert process.node.paused
    assert process.node.base.attributes.get('external_control') == 'keep'
    assert process.node.outputs.result.value == 7


def test_independent_processes_on_one_event_loop(runner, enable_batching):
    """Concurrent tasks retain their own outputs and checkpoints, without a buffer."""
    processes = [OutputCalculation(runner=runner) for _ in range(3)]

    async def execute():
        await asyncio.gather(*(process.step_until_terminated() for process in processes))

    try:
        runner.loop.run_until_complete(execute())
        outputs = []
        for process in processes:
            assert process.node.is_finished_ok
            assert process.node.is_sealed
            assert process.node.checkpoint is None
            outputs.append(process.node.outputs.result.pk)
        assert len(set(outputs)) == 3
    finally:
        for process in processes:
            process.close()


class ParentWorkChain(WorkChain):
    """Dispatch a child and return its committed result."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.output('result', valid_type=orm.Int)
        spec.outline(cls.launch_child, cls.return_result)

    def launch_child(self):
        return self.to_context(child=self.submit(OutputCalculation))

    def return_result(self):
        assert self.ctx.child.is_finished_ok
        self.out('result', self.ctx.child.outputs.result)


def test_child_dispatch_and_parent_wakeup(runner, enable_batching):
    """A real parent resumes with committed child results and provenance."""
    result = run_get_node(ParentWorkChain)
    assert result.node.is_finished_ok
    child = result.node.called[0]
    assert child.is_finished_ok
    assert child.is_sealed
    assert child.caller.pk == result.node.pk
    assert result.node.outputs.result.pk == child.outputs.result.pk


def test_missed_terminal_notification_recovers_through_polling(runner, monkeypatch, enable_batching):
    """No outbox is needed for the existing finish-callback polling fallback."""
    process = OutputCalculation(runner=runner)
    monkeypatch.setattr(runner, '_poll_interval', 0.01)
    if process._communicator is not None:
        monkeypatch.setattr(process._communicator, 'broadcast_send', lambda **kwargs: None)

    async def execute():
        finished = runner.loop.create_future()
        runner.call_on_process_finish(process.pid, lambda: finished.set_result(True))
        await process.step_until_terminated()
        assert await asyncio.wait_for(finished, timeout=5)

    try:
        runner.loop.run_until_complete(execute())
        assert process.node.is_finished_ok
        assert process.node.outputs.result.value == 7
    finally:
        process.close()


def test_local_calcjob_and_cached_replay(aiida_code_installed, enable_batching):
    """Exercise real task boundaries, retrieval, sealing and process caching locally."""
    from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

    inputs = {
        'code': aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash'),
        'x': orm.Int(1),
        'y': orm.Int(2),
    }
    source = run_get_node(ArithmeticAddCalculation, **inputs)
    assert source.node.is_finished_ok
    assert source.node.is_sealed
    assert source.node.checkpoint is None
    assert source.result['sum'].value == 3
    assert source.node.get_job_id()
    with enable_caching():
        cached = run_get_node(ArithmeticAddCalculation, **inputs)
    assert cached.node.is_finished_ok
    assert cached.node.is_sealed
    assert cached.node.base.caching.get_cache_source() == source.node.uuid
    assert cached.result['sum'].value == 3


class WaitingProcess(DummyProcess):
    """Wait between explicit transitions without holding a database transaction."""

    async def run(self):
        return Wait(self.finish)

    def finish(self):
        pass


def test_waiting_reentry_batches_without_timestamp_change(runner, backend, enable_batching):
    """The flush boundary is re-entry, not a change of the persisted state label."""
    process = WaitingProcess(runner=runner)
    try:
        while process.state is not ProcessState.WAITING:
            runner.loop.run_until_complete(process.step())
        timestamp = process.node.backend_entity.process_state_change_timestamp
        output = orm.Int(8).store()
        process.out('result', output)
        assert not backend.in_transaction
        process.on_entered(process._state)
        assert not backend.in_transaction
        assert process.node.process_state is ProcessState.WAITING
        assert process.node.backend_entity.process_state_change_timestamp == timestamp
        assert process.node.outputs.result.pk == output.pk
        assert process.node.checkpoint
    finally:
        process.close()


def test_pause_acknowledgement_and_resume(runner, enable_batching):
    """Phase one leaves immediate pause/resume acknowledgement semantics intact."""
    process = DummyProcess(runner=runner)
    try:
        assert process.pause() is True
        assert process.node.paused
        assert process.node.checkpoint
        assert process.play() is True
        assert not process.node.paused
    finally:
        process.close()


def test_unstored_provenance_bypasses_batching(runner, enable_batching):
    """The engine policy must not accidentally store a provenance-disabled process."""
    process = DummyProcess(runner=runner, inputs={'metadata': {'store_provenance': False}})
    try:
        process.on_entered(None)
        assert not process.node.is_stored
    finally:
        process.close()
