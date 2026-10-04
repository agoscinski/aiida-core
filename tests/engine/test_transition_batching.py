###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Atomic, explicitly opted-in process transition persistence."""

import io

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from aiida import orm
from aiida.common import exceptions
from aiida.common.links import LinkType
from aiida.common.processes import ProcessState
from aiida.engine import Process, run_get_node
from aiida.engine.processes.generic.process import Process as ProcessBase
from aiida.engine.processes.persistence import CheckpointPayload
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
def process(runner):
    """A process with a pending new output and a previously persisted state."""
    process = OutputCalculation(runner=runner, inputs={'metadata': {'batch_transitions': True}})
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
        assert restored.metadata.batch_transitions is True
        assert restored.outputs['result'].pk == process.outputs['result'].pk
        assert restored.node.outputs.result.base.repository.get_object_content('result.txt', mode='rb') == b'result'
    finally:
        restored.close()


@pytest.mark.parametrize('batch', [False, True])
def test_complete_process_and_sealing(batch, runner):
    """Terminal cleanup and sealing retain their existing ordering and behavior."""
    result = run_get_node(OutputCalculation, metadata={'batch_transitions': batch})
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


def test_unstored_provenance_bypasses_batching(runner):
    """Opt-in must not accidentally store a provenance-disabled process."""
    process = DummyProcess(runner=runner, inputs={'metadata': {'batch_transitions': True, 'store_provenance': False}})
    try:
        process.on_entered(None)
        assert not process.node.is_stored
    finally:
        process.close()
