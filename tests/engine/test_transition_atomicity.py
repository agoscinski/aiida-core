###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Atomic persistence for all process state transitions."""

import plumpy
import pytest
from plumpy.process_states import ProcessState

from aiida import orm
from aiida.engine import ExitCode
from tests.utils.processes import AddProcess


@pytest.mark.requires_broker
@pytest.mark.parametrize('state', [ProcessState.RUNNING, ProcessState.WAITING])
@pytest.mark.parametrize('failure', [None, 'before_checkpoint', 'after_checkpoint'])
def test_nonterminal_transition(aiida_manager, monkeypatch, state, failure):
    """Every resumable transition commits its outputs, state and checkpoint together."""
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(AddProcess, a=1, b=1)
    previous_state = process._state
    previous_checkpoint = process.node.checkpoint
    output = orm.Int(2).store()
    process.out('result', output)
    process._state = process.create_state(state, process.run if state is ProcessState.RUNNING else None)
    assert runner.persister is not None
    save_checkpoint = runner.persister.save_checkpoint

    def checkpoint(process, tag=None):
        assert process.node.process_state is state
        assert process.node.outputs.result.uuid == output.uuid
        if failure != 'before_checkpoint':
            save_checkpoint(process, tag)
        if failure is not None:
            msg = 'Injected checkpoint failure'
            raise plumpy.exceptions.PersistenceError(msg)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(runner.persister, 'save_checkpoint', checkpoint)
            if failure is None:
                process.on_entered(previous_state)
            else:
                with pytest.raises(plumpy.exceptions.PersistenceError, match='Injected checkpoint failure'):
                    process.on_entered(previous_state)

        persisted = orm.load_node(process.pid)
        if failure is None:
            assert persisted.process_state is state
            assert persisted.outputs.result.uuid == output.uuid
            assert persisted.checkpoint != previous_checkpoint
        else:
            assert persisted.process_state is ProcessState.CREATED
            assert persisted.checkpoint == previous_checkpoint
            assert not persisted.base.links.get_outgoing().all()
    finally:
        process.close()


@pytest.mark.requires_broker
@pytest.mark.parametrize('state', [ProcessState.FINISHED, ProcessState.KILLED, ProcessState.EXCEPTED])
@pytest.mark.parametrize('failure', [False, True])
def test_terminal_transition(aiida_manager, monkeypatch, state, failure):
    """Terminal state, finish/exception details, sealing and checkpoint removal are one commit."""
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(AddProcess, a=1, b=1)
    previous_state = process._state
    previous_checkpoint = process.node.checkpoint
    output = orm.Int(2).store()
    process.out('result', output)
    if state is ProcessState.FINISHED:
        process._state = process.create_state(state, ExitCode(123, 'Failure'), False)
    elif state is ProcessState.KILLED:
        process._state = process.create_state(state, None)
    else:
        process._state = process.create_state(state, RuntimeError('Failure'), None)
    assert runner.persister is not None
    delete_checkpoint = runner.persister.delete_checkpoint

    def delete(pid, tag=None):
        assert process.node.process_state is state
        delete_checkpoint(pid, tag)
        if failure:
            msg = 'Injected checkpoint deletion failure'
            raise plumpy.exceptions.PersistenceError(msg)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(runner.persister, 'delete_checkpoint', delete)
            if failure:
                with pytest.raises(plumpy.exceptions.PersistenceError, match='Injected checkpoint deletion failure'):
                    process.on_entered(previous_state)
            else:
                # Only inspect persistence here: terminal listeners expect on_entering to have completed.
                process._persist_transition(previous_state)

        persisted = orm.load_node(process.pid)
        if failure:
            assert persisted.process_state is ProcessState.CREATED
            assert persisted.checkpoint == previous_checkpoint
            assert not persisted.is_sealed
            assert persisted.exit_status is None
            assert persisted.exception is None
            assert not persisted.base.links.get_outgoing().all()
        else:
            assert persisted.process_state is state
            assert persisted.checkpoint is None
            assert persisted.is_sealed
            if state is ProcessState.FINISHED:
                assert persisted.exit_status == 123
                assert persisted.exit_message == 'Failure'
            elif state is ProcessState.EXCEPTED:
                assert persisted.exception == 'RuntimeError: Failure'
    finally:
        process.close()
