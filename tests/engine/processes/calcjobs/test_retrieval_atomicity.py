###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Checkpoint consistency at the retrieval-to-parsing transition."""

from pathlib import Path

import plumpy.exceptions
import pytest

from aiida import orm
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.common.datastructures import CalcJobState
from aiida.engine import ProcessState
from aiida.engine.processes.calcjobs import tasks


@pytest.mark.requires_broker
@pytest.mark.parametrize('cut', ['before_transition', 'after_transition', 'before_checkpoint', 'after_checkpoint'])
def test_retrieval_transition_atomicity(aiida_code_installed, aiida_manager, monkeypatch, cut):
    """Recovery sees either the whole parsing transition or the preceding retrieval checkpoint."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(ArithmeticAddCalculation, code=code, x=orm.Int(1), y=orm.Int(2))

    async def replay():
        for _ in range(40):
            if isinstance(process._state, tasks.Waiting) and process._state._command == tasks.RETRIEVE_COMMAND:
                break
            assert not process.has_terminated()
            await process.step()
        else:
            pytest.fail('Did not reach retrieval')

        node = process.node
        previous_state = node.get_state()
        previous_process_state = node.process_state
        previous_checkpoint = node.checkpoint
        waiting = process._state
        next_state = await waiting.execute()
        retrieved_uuid = process.outputs['retrieved'].uuid
        assert node.get_state() == previous_state
        assert 'retrieved' not in node.outputs
        assert node.checkpoint == previous_checkpoint

        if cut == 'after_transition':
            process.transition_to(next_state)
            assert node.get_state() == CalcJobState.PARSING
            assert node.outputs.retrieved.uuid == retrieved_uuid
            assert node.checkpoint != previous_checkpoint
        elif cut in ('before_checkpoint', 'after_checkpoint'):
            assert runner.persister is not None
            save_checkpoint = runner.persister.save_checkpoint

            def fail_checkpoint(process, tag=None):
                # Both preceding writes must already be visible within the transaction.
                assert process.node.get_state() == CalcJobState.PARSING
                assert process.node.outputs.retrieved.uuid == retrieved_uuid
                if cut == 'after_checkpoint':
                    save_checkpoint(process, tag)
                msg = 'Injected checkpoint failure'
                raise plumpy.exceptions.PersistenceError(msg)

            with monkeypatch.context() as patch:
                patch.setattr(runner.persister, 'save_checkpoint', fail_checkpoint)
                # Call the persistence callback directly to inspect rollback before plumpy handles the exception.
                process._state = next_state
                with pytest.raises(plumpy.exceptions.PersistenceError, match='Injected checkpoint failure'):
                    process.on_entered(waiting)

            persisted = orm.load_node(node.pk)
            assert persisted.get_state() == previous_state
            assert persisted.process_state == previous_process_state
            assert persisted.checkpoint == previous_checkpoint
            assert 'retrieved' not in persisted.outputs

        process.close()
        assert runner.persister is not None
        restored = runner.persister.load_checkpoint(process.pid).unbundle()
        try:
            await restored.step_until_terminated()
            assert restored.node.is_finished_ok
            assert restored.outputs['sum'].value == 3
            retrieved = restored.node.outputs.retrieved
            assert retrieved.base.repository.get_object_content('aiida.out') == '3\n'
            assert restored.outputs['retrieved'].uuid == retrieved.uuid
            assert len(restored.node.base.links.get_outgoing(link_label_filter='retrieved').all()) == 1
            if cut == 'after_transition':
                assert retrieved.uuid == retrieved_uuid
            assert (Path(node.get_remote_workdir()) / 'aiida.out').read_text() == '3\n'
        finally:
            restored.close()

    try:
        runner.run_until_complete(replay())
    finally:
        process.close()


@pytest.mark.requires_broker
@pytest.mark.parametrize(
    'command,expected',
    [
        (tasks.UPLOAD_COMMAND, CalcJobState.UPLOADING),
        (tasks.UNSTASH_COMMAND, CalcJobState.UNSTASHING),
        (tasks.SUBMIT_COMMAND, CalcJobState.SUBMITTING),
        (tasks.UPDATE_COMMAND, CalcJobState.WITHSCHEDULER),
        (tasks.STASH_COMMAND, CalcJobState.STASHING),
        (tasks.RETRIEVE_COMMAND, CalcJobState.RETRIEVING),
    ],
)
@pytest.mark.parametrize('failure', [False, True])
def test_waiting_phase_atomicity(aiida_code_installed, aiida_manager, monkeypatch, command, expected, failure):
    """Every transport command's CalcJob phase is committed with its matching checkpoint."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(ArithmeticAddCalculation, code=code, x=orm.Int(1), y=orm.Int(2))
    previous_state = process._state
    previous_checkpoint = process.node.checkpoint
    process._state = process.create_state(ProcessState.WAITING, None, data={'command': command, 'skip_submit': False})
    assert runner.persister is not None
    save_checkpoint = runner.persister.save_checkpoint

    def checkpoint(process, tag=None):
        assert process.node.get_state() is expected
        save_checkpoint(process, tag)
        if failure:
            msg = 'Injected checkpoint failure'
            raise plumpy.exceptions.PersistenceError(msg)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(runner.persister, 'save_checkpoint', checkpoint)
            if failure:
                with pytest.raises(plumpy.exceptions.PersistenceError, match='Injected checkpoint failure'):
                    process.on_entered(previous_state)
            else:
                process.on_entered(previous_state)

        persisted = orm.load_node(process.pid)
        if failure:
            assert persisted.get_state() is None
            assert persisted.process_state is ProcessState.CREATED
            assert persisted.checkpoint == previous_checkpoint
        else:
            assert persisted.get_state() is expected
            assert persisted.process_state is ProcessState.WAITING
            assert persisted.checkpoint != previous_checkpoint
    finally:
        process.close()
