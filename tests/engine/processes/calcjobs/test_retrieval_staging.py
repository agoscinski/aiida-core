###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Isolation, ownership and checkpoint recovery of temporary parser inputs."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import plumpy.process_states as states
import pytest

from aiida import orm
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.common.links import LinkType
from aiida.engine import ToContext, WorkChain, run_get_node
from aiida.engine.daemon import execmanager
from aiida.engine.processes.calcjobs import tasks
from aiida.engine.processes.calcjobs.monitors import CalcJobMonitorResult
from aiida.engine.processes.exit_code import ExitCode
from aiida.engine.utils import InterruptableFuture
from aiida.parsers import Parser
from aiida.plugins import TransportFactory
from aiida.transports.plugins.local import LocalTransport


class TemporaryResultCalculation(ArithmeticAddCalculation):
    """Write a numeric output retrieved only for parsing."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.inputs['metadata']['options']['parser_name'].default = 'test.temporary_result'

    def prepare_for_submission(self, folder):
        info = super().prepare_for_submission(folder)
        with folder.open(self.options.input_filename, 'a') as handle:
            handle.write("mkdir data\nprintf '123456789\\n' > data/result.txt\n")
        info.retrieve_temporary_list = ['data']
        return info


class TemporaryResultParser(Parser):
    """A numeric prefix is valid input, so stale partial downloads can silently change the result."""

    def parse(self, **kwargs):
        try:
            result = int((Path(kwargs['retrieved_temporary_folder']) / 'data' / 'result.txt').read_text())
        except OSError:
            return self.exit_codes.ERROR_READING_OUTPUT_FILE
        except ValueError:
            return self.exit_codes.ERROR_INVALID_OUTPUT
        self.out('sum', orm.Int(result))


class RetrievalParentWorkChain(WorkChain):
    """Check that the complete temporary output reaches the parent workflow."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('code', valid_type=orm.Code)
        spec.output('sum', valid_type=orm.Int)
        spec.exit_code(401, 'ERROR_CHILD_FAILED', message='Calculation child failed.')
        spec.outline(cls.run_calculation, cls.inspect_calculation)

    def run_calculation(self):
        return ToContext(
            child=self.submit(TemporaryResultCalculation, code=self.inputs.code, x=orm.Int(1), y=orm.Int(2))
        )

    def inspect_calculation(self):
        if not self.ctx.child.is_finished_ok:
            return self.exit_codes.ERROR_CHILD_FAILED
        self.out('sum', self.ctx.child.outputs.sum)


@pytest.fixture
def temporary_result_parser(entry_points):
    """Register the test parser without installing a plugin."""
    entry_points.add(TemporaryResultParser, group='aiida.parsers', name='test.temporary_result')


async def reach_retrieval(process):
    """Advance a real calculation to its retrieval checkpoint."""
    for _ in range(40):
        if isinstance(process._state, tasks.Waiting) and process._state._command == tasks.RETRIEVE_COMMAND:
            return
        assert not process.has_terminated()
        await process.step()
    pytest.fail('Did not reach retrieval')


@pytest.mark.requires_broker
@pytest.mark.parametrize('backend', ['local', 'asyncssh', 'openssh'])
@pytest.mark.parametrize('failures,partial', [(0, '123'), (1, 'invalid'), (1, '123'), (3, '123'), (20, '123')])
def test_retries(
    aiida_localhost,
    aiida_computer_ssh,
    aiida_code_installed,
    aiida_manager,
    temporary_result_parser,
    monkeypatch,
    backend,
    failures,
    partial,
):
    """Retries get distinct clean destinations, and partial numeric results never reach the parent."""
    computer = aiida_localhost if backend == 'local' else aiida_computer_ssh(backend=backend)
    if backend != 'local':
        computer.configure(backend=backend, safe_interval=0)
    code = aiida_code_installed(computer=computer, filepath_executable='/bin/bash')
    # Daemon recovery can reload the active profile independently of the session fixture.
    profile = aiida_manager.get_profile()
    assert profile is not None
    monkeypatch.setitem(profile.options, 'transport.task_retry_initial_interval', 0)
    monkeypatch.setitem(profile.options, 'transport.task_maximum_attempts', failures + 1)
    transport_class = TransportFactory(computer.transport_type)
    original_get = transport_class.get_async
    failed_folders = []

    async def interrupted_get(self, source, destination, *args, **kwargs):
        if Path(source).name == 'data' and len(failed_folders) < failures:
            destination = Path(destination)
            destination.mkdir(exist_ok=True)
            (destination / 'result.txt').write_text(partial)
            failed_folders.append(destination.parent)
            raise OSError('injected temporary download error')
        return await original_get(self, source, destination, *args, **kwargs)

    monkeypatch.setattr(transport_class, 'get_async', interrupted_get)
    results, parent = run_get_node(RetrievalParentWorkChain, code=code)
    child = parent.base.links.get_outgoing(node_class=orm.CalcJobNode).one().node
    assert len(set(failed_folders)) == failures
    assert parent.is_finished_ok, (parent.exit_status, child.exit_status)
    assert results['sum'].value == 123456789
    assert child.outputs.retrieved.base.repository.get_object_content('aiida.out') == '3\n'
    assert all(not path.exists() for path in failed_folders)


@pytest.mark.requires_broker
@pytest.mark.parametrize('cut', ['before_transition', 'intact', 'modified', 'partial', 'after_parsing'])
@pytest.mark.parametrize('override_status', [0, 450])
def test_checkpoint_recovery(
    aiida_code_installed,
    aiida_manager,
    temporary_result_parser,
    monkeypatch,
    cut,
    override_status,
):
    """A restored parsing path is never trusted, even when it still exists or looks valid."""
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(
        TemporaryResultCalculation,
        code=aiida_code_installed(filepath_executable='/bin/bash'),
        x=orm.Int(1),
        y=orm.Int(2),
    )

    async def replay():
        await reach_retrieval(process)
        next_state = await process._state.execute()
        folder = Path(next_state.args[0])
        assert (folder / 'data' / 'result.txt').read_text() == '123456789\n'
        if override_status:
            next_state = process._state.parse(str(folder), ExitCode(override_status, 'test override'))
        retrieved_uuid = None
        if cut != 'before_transition':
            process.transition_to(next_state)
            retrieved_uuid = process.node.outputs.retrieved.uuid
            # Recovery must preserve permanent output even if it can no longer be downloaded.
            (Path(process.node.get_remote_workdir()) / 'aiida.out').unlink()
        if cut == 'modified':
            (folder / 'data' / 'result.txt').write_text('123')
        elif cut == 'partial':
            (folder / 'data' / 'result.txt').unlink()
        elif cut == 'after_parsing':
            await process._state.execute()
            assert not folder.exists()

        # Preserve abandoned files to model abrupt worker loss rather than graceful cleanup.
        owner = process._temporary_retrieval
        with monkeypatch.context() as patch:
            patch.setattr(process, '_release_temporary_retrieval', lambda: None)
            process.close()
        if cut != 'after_parsing':
            assert folder.is_dir()

        assert runner.persister is not None
        restored = runner.persister.load_checkpoint(process.pid).unbundle()
        assert restored._temporary_retrieval is None
        try:
            await restored.step_until_terminated()
            # An override added only to an uncommitted next state cannot survive worker loss.
            expected_status = 0 if cut == 'before_transition' else override_status
            assert restored.node.exit_status == expected_status
            assert restored.outputs['sum'].value == 123456789
            if retrieved_uuid is not None:
                assert restored.node.outputs.retrieved.uuid == retrieved_uuid
            assert restored.outputs['retrieved'].uuid == restored.node.outputs.retrieved.uuid
        finally:
            restored.close()
            if owner is not None:
                owner.cleanup()

    try:
        runner.run_until_complete(replay())
    finally:
        process.close()


@pytest.mark.requires_broker
@pytest.mark.parametrize('attempts', [1, 3])
@pytest.mark.parametrize('error_type', [OSError, states.Interruption, asyncio.CancelledError])
def test_failed_attempt_resume(
    aiida_code_installed,
    aiida_manager,
    temporary_result_parser,
    monkeypatch,
    attempts,
    error_type,
):
    """Exhausted retries and interruption clean their staging before checkpoint replay."""
    profile = aiida_manager.get_profile()
    assert profile is not None
    monkeypatch.setitem(profile.options, 'transport.task_retry_initial_interval', 0)
    monkeypatch.setitem(profile.options, 'transport.task_maximum_attempts', attempts)
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(
        TemporaryResultCalculation,
        code=aiida_code_installed(filepath_executable='/bin/bash'),
        x=orm.Int(1),
        y=orm.Int(2),
    )
    transport_class = TransportFactory('core.local')
    original_get = transport_class.get_async
    failed_folders = []

    async def interrupted_get(self, source, destination, *args, **kwargs):
        if Path(source).name == 'data':
            destination = Path(destination)
            destination.mkdir()
            (destination / 'result.txt').write_text('123')
            failed_folders.append(destination.parent)
            raise error_type('injected download interruption')
        return await original_get(self, source, destination, *args, **kwargs)

    async def replay():
        await reach_retrieval(process)
        with monkeypatch.context() as patch:
            patch.setattr(transport_class, 'get_async', interrupted_get)
            if error_type is OSError:
                await process.step()
                assert process.paused
                assert len(failed_folders) == attempts
            else:
                with pytest.raises(error_type, match='injected download interruption'):
                    if error_type is asyncio.CancelledError:
                        await tasks.task_retrieve_job(process, runner.transport, InterruptableFuture())
                    else:
                        await process._state.execute()
                assert len(failed_folders) == 1
        assert all(not path.exists() for path in failed_folders)
        process.close()
        assert runner.persister is not None
        restored = runner.persister.load_checkpoint(process.pid).unbundle()
        try:
            restored.play()
            await restored.step_until_terminated()
            assert restored.node.is_finished_ok
            assert restored.outputs['sum'].value == 123456789
        finally:
            restored.close()

    try:
        runner.run_until_complete(replay())
    finally:
        process.close()


@pytest.mark.requires_broker
@pytest.mark.parametrize('cut', ['close', 'monitor_skip', 'scheduler_error', 'parser_error'])
def test_cleanup_without_successful_parsing(
    aiida_code_installed,
    aiida_manager,
    temporary_result_parser,
    monkeypatch,
    cut,
):
    """The completed directory has an owner even if parsing is skipped or fails early."""
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(
        TemporaryResultCalculation,
        code=aiida_code_installed(filepath_executable='/bin/bash'),
        x=orm.Int(1),
        y=orm.Int(2),
    )
    folders = []

    def directory(*args, **kwargs):
        result = TemporaryDirectory(*args, **kwargs)
        folders.append(Path(result.name))
        return result

    async def replay():
        await reach_retrieval(process)
        if cut == 'monitor_skip':
            process._state._monitor_result = CalcJobMonitorResult(parse=False, message='stop')
        monkeypatch.setattr(tasks, 'TemporaryDirectory', directory)
        next_state = await process._state.execute()
        assert len(folders) == 1
        folder = folders[0]
        if cut == 'monitor_skip':
            assert not folder.exists()
            assert process._temporary_retrieval is None
        elif cut == 'close':
            assert folder.is_dir()
            process.close()
            assert not folder.exists()
        else:
            process.transition_to(next_state)

            def fail(*args, **kwargs):
                msg = 'injected parsing error'
                raise RuntimeError(msg)

            method = 'parse_scheduler_output' if cut == 'scheduler_error' else 'parse_retrieved_output'
            monkeypatch.setattr(process, method, fail)
            with pytest.raises(RuntimeError, match='injected parsing error'):
                process.parse(str(folder))
            assert not folder.exists()
            assert process._temporary_retrieval is None

    try:
        runner.run_until_complete(replay())
    finally:
        process.close()


@pytest.mark.requires_broker
def test_storage_loss_without_restart(aiida_code_installed, aiida_manager, temporary_result_parser):
    """Loss of a completed directory also triggers fresh retrieval in the original worker."""
    runner = aiida_manager.get_runner()
    process = runner.instantiate_process(
        TemporaryResultCalculation,
        code=aiida_code_installed(filepath_executable='/bin/bash'),
        x=orm.Int(1),
        y=orm.Int(2),
    )

    async def replay():
        await reach_retrieval(process)
        next_state = await process._state.execute()
        process.transition_to(next_state)
        retrieved_uuid = process.node.outputs.retrieved.uuid
        owner = process._temporary_retrieval
        assert owner is not None
        owner.cleanup()
        assert not Path(owner.path).exists()
        await process.step_until_terminated()
        assert process.node.is_finished_ok
        assert process.outputs['sum'].value == 123456789
        assert process.node.outputs.retrieved.uuid == retrieved_uuid
        assert process._temporary_retrieval is None

    try:
        runner.run_until_complete(replay())
    finally:
        process.close()


@pytest.mark.asyncio
async def test_retrieve_temporary_with_existing_output(aiida_localhost, tmp_path):
    """Existing permanent provenance does not suppress retrieval of temporary inputs."""
    node = orm.CalcJobNode(computer=aiida_localhost).store()
    remote = tmp_path / 'remote'
    remote.mkdir()
    (remote / 'permanent.txt').write_text('permanent output')
    (remote / 'temporary.txt').write_text('temporary output')
    node.set_remote_workdir(str(remote))
    node.set_retrieve_list(['permanent.txt'])
    node.set_retrieve_temporary_list(['temporary.txt'])
    before = tmp_path / 'before'
    after = tmp_path / 'after'
    before.mkdir()
    after.mkdir()

    with LocalTransport() as transport:
        retrieved = await execmanager.retrieve_calculation(node, transport, before)
        retrieved.base.links.add_incoming(node, LinkType.CREATE, node.link_label_retrieved)
        retrieved_uuid = retrieved.uuid
        (remote / 'permanent.txt').unlink()
        (remote / 'temporary.txt').write_text('recovered temporary output')
        result = await execmanager.retrieve_calculation(node, transport, after)

    assert result is None
    assert (after / 'temporary.txt').read_text() == 'recovered temporary output'
    assert node.outputs.retrieved.uuid == retrieved_uuid
    assert node.outputs.retrieved.base.repository.get_object_content('permanent.txt') == 'permanent output'
