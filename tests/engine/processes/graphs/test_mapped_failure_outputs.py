###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                   #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Mapped process failures keep their stored outputs next to the synthesized error.

A failed `CalcJob` still stores `remote_folder` and any parsed files; the graph
runtime used to drop them and expose only `{'error': member}`, so retries could not
chain `remote_folder` and downstream tasks saw `None` for outputs that exist. These
tests pin the corrected `_resolve_placement_outputs` through real `@graph` runs with
`ShellJob` collaborators: a failed output that is available is preserved, an absent
one stays absent with the right error member, the stored node is never mutated, an
unmapped failure still blocks, and a completed task dispatches once for two consumers.
"""

import io

import pytest

from aiida.calculations.shell import ShellJob
from aiida.common.links import LinkType
from aiida.engine import ExitCode, ExitCodeEnum, graph, task
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.process import GraphProcess
from aiida.orm import Code, RemoteData, SinglefileData

pytestmark = pytest.mark.presto


class ShellErrors(ExitCodeEnum):
    """A single mapped failure, mirroring the QE retryable-error shape."""

    COMMAND_FAILED = ExitCode(400, 'The command failed.')


class ShellPartialErrors(ExitCodeEnum):
    """Two mapped failures, so the absent arm still resolves the right member."""

    COMMAND_FAILED = ExitCode(400, 'The command failed.')
    OUTPUT_MISSING = ExitCode(303, 'An output file was missing.')


fail = task(
    ShellJob,
    inputs={'nodes': {'text': SinglefileData}},
    outputs={'stdout': SinglefileData},
    errors=ShellErrors,
)
fail_partial = task(
    ShellJob,
    inputs={'nodes': {'text': SinglefileData}},
    outputs={'stdout': SinglefileData, 'extra_txt': SinglefileData},
    errors=ShellPartialErrors,
)
cat = task(ShellJob, inputs={'nodes': {'text': SinglefileData}}, outputs={'stdout': SinglefileData})
rerun = task(
    ShellJob,
    inputs={'nodes': {'text': SinglefileData, 'prev': RemoteData}},
    outputs={'stdout': SinglefileData},
)


@task
def read_stdout(content: SinglefileData) -> str:
    """Read command output."""
    return content.get_content()


@task
def read_stdout_with_error(content: SinglefileData, error: ShellErrors) -> str:
    """Recover with the stored output of a failed run next to its error member."""
    return f'recovered from {error.value.status} with {len(content.get_content())} bytes'


@task
def ensure_absent(value: SinglefileData | None, error: ShellPartialErrors) -> str:
    """Recover while the missing output stays absent, naming the right member."""
    if value is not None:
        raise ValueError('the output should be absent on this failure.')
    return f'recovered from {error.value.status}'


@task
def combine(a: str, b: str) -> str:
    """Join two reads of one completed task."""
    return f'{a}|{b}'


@graph
def recover_keeping_stdout(text: SinglefileData, code: Code, arguments: list[str]) -> str:
    """Recover from a mapped failure with the failed run's stored `stdout`."""
    ran = fail(code=code, arguments=arguments, nodes={'text': text})
    if ran.error is not None:
        selected = read_stdout_with_error(content=ran.stdout, error=ran.error)
    else:
        selected = read_stdout(content=ran.stdout)
    return selected


@graph
def read_absent_arm(text: SinglefileData, code: Code, arguments: list[str], outputs: list[str]) -> str:
    """Read a stored arm and a missing arm of one mapped failure."""
    ran = fail_partial(code=code, arguments=arguments, nodes={'text': text}, outputs=outputs)
    return ensure_absent(value=ran.extra_txt, error=ran.error)


@graph
def retry_with_remote(
    text: SinglefileData, fail_code: Code, cat_code: Code, fail_arguments: list[str], ok_arguments: list[str]
) -> SinglefileData:
    """Chain the `remote_folder` a mapped failure stored into the retry's inputs."""
    first = fail(code=fail_code, arguments=fail_arguments, nodes={'text': text})
    second = rerun(code=cat_code, arguments=ok_arguments, nodes={'text': text, 'prev': first.remote_folder})
    return second.stdout


@graph
def reuse_success(text: SinglefileData, code: Code, arguments: list[str]) -> str:
    """Read one completed task from two consumers without redispatching it."""
    first = cat(code=code, arguments=arguments, nodes={'text': text})
    first_read = read_stdout(content=first.stdout)
    second_read = read_stdout(content=first.stdout)
    return combine(a=first_read, b=second_read)


@pytest.fixture
def shell_code(aiida_code_installed):
    """Return a factory for a `core.shell` code running one system command."""
    import shutil

    def factory(command: str):
        return aiida_code_installed(
            label=command, default_calc_job_plugin='core.shell', filepath_executable=shutil.which(command)
        )

    return factory


@pytest.fixture
def text_file():
    """Return a stored-on-use text file."""
    return SinglefileData(io.BytesIO(b'one two three\n'), filename='text.txt')


def test_graphs_build() -> None:
    """The failure-output graphs pass syntax, wiring, and contract checks."""
    assert recover_keeping_stdout.build() is None
    assert read_absent_arm.build() is None
    assert retry_with_remote.build() is None
    assert reuse_success.build() is None


@pytest.mark.usefixtures('aiida_profile_clean')
def test_failed_stdout_reaches_recovery(shell_code, text_file) -> None:
    """A mapped failure preserves its stored `stdout` next to the synthesized error."""
    outputs, node = run_get_node(
        recover_keeping_stdout,
        text=text_file,
        code=shell_code('false'),
        arguments=[],
        task_metadata={'fail': {'options': {}}},
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 'recovered from 400 with 0 bytes'
    (failed,) = [
        link.node
        for link in node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all()
        if '__' not in link.link_label
    ]
    assert failed.exit_status == 400
    # The error arm is synthesized in the graph view only; the stored node keeps its
    # outputs and gains no `error` link.
    assert 'error' not in failed.outputs
    assert 'stdout' in failed.outputs
    assert 'remote_folder' in failed.outputs


@pytest.mark.usefixtures('aiida_profile_clean')
def test_absent_output_stays_absent_with_correct_error(shell_code, text_file) -> None:
    """A missing output of a mapped failure reads as `None` with the right member."""
    outputs, node = run_get_node(
        read_absent_arm,
        text=text_file,
        code=shell_code('false'),
        arguments=[],
        outputs=['extra.txt'],
        task_metadata={'fail_partial': {'options': {}}},
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 'recovered from 303'


@pytest.mark.usefixtures('aiida_profile_clean')
def test_retry_chains_failed_remote_folder(shell_code, text_file) -> None:
    """The retry consumes the `remote_folder` the mapped failure stored."""
    outputs, node = run_get_node(
        retry_with_remote,
        text=text_file,
        fail_code=shell_code('false'),
        cat_code=shell_code('cat'),
        fail_arguments=[],
        ok_arguments=['{text}'],
        task_metadata={'fail': {'options': {}}, 'rerun': {'options': {}}},
    )
    assert node.is_finished_ok
    assert outputs['result'].get_content() == 'one two three\n'
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert calls['fail'].exit_status == 400
    retried = calls['rerun']
    assert retried.is_finished_ok
    assert retried.inputs.nodes.prev.uuid == calls['fail'].outputs.remote_folder.uuid


@pytest.mark.usefixtures('aiida_profile_clean')
def test_unmapped_failure_still_blocks(shell_code, text_file) -> None:
    """A failure without a mapping fails the graph and dispatches nothing else."""
    outputs, node = run_get_node(
        recover_keeping_stdout,
        text=text_file,
        code=shell_code('sh'),
        arguments=['-c', 'echo boom 1>&2'],
        task_metadata={'fail': {'options': {}}},
    )
    assert node.exit_status == GraphProcess.exit_codes.ERROR_TASK_FAILED.status
    assert outputs == {}
    assert node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all_link_labels() == ['fail']


@pytest.mark.usefixtures('aiida_profile_clean')
def test_completed_task_reused_by_two_consumers(shell_code, text_file) -> None:
    """Two reads of one completed task dispatch it once and share its node."""
    outputs, node = run_get_node(
        reuse_success,
        text=text_file,
        code=shell_code('cat'),
        arguments=['{text}'],
        task_metadata={'cat': {'options': {}}},
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 'one two three\n|one two three\n'
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert sorted(calls) == ['cat', 'combine', 'read_stdout', 'read_stdout_2']
    assert calls['read_stdout'].inputs.content.uuid == calls['read_stdout_2'].inputs.content.uuid
