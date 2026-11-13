###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Daemon regression: an error arm reaches its consumer as the enum member.

A ``Result`` error arm is stored as an ``EnumData`` node. Local runs unwrap it to
the declared member through port preparation, but daemon workers reload task
executors from their pickled payload, which duplicates importable enum classes by
value. A plain ``isinstance`` check then misses the provenance member and the
consumer would observe the stored node instead. The consumer below declares the
strict ``DaemonStepErrors | None`` contract (no ``EnumData`` union): it must see
the actual member both locally and when submitted through the real daemon, where
the graph checkpoints between dispatches and validates inputs after restore.
"""

import pytest

from aiida.engine.launch import run_get_node
from aiida.engine.processes.exit_code import ExitCode, ExitCodeEnum
from aiida.engine.processes.graphs.lowering import graph, task
from aiida.engine.processes.graphs.process import GraphProcess
from aiida.engine.processes.graphs.results import Err, Ok, Result
from aiida.orm import CalcFunctionNode, EnumData, GraphNode

pytestmark = pytest.mark.presto


class DaemonStepErrors(ExitCodeEnum):
    """Declared failure for the daemon error-arm regression."""

    BAD = ExitCode(410, 'Bad input for the daemon regression.')


@task
def fallible(flag: bool) -> Result[float, DaemonStepErrors]:
    """Fail with the declared member when flagged, else succeed."""
    if flag:
        return Err(DaemonStepErrors.BAD)
    return Ok(1.5)


@task
def probe(error: DaemonStepErrors | None) -> str:
    """Report the error arm; it must arrive as the member, never the stored node."""
    if error is None:
        return 'none'
    assert not isinstance(error, EnumData), f'consumer received a stored node, not a member: {error!r}'
    assert error is DaemonStepErrors.BAD, f'consumer received {error!r}, expected the declared member.'
    return f'{type(error).__name__}.{error.name}'


@graph
def error_to_probe(flag: bool) -> str:
    """Route a declared failure arm into the strict consumer."""
    _value, error = fallible(flag=flag)
    return probe(error=error)


def test_consumer_declares_strict_contract() -> None:
    """The regression pins the strict contract: no ``EnumData`` weakening."""
    valid_type = probe.spec().inputs['error'].valid_type
    kinds = valid_type if isinstance(valid_type, tuple) else (valid_type,)
    assert DaemonStepErrors in kinds
    assert EnumData not in kinds


@pytest.mark.usefixtures('aiida_profile_clean')
def test_error_arm_member_local() -> None:
    """Locally the consumer sees the declared member on both arms."""
    outputs, node = run_get_node(error_to_probe, flag=True)
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert outputs['result'].value == 'DaemonStepErrors.BAD'

    outputs, node = run_get_node(error_to_probe, flag=False)
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert outputs['result'].value == 'none'


@pytest.mark.usefixtures('started_daemon_client')
def test_error_arm_member_submitted(submit_and_await) -> None:
    """Submitted, the consumer sees the same member after checkpoint restore.

    The graph dispatches ``fallible`` then checkpoints while waiting; ``probe``
    inputs are prepared and validated after that restore on the daemon worker.
    """
    node = submit_and_await(GraphProcess, timeout=120, **error_to_probe.get_launch_inputs(flag=True))
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert isinstance(node, GraphNode)
    assert node.outputs.result.value == 'DaemonStepErrors.BAD'
    consumers = [
        child for child in node.called_descendants if isinstance(child, CalcFunctionNode) and 'result' in child.outputs
    ]
    assert consumers, 'the submitted graph must place the consumer task.'
    assert any(child.outputs.result.value == 'DaemonStepErrors.BAD' for child in consumers)

    node = submit_and_await(GraphProcess, timeout=120, **error_to_probe.get_launch_inputs(flag=False))
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert node.outputs.result.value == 'none'
