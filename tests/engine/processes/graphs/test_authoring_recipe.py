###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Canonical graph-authoring recipe: start here, not in ``programs/``.

This module is a concise, tested entry point for a bounded scientific workflow.
The ``programs/`` subtree beside it also holds historical regression fixtures for the
behavior suites; those fixtures cover edge cases and failure modes on purpose and
should not all be treated as recommended authoring style.
Copy this module's shape: a ``calcfunction`` holds the science, a ``WorkChain``
places it, the ``@graph`` only orchestrates, and a ``@task`` report records the
outcome so the root returns an existing node.
"""

import pytest

from aiida.common.links import LinkType
from aiida.engine import WorkChain, calcfunction, graph, run_get_node, task
from aiida.orm import Dict, Int

pytestmark = pytest.mark.presto

# recipe-collaborators-start
# Collaborators: the science lives here, not in the graph body.


@calcfunction
def _step_toward(value: Int, target: Int) -> Int:
    """Move one unit toward the target; staying put at the target avoids overshoot."""
    if value.value < target.value:
        return Int(value.value + 1)
    if value.value > target.value:
        return Int(value.value - 1)
    return Int(value.value)


class StepWorkChain(WorkChain):
    """Take one scientific step toward the target through a single outline step."""

    @classmethod
    def define(cls, spec) -> None:  # type: ignore[override]
        super().define(spec)
        spec.input('value', valid_type=Int)
        spec.input('target', valid_type=Int)
        spec.output('result', valid_type=Int)
        spec.outline(cls.take_step)

    def take_step(self):
        """Store the stepped value."""
        self.out('result', _step_toward(self.inputs.value, self.inputs.target))


step_chain = task(StepWorkChain)


@task
def summarize(value: int, target: int, attempts: int, status: str) -> dict[str, int | str]:
    """Record the final value, target, attempts, and status as one report."""
    return {'value': value, 'target': target, 'attempts': attempts, 'status': status}


# recipe-collaborators-end

# recipe-graph-start
# Main graph: orchestration only; the science stays in the collaborators above.


@graph
def converge(initial: int, target: int, max_steps: int) -> dict[str, int | str]:
    """Step ``initial`` toward ``target`` at most ``max_steps`` times, then report."""
    value = initial
    attempts = 0
    while value != target and attempts < max_steps:
        step = step_chain(value=value, target=target)
        value = step.result
        attempts = attempts + 1
    if value == target:
        status = 'converged'
    else:
        status = 'exhausted'
    report = summarize(value=value, target=target, attempts=attempts, status=status)
    return report


# recipe-graph-end


def test_build_validates_without_running_tasks() -> None:
    """``build()`` validates without launching anything, so call it first while authoring."""
    assert converge.build() is None


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize(
    'inputs, expected',
    [
        pytest.param(
            {'initial': 0, 'target': 3, 'max_steps': 10},
            {'value': 3, 'target': 3, 'attempts': 3, 'status': 'converged'},
            id='ascending',
        ),
        pytest.param(
            {'initial': 2, 'target': -1, 'max_steps': 10},
            {'value': -1, 'target': -1, 'attempts': 3, 'status': 'converged'},
            id='descending-negative',
        ),
        pytest.param(
            {'initial': 0, 'target': 5, 'max_steps': 2},
            {'value': 2, 'target': 5, 'attempts': 2, 'status': 'exhausted'},
            id='exhausted',
        ),
        pytest.param(
            {'initial': 4, 'target': 4, 'max_steps': 5},
            {'value': 4, 'target': 4, 'attempts': 0, 'status': 'converged'},
            id='initially-converged',
        ),
        pytest.param(
            {'initial': 0, 'target': 3, 'max_steps': 0},
            {'value': 0, 'target': 3, 'attempts': 0, 'status': 'exhausted'},
            id='zero-budget',
        ),
    ],
)
def test_converge_reports_final_state(inputs: dict, expected: dict) -> None:
    """Ascending, descending/negative, exhaustion, initially-converged, and zero-budget cases report."""
    outputs, node = run_get_node(converge, **inputs)
    assert node.is_finished_ok
    assert isinstance(outputs['result'], Dict)
    assert outputs['result'].get_dict() == expected


@pytest.mark.usefixtures('aiida_profile_clean')
def test_converge_returns_existing_summary_node() -> None:
    """The root returns the summary task output; the science ran in the ``WorkChain`` child."""
    outputs, node = run_get_node(converge, initial=0, target=2, max_steps=10)
    assert node.is_finished_ok
    assert node.base.links.get_outgoing(link_type=LinkType.CREATE).all() == []
    children = node.called_descendants
    assert any(child.process_label == 'StepWorkChain' and child.is_finished_ok for child in children)
    assert any(child.process_label == '_step_toward' and child.is_finished_ok for child in children)
    summary_node = next(child for child in children if child.process_label == 'summarize')
    assert outputs['result'].uuid == summary_node.outputs.result.uuid


@pytest.mark.usefixtures('aiida_profile_clean')
def test_root_inputs_are_flat_workflow_arguments() -> None:
    """The stored graph node keeps flat workflow arguments: ``root.inputs.initial``."""
    initial = Int(3)
    _outputs, node = run_get_node(converge, initial=initial, target=0, max_steps=10)
    assert node.is_finished_ok
    assert node.inputs.initial.uuid == initial.uuid
