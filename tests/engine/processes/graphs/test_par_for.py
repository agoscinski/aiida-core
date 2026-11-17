###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Execution contracts for `par_for` parallel fan-out over owned containers.

Covers list/dict fan-out with order and key preservation, empty collections,
downstream chaining, serialized IR execution, `CalcJob` and `WorkChain` tasks
with a shared `runtime_config` entry, provenance (graphs never `CREATE`
result containers), failure propagation, scheduler fan-out, checkpoint
restoration, and build rejections including `@graph` callables.
"""

import json
import typing as t

import pytest

from aiida.common.exceptions import GraphLanguageError, GraphTypeCheckError, GraphValidationError
from aiida.common.links import LinkType
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.ir import GraphIR
from aiida.engine.processes.graphs.lowering import GraphHandle, compile_graph
from aiida.engine.processes.graphs.process import GraphProcess
from aiida.engine.processes.graphs.run import GraphRun
from aiida.orm import DataDict, DataList
from aiida.orm.nodes.data.int import Int
from aiida.orm.nodes.process.process import ProcessNode
from tests.engine.processes.graphs.programs import par_fors

pytestmark = pytest.mark.presto


def _launch(
    declaration: GraphHandle, inputs: dict[str, t.Any], serialized: bool
) -> tuple[dict[str, t.Any], ProcessNode]:
    """Run a graph declaration directly or from its serialized IR."""
    if serialized:
        spec = GraphIR.from_dict(json.loads(json.dumps(compile_graph(declaration).to_dict())))
        return run_get_node(GraphProcess, **GraphProcess._launch_inputs(spec, inputs))
    return run_get_node(declaration, **inputs)


@pytest.fixture
def executable(aiida_code_installed):
    """Return a localhost code running the arithmetic add executable."""
    return aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('serialized', [pytest.param(False, id='declaration'), pytest.param(True, id='serialized')])
def test_for_list_executes(serialized: bool) -> None:
    """A task fanned out over a list increments every element in order."""
    original = DataList([Int(1), Int(2), Int(3)])
    outputs, node = _launch(par_fors.for_list, {'values': original}, serialized)
    assert node.is_finished_ok
    result = outputs['result']
    assert isinstance(result, DataList)
    assert [member.value for member in result] == [2, 3, 4]


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('serialized', [pytest.param(False, id='declaration'), pytest.param(True, id='serialized')])
def test_for_mapping_executes(serialized: bool) -> None:
    """A task fanned out over a mapping increments values while keeping keys and order."""
    original = DataDict({'zeta': Int(1), 'alpha': Int(2), 'mid': Int(3)})
    outputs, node = _launch(par_fors.for_mapping, {'values': original}, serialized)
    assert node.is_finished_ok
    result = outputs['result']
    assert isinstance(result, DataDict)
    assert list(result) == ['zeta', 'alpha', 'mid']
    assert {key: member.value for key, member in result.items()} == {'zeta': 2, 'alpha': 3, 'mid': 4}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_empty_collection_executes() -> None:
    """Fanning out over an empty list returns an empty stored list with unset element type."""
    outputs, node = run_get_node(par_fors.for_empty, values=DataList())
    assert node.is_finished_ok
    result = outputs['result']
    assert isinstance(result, DataList)
    assert len(result) == 0
    assert result.element_type is None
    assert result.is_stored


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_downstream_chaining_executes() -> None:
    """A fanned container feeds a downstream task unchanged."""
    original = DataList([Int(1), Int(2), Int(3)])
    outputs, node = run_get_node(par_fors.for_chained, values=original)
    assert node.is_finished_ok
    assert outputs['result'].value == 9
    calls: dict[str, ProcessNode] = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert 'total' in calls
    assert calls['total'].inputs.values.uuid == calls['for_increment_collect'].outputs.result.uuid


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_keyword_and_after_execute() -> None:
    """Keyword binding and `after` ordering execute with exact values."""
    original = DataList([Int(4)])
    outputs, node = run_get_node(par_fors.for_keyword, values=original)
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == [5]
    outputs, node = run_get_node(par_fors.for_after, x=10, values=original)
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == [5]
    assert any(
        edge.source == 'increment' and edge.target == 'for_increment' and edge.carried_between is None
        for edge in compile_graph(par_fors.for_after).dependencies
    )


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_workchain_executes() -> None:
    """A `WorkChain` fanned out over a list runs one child workflow per element."""
    original = DataList([Int(1), Int(2)])
    outputs, node = run_get_node(
        par_fors.for_chain,
        values=original,
        runtime_config={'for_increment_chain': {'metadata': {'label': 'fanned'}}},
    )
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == [2, 3]
    calls: dict[str, ProcessNode] = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert set(calls) == {'for_increment_chain_0', 'for_increment_chain_1'}
    assert all(child.label == 'fanned' for child in calls.values())
    assert node.base.links.get_outgoing(link_type=LinkType.CREATE).all() == []


def test_for_calc_fan_out_uses_shared_config(executable) -> None:
    """A `CalcJob` fanned out over a list launches every item with the shared `code`."""
    original = DataList([Int(1), Int(2), Int(3)])
    outputs, node = run_get_node(
        par_fors.for_calc,
        values=original,
        runtime_config={
            'for_increment_calc': {
                'code': executable,
                'metadata': {'options': {'custom_scheduler_commands': '# fan-out'}},
            }
        },
    )
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == [2, 3, 4]
    calls: dict[str, ProcessNode] = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert set(calls) == {
        'for_increment_calc_0',
        'for_increment_calc_1',
        'for_increment_calc_2',
        'for_increment_calc_collect',
    }
    items = [calls[f'for_increment_calc_{index}'] for index in range(3)]
    assert all(child.inputs.code.uuid == executable.uuid for child in items)
    assert all(child.get_option('custom_scheduler_commands') == '# fan-out' for child in items)
    assert node.base.links.get_outgoing(link_type=LinkType.CREATE).all() == []


def test_for_calc_fan_out_requires_code(aiida_profile) -> None:
    """Fanning out a `CalcJob` without its shared `code` fails launch validation."""
    with pytest.raises(GraphValidationError, match='code'):
        par_fors.for_calc.get_launch_inputs(values=DataList([Int(1)]))


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('flag, expected', [(True, [2, 3]), (False, [1, 2])])
def test_for_in_branch_executes(flag: bool, expected: list[int]) -> None:
    """A fan-out in a branch arm runs for the taken side and passes through otherwise."""
    original = DataList([Int(1), Int(2)])
    outputs, node = run_get_node(par_fors.for_in_branch, values=original, flag=flag)
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == expected


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_in_nested_graph_executes() -> None:
    """A fan-out inside a nested graph dispatches with hierarchical call labels."""
    original = DataList([Int(1), Int(2)])
    outputs, node = run_get_node(par_fors.for_nested, values=original)
    assert node.is_finished_ok
    assert [member.value for member in outputs['result']] == [2, 3]
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all_link_labels()
    assert set(calls) == {
        'for_wrapped__for_increment_0',
        'for_wrapped__for_increment_1',
        'for_wrapped__for_increment_collect',
    }


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_provenance() -> None:
    """The graph never creates the result container; the collector calculation does."""
    original = DataList([Int(1), Int(2)])
    outputs, node = run_get_node(par_fors.for_list, values=original)
    assert node.is_finished_ok
    result = outputs['result']
    assert isinstance(result, DataList)
    assert node.base.links.get_outgoing(link_type=LinkType.CREATE).all() == []
    calls: dict[str, ProcessNode] = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert set(calls) == {'for_increment_0', 'for_increment_1', 'for_increment_collect'}
    collected = calls['for_increment_collect'].outputs.result
    assert isinstance(collected, DataList)
    assert [member.value for member in collected] == [2, 3]
    assert result.uuid == collected.uuid


@pytest.mark.usefixtures('aiida_profile_clean')
def test_for_failure_fails_parent() -> None:
    """A failing item fails the fan-out and the graph without ever collecting."""
    original = DataList([Int(1), Int(2), Int(3)])
    outputs, node = run_get_node(par_fors.for_fails, values=original)
    assert not node.is_finished_ok
    assert node.exit_status == 400
    assert outputs == {}
    calls: dict[str, ProcessNode] = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    assert set(calls) == {'for_explode_on_two_0', 'for_explode_on_two_1', 'for_explode_on_two_2'}
    assert not calls['for_explode_on_two_1'].is_finished_ok


@pytest.mark.usefixtures('aiida_profile_clean')
def test_par_for_direct_call_fails() -> None:
    """Calling `par_for` outside a graph body names the authoring fix."""
    from aiida.common.exceptions import GraphLanguageError
    from aiida.engine import par_for

    with pytest.raises(GraphLanguageError, match='inside a `@graph` body'):
        par_for(par_fors.increment, DataList([Int(1)]))


@pytest.mark.parametrize(
    'declaration, match',
    [
        pytest.param(par_fors.for_graph, 'double_graph', id='graph'),
        pytest.param(par_fors.for_aliased_graph, 'double_graph', id='aliased-graph'),
        pytest.param(par_fors.for_two_inputs, 'exactly one', id='non-unary'),
        pytest.param(par_fors.for_pair_outputs, 'single output', id='multi-output'),
        pytest.param(par_fors.for_plain_function, 'registered declarations', id='unregistered'),
        pytest.param(par_fors.for_bare_calc, 'outputs=', id='unselected-multi-output'),
        pytest.param(par_fors.for_bad_collection, 'annotate the collection', id='bad-collection'),
        pytest.param(par_fors.for_missing_collection, 'with both', id='missing-collection'),
    ],
)
def test_for_invalid_declarations_rejected(declaration: GraphHandle, match: str) -> None:
    """Unsupported fan-outs fail at lowering with an actionable message."""
    with pytest.raises((GraphLanguageError, GraphTypeCheckError), match=match):
        declaration.build()


@pytest.mark.usefixtures('aiida_profile_clean')
def test_scheduler_dispatches_all_items_before_waiting() -> None:
    """One scheduling step fans out to every item before any completion."""
    original = DataList([Int(1), Int(2), Int(3)])
    state = GraphRun(compile_graph(par_fors.for_list), given={'values': original})
    step = state.step()
    assert [start.instance for start in step.starts] == ['for_increment_0', 'for_increment_1', 'for_increment_2']
    assert [start.config_path for start in step.starts] == [('for_increment',)] * 3
    assert state.pending == {}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_scheduler_checkpoint_restores_expansion() -> None:
    """Checkpointing between fan-out and completion resumes without redispatching."""
    original = DataList([Int(1), Int(2)])
    state = GraphRun(compile_graph(par_fors.for_list), given={'values': original})
    state.step()
    _, first_node = par_fors.increment.run_get_node(x=1)
    _, second_node = par_fors.increment.run_get_node(x=2)
    state.started('for_increment_0', first_node.pk)
    state.started('for_increment_1', second_node.pk)
    restored = GraphRun.from_dict(state.graph, state.given, state.to_dict())
    assert restored.step().starts == []
    restored.completed('for_increment_0', first_node.pk)
    restored.completed('for_increment_1', second_node.pk)
    step = restored.step()
    assert [start.instance for start in step.starts] == ['for_increment_collect']
    assert step.starts[0].inputs['item_0'].value == 2
    assert step.starts[0].inputs['item_1'].value == 3
