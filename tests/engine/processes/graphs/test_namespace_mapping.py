###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Build and dispatch contracts for `DataDict` wires forwarded into dynamic namespaces.

A bare `DataDict` in ``task(..., inputs=...)`` marks a dynamic namespace as
container-forwarded: the graph wires one `DataDict` there, and dispatch expands its
members into the namespace. These tests pin the declaration, build, expansion, and
execution contracts, including actual `@graph` runs through the dispatch hook in
`GraphRun._inputs_for`.
"""

import json

import pytest

from aiida import orm
from aiida.common.exceptions import GraphTypeCheckError, GraphValidationError
from aiida.common.links import LinkType
from aiida.engine import WorkChain, task
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.ir import GraphIR, ProcessTask
from aiida.engine.processes.graphs.lowering import compile_graph
from aiida.engine.processes.ports import InputPort
from aiida.orm import DataDict, Int, Str
from tests.engine.processes.graphs.programs import namespace_mapping

pytestmark = pytest.mark.presto


@pytest.mark.parametrize(
    'declaration',
    [
        pytest.param(namespace_mapping.forward_flat, id='flat'),
        pytest.param(namespace_mapping.forward_optional, id='optional'),
        pytest.param(namespace_mapping.forward_nested, id='nested'),
        pytest.param(namespace_mapping.forward_chained, id='chained'),
        pytest.param(namespace_mapping.forward_calc, id='calcjob'),
        pytest.param(namespace_mapping.static_nested, id='static-nested'),
    ],
)
def test_forwarding_graphs_build(declaration) -> None:
    """Forwarded and nested-static wirings pass static validation without running tasks."""
    assert declaration.build() is None


def test_flat_forwarded_declaration_closes() -> None:
    """A bare `DataDict` closes a dynamic namespace into a static `DataDict` leaf."""
    spec = namespace_mapping.flat_gather._task_spec
    assert spec.forwarded == ('members',)
    port = spec.inputs['members']
    assert isinstance(port, InputPort)
    assert port.valid_type == (DataDict,)
    assert port.required
    assert spec.input_shape.select('members').types == (DataDict,)


def test_nested_forwarded_declaration_closes() -> None:
    """A nested bare `DataDict` closes every enclosing namespace, like nested static names."""
    assert namespace_mapping.NestedGather.spec().inputs['sub'].dynamic
    spec = namespace_mapping.nested_gather._task_spec
    assert spec.forwarded == ('sub.members',)
    assert not spec.inputs['sub'].dynamic
    assert spec.inputs['sub']['members'].valid_type == (DataDict,)


def test_static_declarations_still_close() -> None:
    """Static names keep working beside forwarding and record no expansion path."""
    assert namespace_mapping.static_members._task_spec.forwarded == ()
    assert namespace_mapping.static_nested_gather._task_spec.forwarded == ()
    closed = namespace_mapping.static_nested_gather._task_spec.inputs
    assert not closed['sub'].dynamic
    assert not closed['sub']['members'].dynamic


def test_forwarded_declaration_survives_serialization() -> None:
    """Forwarded paths persist through the stored declaration round trip."""
    template = compile_graph(namespace_mapping.forward_nested)
    assert [task.spec.forwarded for task in template.tasks if isinstance(task, ProcessTask)] == [('sub.members',)]
    revived = GraphIR.from_dict(json.loads(json.dumps(template.to_dict())))
    assert [task.spec.forwarded for task in revived.tasks if isinstance(task, ProcessTask)] == [('sub.members',)]
    assert revived.task_names == template.task_names


@pytest.mark.parametrize(
    'target, inputs, reason',
    [
        pytest.param(namespace_mapping.ItemGather, {'factor': DataDict}, 'already declared port', id='static-leaf'),
        pytest.param(
            namespace_mapping.NestedGather,
            {'sub': {'factor': DataDict}},
            'already declared port',
            id='nested-static-leaf',
        ),
        pytest.param(namespace_mapping.ItemGather, {'nope': DataDict}, 'no process input', id='unknown-path'),
        pytest.param(
            namespace_mapping.ItemGather, {'members': dict}, 'takes `DataNode` subclasses', id='plain-dict-loophole'
        ),
        pytest.param(
            namespace_mapping.ItemGather,
            {'members': {'a': str}},
            'takes `DataNode` subclasses',
            id='non-node-member',
        ),
    ],
)
def test_forwarding_rejects_misuse(target, inputs, reason: str) -> None:
    """Forwarding declares bare `DataDict` only at dynamic namespaces, nothing else."""
    with pytest.raises(GraphTypeCheckError, match=reason):
        task(target, inputs=inputs)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_flat_preserves_identities_and_validates() -> None:
    """Expansion unpacks members with their identities and validates against the live spec."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    factor = Int(3)
    factor.store()
    raw = {'members': bundle, 'factor': factor}
    expanded = namespace_mapping.flat_gather._task_spec.expand_forwarded(raw)
    assert set(expanded['members']) == {'a', 'b'}
    assert expanded['members']['a'] is bundle['a']
    assert expanded['members']['a'].uuid == bundle['a'].uuid
    assert expanded['factor'] is factor
    assert raw == {'members': bundle, 'factor': factor}
    assert raw['members'] is bundle
    assert namespace_mapping.ItemGather.spec().inputs.validate(expanded) is None


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_nested_preserves_identities_and_validates() -> None:
    """Nested expansion unpacks under the enclosing namespace and validates live."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    factor = Int(3)
    factor.store()
    raw = {'sub': {'members': bundle, 'factor': factor}}
    expanded = namespace_mapping.nested_gather._task_spec.expand_forwarded(raw)
    assert expanded['sub']['members']['b'] is bundle['b']
    assert expanded['sub']['factor'] is factor
    assert raw['sub']['members'] is bundle
    assert namespace_mapping.NestedGather.spec().inputs.validate(expanded) is None


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_rejects_wrong_member_type() -> None:
    """Members outside the namespace's accepted types fail loudly, naming the member."""
    bundle = DataDict({'a': Str('oops')})
    bundle.store()
    with pytest.raises(GraphValidationError, match=r'member `a`.*outside the accepted `Int`'):
        namespace_mapping.flat_gather._task_spec.expand_forwarded({'members': bundle, 'factor': Int(1)})


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_rejects_empty_required_container() -> None:
    """An empty wire on a required namespace fails instead of launching without entries."""
    empty = DataDict()
    empty.store()
    with pytest.raises(GraphValidationError, match='empty'):
        namespace_mapping.flat_gather._task_spec.expand_forwarded({'members': empty, 'factor': Int(1)})


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_drops_empty_optional_container() -> None:
    """An empty wire on a non-required namespace leaves the input unwired."""
    bundle = DataDict({'a': Int(1)})
    bundle.store()
    empty = DataDict()
    empty.store()
    expanded = namespace_mapping.optional_gather._task_spec.expand_forwarded(
        {'members': bundle, 'extras': empty, 'factor': Int(1)}
    )
    assert 'extras' not in expanded
    assert namespace_mapping.ItemGather.spec().inputs.validate(expanded) is None


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_rejects_non_container_wire() -> None:
    """Only a `DataDict` node expands; a bare node at a forwarded path fails loudly."""
    with pytest.raises(GraphValidationError, match=r'takes a `DataDict` wire'):
        namespace_mapping.flat_gather._task_spec.expand_forwarded({'members': Int(1), 'factor': Int(1)})


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expand_rejects_missing_required_wire() -> None:
    """A missing wire on a required path fails instead of launching without entries."""
    with pytest.raises(GraphValidationError, match='required but missing'):
        namespace_mapping.flat_gather._task_spec.expand_forwarded({'factor': Int(1)})


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expanded_flat_inputs_run() -> None:
    """Expanded inputs launch the real process: scaled sum plus the static shift."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    expanded = namespace_mapping.flat_gather._task_spec.expand_forwarded({'members': bundle, 'factor': Int(3)})
    outputs, node = run_get_node(namespace_mapping.ItemGather, **expanded)
    assert node.is_finished_ok
    assert outputs['total'].value == 9


@pytest.mark.usefixtures('aiida_profile_clean')
def test_expanded_nested_inputs_run() -> None:
    """Expanded nested inputs launch the real exposed process with its defaults filled."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    expanded = namespace_mapping.nested_gather._task_spec.expand_forwarded(
        {'sub': {'members': bundle, 'factor': Int(3)}}
    )
    outputs, node = run_get_node(namespace_mapping.NestedGather, **expanded)
    assert node.is_finished_ok
    assert outputs['total'].value == 9


@pytest.mark.parametrize(
    'declaration, forwarded',
    [
        pytest.param(namespace_mapping.forward_flat, [('members',)], id='flat'),
        pytest.param(namespace_mapping.forward_optional, [('members', 'extras')], id='optional'),
        pytest.param(namespace_mapping.forward_nested, [('sub.members',)], id='nested'),
        pytest.param(namespace_mapping.forward_chained, [(), ('members',)], id='chained'),
        pytest.param(namespace_mapping.forward_calc, [('members',)], id='calcjob'),
    ],
)
def test_forwarding_templates_round_trip(declaration, forwarded) -> None:
    """Every forwarding graph survives the stored declaration round trip with its expansion paths."""
    template = compile_graph(declaration)
    assert [task.spec.forwarded for task in template.tasks if isinstance(task, ProcessTask)] == forwarded
    revived = GraphIR.from_dict(json.loads(json.dumps(template.to_dict())))
    assert [task.spec.forwarded for task in revived.tasks if isinstance(task, ProcessTask)] == forwarded
    assert revived.task_names == template.task_names


def _member_values(node, *path: str) -> dict[str, int]:
    """Return the plain values behind a child namespace, following one attribute path."""
    target = node.inputs
    for name in path:
        target = getattr(target, name)
    return {key: target[key].value for key in target}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_flat_graph_run() -> None:
    """An actual graph run forwards one `DataDict` wire into the dynamic namespace."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    outputs, node = run_get_node(namespace_mapping.forward_flat, items=bundle, factor=Int(3))
    assert node.is_finished_ok
    assert outputs['result'].value == 9
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert sorted(calls) == ['flat_gather']
    assert _member_values(calls['flat_gather'], 'members') == {'a': 1, 'b': 2}
    assert calls['flat_gather'].inputs.factor.value == 3
    # The wire keeps its entries; dispatch reads the container without consuming it.
    assert {key: bundle[key].value for key in bundle} == {'a': 1, 'b': 2}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_nested_graph_run() -> None:
    """An actual graph run forwards one `DataDict` wire into a nested dynamic namespace."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    outputs, node = run_get_node(namespace_mapping.forward_nested, items=bundle, factor=Int(3))
    assert node.is_finished_ok
    assert outputs['result'].value == 9
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert sorted(calls) == ['nested_gather']
    assert _member_values(calls['nested_gather'], 'sub', 'members') == {'a': 1, 'b': 2}
    assert calls['nested_gather'].inputs.sub.factor.value == 3


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_chained_graph_run() -> None:
    """An actual graph run forwards a task-produced `DataDict` into the dynamic namespace."""
    outputs, node = run_get_node(namespace_mapping.forward_chained, count=3, factor=2)
    assert node.is_finished_ok
    assert outputs['result'].value == 6
    calcs = node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).nested()
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert sorted(calcs) == ['make_bundle']
    assert sorted(calls) == ['flat_gather']
    produced = {key: calcs['make_bundle'].outputs.result[key].value for key in calcs['make_bundle'].outputs.result}
    assert produced == {'item_0': 0, 'item_1': 1, 'item_2': 2}
    assert _member_values(calls['flat_gather'], 'members') == produced


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_optional_graph_run() -> None:
    """An actual graph run forwards two `DataDict` wires, one into an optional namespace."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    extras = DataDict({'c': Int(10)})
    extras.store()
    outputs, node = run_get_node(namespace_mapping.forward_optional, items=bundle, extras=extras, factor=Int(3))
    assert node.is_finished_ok
    assert outputs['result'].value == 39
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert sorted(calls) == ['optional_gather']
    assert _member_values(calls['optional_gather'], 'members') == {'a': 1, 'b': 2}
    assert _member_values(calls['optional_gather'], 'extras') == {'c': 10}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_optional_empty_graph_run() -> None:
    """An empty wire on an optional namespace leaves the input unwired at dispatch."""
    bundle = DataDict({'a': Int(1), 'b': Int(2)})
    bundle.store()
    empty = DataDict()
    empty.store()
    outputs, node = run_get_node(namespace_mapping.forward_optional, items=bundle, extras=empty, factor=Int(3))
    assert node.is_finished_ok
    assert outputs['result'].value == 9
    calls = node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).nested()
    assert sorted(calls) == ['optional_gather']
    assert 'extras' not in calls['optional_gather'].inputs


@pytest.mark.usefixtures('aiida_profile_clean')
def test_forward_flat_rejects_wrong_member_before_dispatch() -> None:
    """A wrong-typed member fails the graph run before any child dispatches.

    Containers lock their element type, so a mixed bundle cannot even be built: the
    rejection path takes a homogeneously wrong-typed `DataDict`.
    """
    bundle = DataDict({'a': Str('oops')})
    bundle.store()
    with pytest.raises(GraphValidationError, match=r'member `a`.*outside the accepted `Int`'):
        run_get_node(namespace_mapping.forward_flat, items=bundle, factor=Int(3))
    assert orm.QueryBuilder().append(orm.WorkChainNode).count() == 0
    assert orm.QueryBuilder().append(orm.CalcFunctionNode).count() == 0


def test_expand_drops_missing_optional_nested_wire() -> None:
    """A missing wire on an optional nested path leaves the input unwired, not empty."""

    class OptNested(WorkChain):
        """Expose one gather under an optional ``sub`` namespace."""

        @classmethod
        def define(cls, spec) -> None:  # type: ignore[override]
            super().define(spec)
            spec.expose_inputs(
                namespace_mapping.ItemGather,
                namespace='sub',
                include=('members', 'factor'),
                namespace_options={'required': False},
            )

    assert not OptNested.spec().inputs['sub'].required
    handle = task(OptNested, inputs={'sub': {'members': DataDict}})
    assert handle._task_spec.forwarded == ('sub.members',)
    assert handle._task_spec.expand_forwarded({}) == {}
    assert OptNested.spec().inputs.validate({}) is None
