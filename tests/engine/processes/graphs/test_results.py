###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Declared task failures in source contracts, serialized IR and process runs."""

import json

import pytest

from aiida.common.exceptions import GraphTypeCheckError, GraphValidationError
from aiida.engine import Err, ExitCode, ExitCodeEnum, GraphProcess, Ok, Result, run_get_node
from aiida.engine.processes.graphs.results import result_contract, result_types
from aiida.engine.processes.graphs.shapes import LeafShape, NamespaceShape
from aiida.engine.processes.graphs.spec import GraphSpec, TaskSpec
from aiida.engine.processes.graphs.tasks import build_task
from tests.engine.processes.graphs import declarations


def test_result_contract_in_ir() -> None:
    spec = declarations.checked_pipeline.build()
    task = spec.tasks[0].spec
    assert task.exit_codes == {member.name: member.value for member in declarations.TaskErrors}
    assert task.outputs['value'].valid_type == (declarations.Int,)
    assert {edge.source_port for edge in spec.dependencies} == {'value', 'error'}
    shape = task.result_shape
    assert isinstance(shape, NamespaceShape)
    assert shape.select('value') == LeafShape(types=(declarations.Int, type(None)), required=False)
    assert shape.select('error') == LeafShape(types=(declarations.TaskErrors, type(None)), required=False)
    assert GraphSpec.from_dict(json.loads(json.dumps(spec.to_dict()))).to_dict() == spec.to_dict()
    assert TaskSpec.from_dict(json.loads(json.dumps(task.to_dict()))).exit_codes == task.exit_codes
    for label, code in task.exit_codes.items():
        assert declarations.checked_value.spec().exit_codes[label] == code
    assert list(declarations.checked_pair.spec().outputs['value']) == ['value', 'double']
    assert list(declarations.checked_empty.spec().outputs) == ['value', 'error']


def test_model_and_union_contract_round_trip() -> None:
    annotation = Result[declarations.ResultValue, declarations.TaskErrors | declarations.InputErrors]
    assert result_types(annotation) == (
        (declarations.ResultValue, type(None)),
        (declarations.TaskErrors, declarations.InputErrors, type(None)),
    )
    spec = declarations.model_result.build()
    task = spec.tasks[0].spec
    assert set(task.exit_codes) == {'TaskErrors_NEGATIVE', 'TaskErrors_ZERO', 'InputErrors_ZERO'}
    shape = task.result_shape
    assert isinstance(shape, NamespaceShape)
    assert isinstance(shape.select('value'), NamespaceShape)
    assert not shape.select('value').required
    assert shape.select('error') == LeafShape(
        types=(declarations.TaskErrors, declarations.InputErrors, type(None)),
        required=False,
    )
    assert GraphSpec.from_dict(json.loads(json.dumps(spec.to_dict()))).to_dict() == spec.to_dict()
    assert spec.tasks[1].condition_test == 'is_not_none'


@pytest.mark.parametrize('errors', [declarations.TaskErrors | int, declarations.TaskErrors | None])
def test_union_requires_only_error_enums(errors) -> None:
    with pytest.raises(GraphTypeCheckError, match='non-empty ExitCodeEnum'):
        result_contract(Result[int, errors])


def test_union_rejects_duplicate_statuses() -> None:
    first = ExitCodeEnum('First', {'FAILURE': ExitCode(410, 'first')})
    second = ExitCodeEnum('Second', {'FAILURE': ExitCode(410, 'second')})
    with pytest.raises(GraphTypeCheckError, match='duplicate exit status'):
        result_contract(Result[int, first | second])


def test_legacy_task_ir_without_exit_codes() -> None:
    data = declarations.add.task_spec.to_dict()
    data.pop('exit_codes')
    assert TaskSpec.from_dict(data).exit_codes == {}


@pytest.mark.parametrize(
    'value, message',
    [
        (400, 'must be `ExitCode` values'),
        (ExitCode(0, 'success'), 'positive exit status'),
        (ExitCode(-1, 'negative'), 'positive exit status'),
        (ExitCode(400), 'string message'),
        (ExitCode(400, 'ok', invalidates_cache='yes'), 'boolean cache flag'),
    ],
)
def test_invalid_error_values_rejected_at_definition(value, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        ExitCodeEnum('Errors', {'BAD': value})


def test_empty_error_enum_rejected_at_contract() -> None:
    errors = ExitCodeEnum('Errors', {})
    with pytest.raises(GraphTypeCheckError, match='non-empty ExitCodeEnum'):
        result_contract(Result[int, errors])


def test_duplicate_error_status_rejected_at_contract() -> None:
    first = ExitCodeEnum('First', {'FAILURE': ExitCode(400, 'a')})
    second = ExitCodeEnum('Second', {'FAILURE': ExitCode(400, 'b')})
    with pytest.raises(GraphTypeCheckError, match='duplicate exit status'):
        result_contract(Result[int, first | second])


@pytest.mark.parametrize('annotation', [Result, Result[int, ExitCode], Result[int, int]])
def test_result_requires_declared_errors(annotation) -> None:
    with pytest.raises(GraphTypeCheckError, match='Result'):
        result_contract(annotation)


def test_result_success_contract_supports_namespaces() -> None:
    def function() -> Result[declarations.Settings, declarations.TaskErrors]:
        raise AssertionError('building must not execute the body')

    handle = build_task(function)
    assert set(handle.spec().outputs['value']) == {'count', 'nullable'}
    assert result_types(Result[declarations.Settings, declarations.TaskErrors]) == (
        (declarations.Settings, type(None)),
        (declarations.TaskErrors, type(None)),
    )


def test_result_rejects_nested_results() -> None:
    with pytest.raises(GraphTypeCheckError, match='nested Result'):
        result_contract(Result[Result[int, declarations.TaskErrors], declarations.TaskErrors])


@pytest.mark.parametrize(
    'label, code', [('CUSTOM', ExitCode(1, 'reserved status')), ('ERROR_UNSPECIFIED', ExitCode(410, 'reserved label'))]
)
def test_engine_exit_code_conflicts(label: str, code: ExitCode) -> None:
    errors = ExitCodeEnum('Errors', {label: code})

    def function() -> Result[int, errors]:
        raise AssertionError('building must not execute the body')

    with pytest.raises(GraphTypeCheckError, match='conflicts with an engine exit code'):
        build_task(function)


@pytest.mark.parametrize(
    'function, result, message',
    [
        (declarations.add, ExitCode(999, 'undeclared'), 'must be declared'),
        (declarations.add, Err(declarations.TaskErrors.ZERO), 'must be declared'),
        (declarations.checked_value, Ok(ExitCode(999, 'undeclared')), 'Ok must contain task outputs'),
        (declarations.checked_value, Ok(Ok(1)), 'Ok must contain task outputs'),
        (declarations.checked_value, Ok(None), 'successful Result type does not allow None'),
        (declarations.checked_model, Ok(None), 'successful Result type does not allow None'),
        (declarations.checked_value, Err(ExitCode(410, 'wrong cache flag')), 'undeclared exit code'),
        (
            declarations.checked_value,
            Err(ExitCodeEnum('OtherErrors', {'ZERO': ExitCode(412, 'other')}).ZERO),
            'undeclared enum',
        ),
    ],
)
def test_runtime_contract_validation(function, result, message: str) -> None:
    # Normalization itself does not require a database or runner.
    process = object.__new__(function.process_class)
    with pytest.raises(GraphValidationError, match=message):
        process._prepare_result(result)


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('value, status, message', [(0, 411, 'Zero value'), (-2, 410, 'Negative value: -2')])
def test_declared_error_is_available_to_graph(value: int, status: int, message: str) -> None:
    outputs, node = run_get_node(declarations.checked_pipeline, value=value)
    assert outputs['result'].value == -status
    assert node.is_finished_ok
    assert len(node.called) == 2
    child = node.called[0]
    assert child.exit_status == status
    assert child.exit_message == message
    assert list(child.outputs) == ['error']
    assert child.outputs.error.get_member().value.status == status


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
def test_success_outputs_and_empty_result() -> None:
    outputs, node = run_get_node(declarations.checked_pipeline, value=3)
    assert node.is_finished_ok
    assert outputs['result'].value == 6
    outputs, node = declarations.checked_pair.run_get_node(value=3)
    assert node.is_finished_ok
    assert outputs['value']['value'].value == 3
    assert outputs['value']['double'].value == 6
    assert 'error' not in outputs
    outputs, node = declarations.checked_empty.run_get_node()
    assert node.is_finished_ok
    assert not outputs


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('serialized', [False, True])
@pytest.mark.parametrize(
    'declaration, value, expected',
    [
        (declarations.model_result, 3, 9),
        (declarations.model_result, 0, -412),
        (declarations.model_result, -2, -410),
        (declarations.optional_model_result, 3, 9),
        (declarations.optional_model_result, 0, 412),
        (declarations.optional_model_result, -2, 410),
        (declarations.bound_model_result, 3, 9),
        (declarations.bound_model_result, 0, -412),
        (declarations.bound_model_result, -2, -410),
        (declarations.model_leaf_result, 3, 6),
        (declarations.model_leaf_result, 0, -412),
        (declarations.model_leaf_result, -2, -410),
        (declarations.value_none_branch, 3, 3),
        (declarations.value_none_branch, 0, -411),
        (declarations.optional_result, 3, 3),
        (declarations.optional_result, 0, 411),
        (declarations.optional_result, -2, 410),
    ],
)
def test_graph_result_logic(declaration, value: int, expected: int, serialized: bool) -> None:
    if serialized:
        spec = GraphSpec.from_dict(json.loads(json.dumps(declaration.build().to_dict())))
        outputs, node = run_get_node(GraphProcess, **GraphProcess.launch_inputs(spec, {'value': value}))
    else:
        outputs, node = run_get_node(declaration, value=value)
    assert node.is_finished_ok
    assert outputs['result'].value == expected
    child = node.called[0]
    if value > 0:
        assert child.is_finished_ok
        assert 'value' in child.outputs and 'error' not in child.outputs
    else:
        assert child.is_failed
        assert 'error' in child.outputs and 'value' not in child.outputs


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('fail', [False, True])
def test_nullable_model_fields_and_arm(fail: bool) -> None:
    spec = GraphSpec.from_dict(json.loads(json.dumps(declarations.nullable_fields_result.build().to_dict())))
    outputs, node = run_get_node(GraphProcess, **GraphProcess.launch_inputs(spec, {'fail': fail}))
    assert node.is_finished_ok
    assert outputs['result'].value is True
    child = node.called[0]
    assert child.is_failed if fail else child.is_finished_ok
    assert list(child.outputs) == (['error'] if fail else [])


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('value', [3, 0, -2])
def test_graph_outputs_only_present_arm(value: int) -> None:
    spec = GraphSpec.from_dict(json.loads(json.dumps(declarations.model_result_outputs.build().to_dict())))
    assert not spec.output_spec()['count'].required
    assert not spec.output_spec()['error'].required
    outputs, node = run_get_node(GraphProcess, **GraphProcess.launch_inputs(spec, {'value': value}))
    assert node.is_finished_ok
    if value > 0:
        assert set(outputs) == {'count'}
        assert outputs['count'].value == value
    else:
        assert set(outputs) == {'error'}
        assert outputs['error'].get_member() in (declarations.TaskErrors.NEGATIVE, declarations.InputErrors.ZERO)


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
def test_model_success_stores_individual_fields() -> None:
    outputs, node = declarations.checked_model.run_get_node(value=3)
    assert node.is_finished_ok
    assert outputs['value']['count'].value == 3
    assert outputs['value']['settings']['count'].value == 6
    assert {entry.link_label for entry in node.base.links.get_outgoing().all()} == {
        'value__count',
        'value__settings__count',
    }


@pytest.mark.presto
@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize(
    'function, message',
    [
        (declarations.undeclared_error, 'undeclared exit code'),
        (declarations.missing_result_wrapper, 'must return Ok'),
    ],
)
def test_invalid_runtime_results(function, message: str) -> None:
    with pytest.raises(GraphValidationError, match=message):
        function.run_get_node(value=3)
