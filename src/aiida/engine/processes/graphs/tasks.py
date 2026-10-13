###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Task construction shared by source declarations and their process runtime."""

from __future__ import annotations

import functools
import inspect
import typing as t
from collections.abc import Callable, Mapping, Sequence

from aiida.common.exceptions import GraphError, GraphTypeCheckError, GraphValidationError
from aiida.common.lang import override
from aiida.engine.processes.exit_code import ExitCode, ExitCodeEnum
from aiida.engine.processes.functions import (
    FunctionProcessBase,
    P,
    ProcessFunctionType,
    declare_output_ports,
    process_function,
)
from aiida.engine.processes.generic.ports import UNSPECIFIED
from aiida.engine.processes.graphs.results import Err, Ok, Result, error_types, result_contract
from aiida.engine.processes.graphs.spec import TaskSpec, fixed_inputs
from aiida.engine.processes.port_model import _namespace_value, field_from_annotation, is_structured, nullable_model
from aiida.engine.processes.ports import OutputPort, PortNamespace
from aiida.engine.processes.process_spec import GraphProcessSpec, ProcessSpec
from aiida.orm import CalcFunctionNode, Data, EnumData, to_aiida_type

__all__ = ('TaskHandle', 'TaskProcess')

R = t.TypeVar('R')


class TaskProcess(FunctionProcessBase):
    """Task process with Python-valued inputs and static output contracts."""

    _spec_class = GraphProcessSpec

    @classmethod
    @override
    def _declare_input(
        cls,
        spec: ProcessSpec,
        parameter: inspect.Parameter,
        annotations: dict[str, t.Any],
        param_help_string: dict[str, str | None],
    ) -> None:
        assert isinstance(spec, GraphProcessSpec)
        spec.input_from_field(
            parameter.name,
            field_from_annotation(
                parameter.name,
                annotations.get(parameter.name),
                parameter.default if parameter.default is not parameter.empty else UNSPECIFIED,
                param_help_string.get(parameter.name, None),
            ),
        )

    @classmethod
    @override
    def _declare_outputs(cls, spec: ProcessSpec, outputs: Sequence[str] | None, annotations: dict[str, t.Any]) -> None:
        annotation, codes = result_contract(annotations.get('return'))
        for label, code in codes.items():
            if label in spec.exit_codes or any(existing.status == code.status for existing in spec.exit_codes.values()):
                msg = f'task exit code `{label}` ({code.status}) conflicts with an engine exit code.'
                raise GraphTypeCheckError(msg)
            assert code.message is not None
            spec.exit_code(code.status, label, code.message, invalidates_cache=code.invalidates_cache)
        if t.get_origin(annotations.get('return')) is Result:
            model = nullable_model(annotation) or annotation
            if is_structured(model):
                spec.output_namespace('value', required=False, nullable=True)
                spec.outputs_from(t.cast(type, model), prefix='value.')
            elif annotation is type(None):
                spec.output('value', valid_type=type(None), required=False)
            else:
                declare_output_ports(outputs, annotation, spec)
                ports = list(spec.outputs.items())
                for name, _ in ports:
                    del spec.outputs[name]
                if outputs is not None:
                    value = PortNamespace('value', required=False, nullable=True)
                    for name, port in ports:
                        value[name] = port
                    spec.outputs['value'] = value
                else:
                    port = ports[0][1]
                    spec.output('value', valid_type=port.valid_type, required=False)

            def nullable_leaves(namespace: PortNamespace) -> None:
                for child in namespace.values():
                    if isinstance(child, PortNamespace):
                        nullable_leaves(child)
                    else:
                        kinds = child.valid_type or ()
                        kinds = (kinds,) if isinstance(kinds, type) else kinds
                        if type(None) in kinds:
                            child.required = False

            # Null model fields are represented by absent links, not synthetic data nodes.
            nullable_leaves(spec.outputs)
            spec.output('error', valid_type=EnumData, required=False)
            spec.outputs.dynamic = False
            return
        if declare_output_ports(outputs, annotation, spec):
            spec.outputs.dynamic = False
        else:
            spec.outputs.valid_type = (Data, dict)

    @override
    def _prepare_result(self, result: t.Any) -> t.Any:
        annotation = t.get_type_hints(self._func, include_extras=True).get('return')
        _, codes = result_contract(annotation)
        if t.get_origin(annotation) is not Result:
            if isinstance(result, (ExitCode, Result)):
                msg = 'task exit codes must be declared in a Result return annotation.'
                raise GraphValidationError(msg)
            return super()._prepare_result(result)
        if isinstance(result, Ok):
            if isinstance(result.value, (ExitCode, Result)):
                msg = 'Ok must contain task outputs, not an exit code or another Result.'
                raise GraphValidationError(msg)
            success = t.get_args(annotation)[0]
            if result.value is None and success is not type(None) and type(None) not in t.get_args(success):
                msg = 'the successful Result type does not allow None.'
                raise GraphValidationError(msg)
            self._out_value(result.value)
            return ExitCode()
        if isinstance(result, Err):
            error = result.error
            if isinstance(error, ExitCodeEnum):
                if not isinstance(error, error_types(annotation)):
                    msg = f'task returned an error from an undeclared enum: {error!r}.'
                    raise GraphValidationError(msg)
                error = error.value
            if isinstance(error, ExitCode) and any(
                error.status == code.status and error.invalidates_cache == code.invalidates_cache
                for code in codes.values()
            ):
                member = next(
                    member for kind in error_types(annotation) for member in kind if member.value.status == error.status
                )
                self.out('error', to_aiida_type(member))
                return error
            msg = f'task returned an undeclared exit code: {error!r}.'
            raise GraphValidationError(msg)
        msg = 'a task annotated with Result must return Ok(outputs) or Err(error).'
        raise GraphValidationError(msg)

    def _out_value(self, value: t.Any) -> None:
        """Store the successful arm as leaf CREATE links, preserving model namespaces."""
        port = self.spec().outputs['value']
        assert isinstance(port, (OutputPort, PortNamespace))
        value = _namespace_value(value)
        if isinstance(port, PortNamespace) and value is not None and not isinstance(value, Mapping):
            names = list(port)
            values = value if isinstance(value, tuple) else (value,)
            if len(names) != len(values):
                msg = f'task declares {len(names)} value outputs but returned {len(values)} values.'
                raise GraphValidationError(msg)
            value = dict(zip(names, values, strict=True))
        stored: dict[str, Data] = {}

        def collect(port: OutputPort | PortNamespace, value: t.Any, path: str) -> None:
            value = _namespace_value(value)
            if isinstance(port, PortNamespace):
                if port.nullable and value is None:
                    return
                if not isinstance(value, Mapping) or value.keys() - port.keys():
                    msg = f'task value `{path}` must match its declared output namespace.'
                    raise GraphValidationError(msg)
                for name, child in port.items():
                    if name not in value:
                        if child.required:
                            msg = f'task value is missing required output `{path}.{name}`.'
                            raise GraphValidationError(msg)
                        continue
                    collect(child, value[name], f'{path}.{name}')
            elif value is not None:
                stored[path] = value if isinstance(value, Data) else to_aiida_type(value)
            else:
                kinds = port.valid_type or ()
                kinds = (kinds,) if isinstance(kinds, type) else kinds
                if type(None) not in kinds:
                    msg = f'task value `{path}` does not allow None.'
                    raise GraphValidationError(msg)

        collect(port, value, 'value')
        super()._out_result(stored)

    @override
    def _out_result(self, result: t.Any) -> None:
        outputs = self.spec().outputs
        names = list(outputs)
        values = result if isinstance(result, tuple) else (result,)
        if not names and result is None:
            return
        if len(values) != len(names):
            msg = f'task declares {len(names)} outputs {names} but returned {len(values)} values.'
            raise GraphValidationError(msg)
        stored = {
            name: value if isinstance(value, Data) else to_aiida_type(value)
            for name, value in zip(names, values, strict=True)
        }
        super()._out_result(stored)


class TaskHandle(t.Generic[P, R]):
    """A callable process function carrying its graph declaration."""

    is_process_function = True

    def __init__(self, function: ProcessFunctionType[P, R, CalcFunctionNode]) -> None:
        self._function = function
        self.task_spec = TaskSpec.from_process(function)
        # Copy name and docstring, not the function state: the handle owns its task spec.
        functools.update_wrapper(self, function, updated=())

    @t.overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...

    @t.overload
    def __call__(self: TaskHandle[P, R], *args: t.Any, after: t.Any = ..., **kwargs: t.Any) -> R: ...

    def __call__(self, *args: t.Any, after: t.Any = None, **kwargs: t.Any) -> t.Any:
        if after is not None:
            msg = '`after` orders placements in a `@graph` body; it is not a task input.'
            raise GraphError(msg)
        return self._function(*args, **kwargs)

    @property
    def process_class(self) -> type[TaskProcess]:
        """Return the task process class behind the decorated function."""
        # `build_task` always decorates with `TaskProcess` as the base class.
        return t.cast(type[TaskProcess], self._function.process_class)

    @property
    def node_class(self) -> type[CalcFunctionNode]:
        """Return the provenance node class of task runs."""
        return CalcFunctionNode

    def get_launch_inputs(self, **inputs: t.Any) -> dict[str, t.Any]:
        """Validate the task inputs through the decorated function."""
        return self._function.get_launch_inputs(**inputs)

    def spec(self) -> ProcessSpec:
        """Return the process spec of the task process class."""
        return self.process_class.spec()

    def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run the task function locally and return its outputs."""
        return self._function.run(*args, **kwargs)

    def run_get_node(self, *args: P.args, **kwargs: P.kwargs) -> tuple[dict[str, t.Any] | None, CalcFunctionNode]:
        """Run the task function locally and return its outputs with the node."""
        return self._function.run_get_node(*args, **kwargs)

    def run_get_pk(self, *args: P.args, **kwargs: P.kwargs) -> tuple[dict[str, t.Any] | None, int]:
        """Run the task function locally and return its outputs with the node pk."""
        return self._function.run_get_pk(*args, **kwargs)

    def recreate_from(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        """Recreate the task node from cached inputs through the decorated function."""
        return self._function.recreate_from(*args, **kwargs)


def _check_task_contract(function: Callable[..., t.Any], outputs: Sequence[str] | None) -> Sequence[str] | None:
    """Reject task declarations the graph runtime cannot represent."""
    signature = inspect.signature(function)
    if 'after' in signature.parameters:
        msg = '`after` is reserved for source graph ordering dependencies.'
        raise GraphError(msg)
    hints = t.get_type_hints(function, include_extras=True)
    returned = hints.get('return', inspect.Signature.empty)
    annotation, _ = result_contract(returned)
    model = nullable_model(annotation) or annotation
    if nullable_model(annotation) is not None and t.get_origin(returned) is Result:
        msg = 'a Result model success type must be a PortModel; its value arm is already nullable.'
        raise GraphTypeCheckError(msg)
    if is_structured(model) and t.get_origin(returned) is Result and outputs is not None:
        msg = 'Result model outputs are named by their fields, not explicit output names.'
        raise GraphTypeCheckError(msg)
    if is_structured(model) and t.get_origin(returned) is not Result:
        msg = 'task output namespaces are not supported; declare leaf outputs instead.'
        raise GraphTypeCheckError(msg)
    if outputs is None:
        if annotation is inspect.Signature.empty:
            msg = 'tasks require a return annotation or explicit static output names.'
            raise GraphTypeCheckError(msg)
        if annotation is type(None):
            outputs = ()
    if outputs is not None:
        if (
            isinstance(outputs, str)
            or any(not name.isidentifier() for name in outputs)
            or len(set(outputs)) != len(outputs)
        ):
            msg = 'task outputs must be unique identifier names.'
            raise GraphTypeCheckError(msg)
    return outputs


def build_task(function: Callable[P, R], *, outputs: Sequence[str] | None = None) -> TaskHandle[P, R]:
    """Construct a task process with static leaf outputs and Python-valued inputs."""
    outputs = _check_task_contract(function, outputs)
    decorated = process_function(node_class=CalcFunctionNode, base_class=TaskProcess, outputs=outputs)(function)
    handle = TaskHandle(t.cast(ProcessFunctionType[P, R, CalcFunctionNode], decorated))
    spec = handle.process_class.spec()
    fixed_inputs(spec.inputs)
    is_result = t.get_origin(t.get_type_hints(function, include_extras=True).get('return')) is Result
    if spec.outputs.dynamic or (
        not is_result and any(isinstance(port, PortNamespace) for port in spec.outputs.values())
    ):
        msg = 'tasks require static leaf outputs.'
        raise GraphTypeCheckError(msg)
    return handle
