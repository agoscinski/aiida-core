###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Importable task and graph declarations shared by the behavior suites."""

import typing as t

from aiida.engine import Err, ExitCode, ExitCodeEnum, Ok, Result
from aiida.engine.processes.graphs.build_source import graph, task
from aiida.engine.processes.port_model import PortModel
from aiida.orm.nodes.data.int import Int

# Input models.


class Settings(PortModel):
    """Explicit namespace with a required field, for missing-input tests."""

    count: int = 2
    nullable: int | None


class Configuration(PortModel):
    """Top-level namespace carrying settings and a label."""

    settings: Settings
    label: str = 'test'


class DefaultedSettings(PortModel):
    """A namespace usable without any inputs, for default coverage."""

    count: int = 2


# Task definitions.


class TaskErrors(ExitCodeEnum):
    """Declared failures for result-bearing tasks."""

    NEGATIVE = ExitCode(410, 'Negative value: {value}', invalidates_cache=True)
    ZERO = ExitCode(411, 'Zero value')


@task
def checked_value(value: int) -> Result[int, TaskErrors]:
    if value < 0:
        return Err(TaskErrors.NEGATIVE.value.format(value=str(value)))
    if value == 0:
        return Err(TaskErrors.ZERO)
    return Ok(value)


@task(outputs=('value', 'double'))
def checked_pair(value: int) -> Result[tuple[int, int], TaskErrors]:
    return Ok((value, value * 2))


@task
def checked_empty() -> Result[None, TaskErrors]:
    return Ok(None)


@task
def undeclared_error(value: int) -> Result[int, TaskErrors]:
    return Err(ExitCode(999, 'Not declared'))


@task
def missing_result_wrapper(value: int) -> Result[int, TaskErrors]:
    return value


@task
def checked_fallback(error: TaskErrors) -> int:
    return -error.value.status


@graph
def checked_pipeline(value: int) -> int:
    result, error = checked_value(value=value)
    if error is not None:
        doubled = checked_fallback(error=error)
    else:
        doubled = add(x=result, y=result)
    return doubled


class InputErrors(ExitCodeEnum):
    """An independent enum for union-error tests, with a shared member name."""

    ZERO = ExitCode(412, 'Empty input')


class ResultValue(PortModel):
    """A structured result with a nested namespace."""

    count: int
    settings: DefaultedSettings


@task
def checked_model(value: int) -> Result[ResultValue, TaskErrors | InputErrors]:
    if value < 0:
        return Err(TaskErrors.NEGATIVE)
    if value == 0:
        return Err(InputErrors.ZERO)
    return Ok(ResultValue(count=value, settings=DefaultedSettings(count=value * 2)))


@task
def model_total(value: ResultValue) -> int:
    return value.count + value.settings.count


@task
def union_fallback(error: TaskErrors | InputErrors) -> int:
    return -error.value.status


@task
def inspect_optional(value: int | None, error: TaskErrors | None) -> int:
    if error is not None:
        assert value is None
        return error.value.status
    assert value is not None
    return value


@task
def inspect_optional_model(value: ResultValue | None, error: TaskErrors | InputErrors | None) -> int:
    if error is not None:
        assert value is None
        return error.value.status
    assert value is not None
    return value.count + value.settings.count


@graph
def optional_model_result(value: int) -> int:
    result, error = checked_model(value=value)
    selected = inspect_optional_model(value=result, error=error)
    return selected


class NullableValue(PortModel):
    """A model whose successful value needs no data nodes."""

    count: int | None
    settings: DefaultedSettings | None


@task
def nullable_fields(fail: bool) -> Result[NullableValue, TaskErrors]:
    if fail:
        return Err(TaskErrors.ZERO)
    return Ok(NullableValue(count=None, settings=None))


@task
def inspect_nullable_fields(value: NullableValue | None, error: TaskErrors | None) -> bool:
    if error is not None:
        return value is None
    return value is not None and value.count is None and value.settings is None


@graph
def nullable_fields_result(fail: bool) -> bool:
    value, error = nullable_fields(fail=fail)
    selected = inspect_nullable_fields(value=value, error=error)
    return selected


@graph
def optional_result(value: int) -> int:
    result, error = checked_value(value=value)
    selected = inspect_optional(value=result, error=error)
    return selected


@graph
def model_result(value: int) -> int:
    result, error = checked_model(value=value)
    if error is not None:
        selected = union_fallback(error=error)
    else:
        selected = model_total(value=result)
    return selected


@graph
def bound_model_result(value: int) -> int:
    result = checked_model(value=value)
    if result.error is None:
        selected = model_total(value=result.value)
    else:
        selected = union_fallback(error=result.error)
    return selected


@graph
def model_result_outputs(value: int):
    result, error = checked_model(value=value)
    return {'count': result.count, 'error': error}


@graph
def model_leaf_result(value: int) -> int:
    result, error = checked_model(value=value)
    if error:
        selected = union_fallback(error=error)
    else:
        selected = pass_int(value=result.settings.count)
    return selected


@graph
def value_none_branch(value: int) -> int:
    result, error = checked_value(value=value)
    if result is None:
        selected = checked_fallback(error=error)
    else:
        selected = pass_int(value=result)
    return selected


@task
def add(x: int, y: int) -> int:
    """Add two integers."""
    return x + y


@task(outputs=('sum',))
def add_named(x: int, y: int) -> int:
    """Return the integer sum under a named output, for selection wiring."""
    return x + y


@task
def multiply(x: int, y: int) -> int:
    """Multiply two integers."""
    return x * y


@task(outputs=('sum', 'product'))
def arithmetic_pair(x: int, y: int) -> tuple[int, int]:
    """Return sum and product as named outputs."""
    return x + y, x * y


@task
def read_nested_count(config: Configuration) -> int:
    """Read the nested count from a model."""
    return config.settings.count


@task
def read_answer(values: dict[str, int]) -> int:
    """Read one answer from a mapping."""
    return values['answer']


@task
def pass_int(value: int) -> int:
    """Pass an integer through unchanged."""
    return value


@task
def read_orm_node(value: Int) -> int:
    """Unwrap an `Int` node to its value."""
    assert isinstance(value, Int)
    return value.value


@task
def is_positive(value: int) -> bool:
    """Decide whether a value is positive."""
    return value > 0


@task
def decrement(value: int) -> int:
    """Subtract one."""
    return value - 1


@task(outputs=('mean', 'stdev'))
def statistics(values: list[float]) -> tuple[float, float]:
    """Summarize samples; bodies run as plain Python and are never lowered."""
    import math

    total = 0.0
    for sample in values:
        total += sample
    mean = total / len(values)
    variance = sum((sample - mean) ** 2 for sample in values) / len(values)
    return mean, math.sqrt(variance)


@task(outputs=('alpha',))
def mismatched_alpha(value: int) -> int:
    """Return its input under the `alpha` output."""
    return value


@task(outputs=('beta',))
def mismatched_beta(value: int) -> int:
    """Return its input under the `beta` output."""
    return value


@task
def prepare(value: int) -> None:
    """Order downstream placements after this one."""
    return None


@task
def fail_always(x: int) -> int:
    """Raise unconditionally, for failure propagation."""
    raise ValueError('deliberately failed')


@task
def mistyped_output(x: int) -> int:
    """Return a mistyped value, rejected at runtime."""
    return 'wrong'  # rejected at runtime


@task
def mistyped_list(x: int) -> list[int]:
    """Return a mistyped collection, rejected at runtime."""
    return {'value': x}  # rejected at runtime


@task
def mistyped_dict(x: int) -> dict[str, int]:
    """Return a mistyped mapping, rejected at runtime."""
    return [x]  # rejected at runtime


@task
def collision_add(x: int, y: int) -> int:
    """Provide a second addition task for collision coverage."""
    return x + y


# Invalid task definitions.


def unannotated_function(x: int):
    """Reject undecorated functions without annotations at decoration."""
    return x


def kwargs_function(**kwargs: t.Any) -> int:
    """Reject undecorated functions with kwargs at decoration."""
    return 1


# Supported straight-line graphs.


@graph
def chain(x: int, y: int) -> int:
    """Two whole-result placements."""
    first = add(x=x, y=y)
    return add(x=first, y=y)


@graph
def multiply_add(x: int, y: int, z: int) -> int:
    """Return the arithmetic result for the local multiply-add workflow example."""
    product = multiply(x=x, y=y)
    return add(x=product, y=z)


@graph
def diamond(x: int) -> int:
    """Fan two additions into one."""
    left = add(x=x, y=1)
    right = add(x=x, y=2)
    return add(x=left, y=right)


@graph
def ordered(x: int) -> int:
    """Order a placement with `after`."""
    first = prepare(value=x)
    return add(x=x, y=1, after=first)


@graph
def ordered_named(value: int) -> int:
    """Order on a named-output task; the bare return takes the `sum` port."""
    prepared = prepare(value=value)
    return add_named(x=value, y=1, after=prepared)


@graph
def multiple_ordered(x: int) -> int:
    """Order on two placements at once."""
    first = prepare(value=x)
    second = prepare(value=x)
    return add(x=x, y=1, after=(first, second))


@graph
def named(x: int, y: int) -> dict[str, int]:
    """Return multiple named outputs from one task."""
    pair = arithmetic_pair(x=x, y=y)
    return {'total': pair.sum, 'product': pair.product}


@graph
def whole(config: Configuration) -> int:
    """Pass a whole namespace through."""
    return read_nested_count(config=config)


@graph
def selected(config: Configuration) -> int:
    """Select one namespace field."""
    return add(x=config.settings.count, y=1)


@graph
def selected_field(settings: DefaultedSettings) -> int:
    """Select a defaulted namespace field, without passing any inputs."""
    return add_named(x=settings.count, y=1)


@graph
def wired(config: Configuration) -> int:
    """Wire constructed namespaces between tasks."""
    chosen = add(x=config.settings.count, y=1)
    return read_nested_count(config={'settings': {'count': chosen, 'nullable': config.settings.nullable}})


@graph
def dictionary_input(values: dict[str, int]) -> int:
    """Read a passed mapping."""
    return read_answer(values=values)


@graph
def literal_dictionary() -> int:
    """Read a dictionary literal, without passing any inputs."""
    return read_answer(values={'answer': 42})


@graph
def orm_passthrough(value: Int) -> int:
    """Pass an ORM node through a task."""
    return read_orm_node(value=value)


@graph
def input_passthrough(value: int) -> int:
    """Pass a graph input straight to outputs."""
    return value


@graph(identifier='custom-label')
def labelled(x: int) -> int:
    """Carry a custom identifier."""
    return add(x=x, y=1)


@graph
def failed(x: int) -> int:
    """Fail through a broken placement."""
    first = fail_always(x=x)
    return add(x=first, y=1)


@graph
def collision(x: int) -> int:
    """Place three tasks across two callees."""
    first = add(x=x, y=1)
    second = add(x=first, y=1)
    return collision_add(x=second, y=1)


@graph
def missing_task_input(x: int) -> int:
    """Omit a required task input."""
    return add(x=x)


# Composition: nesting, branching, and looping.


@graph
def nested(x: int, y: int) -> int:
    """Nest one graph in another."""
    inner = chain(x=x, y=y)
    return add(x=inner, y=x)


@graph
def nested_multiply_add(x: int, y: int, z: int) -> int:
    """Nest the multiply-add graph once."""
    intermediate = multiply_add(x=x, y=y, z=z)
    return add(x=intermediate, y=1)


@graph
def external_choice(value: int, condition: bool) -> int:
    """Branch on an input flag."""
    if condition:
        selected_branch = add(x=value, y=1)
    else:
        selected_branch = add(x=value, y=2)
    return selected_branch


@graph
def computed_choice(value: int) -> int:
    """Branch on a task-computed condition."""
    condition = is_positive(value=value)
    if condition:
        selected_branch = add(x=value, y=1)
    else:
        selected_branch = add(x=value, y=2)
    return selected_branch


@graph
def inline_condition(value: int) -> int:
    """Branch on an inline predicate call, bound like the explicit form."""
    if is_positive(value=value):
        selected_branch = add(x=value, y=1)
    else:
        selected_branch = add(x=value, y=2)
    return selected_branch


@graph
def countdown(value: int, condition: bool) -> int:
    """Loop until a task-computed condition clears."""
    while condition:
        value = decrement(value=value)
        condition = is_positive(value=value)
    return value


@graph
def countdown_call(value: int) -> int:
    """Loop on an inline predicate call, re-evaluated every iteration."""
    while is_positive(value=value):
        value = decrement(value=value)
    return value


@graph
def summarize(values: list[float]):
    """Expose summary statistics computed by plain Python."""
    stats = statistics(values=values)
    return {'mean': stats.mean, 'stdev': stats.stdev}


@graph
def mismatched_branches(value: int, condition: bool) -> int:
    """Return different outputs per side, rejected."""
    if condition:
        selected_branch = mismatched_alpha(value=value)
    else:
        selected_branch = mismatched_beta(value=value)
    return selected_branch


# Flexible spellings, desugared to the explicit forms.


@graph
def positional_task_arguments(value: int) -> int:
    """Call with positional arguments, bound through signature order."""
    return add(value, 1)


@graph
def tuple_assignment(x: int, y: int) -> int:
    """Bind two names at once from multiple outputs."""
    total, product = arithmetic_pair(x=x, y=y)
    return add(x=total, y=product)


@graph
def chained_assignment(value: int) -> int:
    """Assign two names in one statement to one placement."""
    first = second = add(x=value, y=1)
    return add(x=first, y=second)


@graph
def default_parameter(value: int = 2) -> int:
    """Declare a parameter default, filled when absent."""
    return add(x=value, y=1)


@graph
def keyword_only_parameter(*, value: int) -> int:
    """Declare a keyword-only parameter, treated as ordinary."""
    return add(x=value, y=1)


@graph
def positional_only_parameter(value: int, /) -> int:
    """Declare a positional-only parameter, treated as ordinary."""
    return add(x=value, y=1)


# Rejected declarations.
# TODO: Good error messages (verify)
# -> yes, we will still do


@graph
def expanded_task_arguments(values: dict[str, int]) -> int:  # skip on PR
    """Call with unpacked arguments; mappings cannot expand statically.

    Declare the inputs explicitly instead:

    >>> @graph
    >>> def explicit_arguments(x: int, y: int) -> int:
    >>>     return add(x=x, y=y)
    """
    return add(**values)


@graph
def arithmetic_expression(value: int) -> int:
    """Compute an input inline; every value needs a producing task.

    Bind the intermediate explicitly:

    >>> @graph
    >>> def bound_expression(value: int) -> int:
    >>>     incremented = add(x=value, y=1)
    >>>     return add(x=incremented, y=2)
    """
    return add(x=value + 1, y=2)


@graph
def reassigned_name(value: int) -> int:
    """Rebind a name; single-assignment keeps the graph well-defined, not just readable.

    Use fresh names instead:

    >>> @graph
    >>> def assigned_twice(value: int) -> int:
    >>>     first = add(x=value, y=1)
    >>>     return add(x=first, y=2)
    """
    result = add(x=value, y=1)
    result = add(x=result, y=2)
    return result


# NOTE: we could internally represent `for`-loops as `while`
# -> defer for follow-up
# did XW have `for` construct?
# reassignment could just be copying the object? unrolled for-loop in db?
# if not in db, for loop in task
@graph
def fanout(values: list[int]) -> int:  # why does fail, is it not just the rebinding?
    """Loop over inputs, rebinding each round; fan-out over runtime collections is not supported.

    Unroll the placements instead:

    >>> @graph
    >>> def unrolled_fanout(value: int) -> int:
    >>>     first = add(x=value, y=1)
    >>>     second = add(x=first, y=1)
    >>>     return add(x=second, y=1)
    """
    for value in values:
        result = add(x=value, y=1)
    return result  # rejected at lowering


@graph
def variadic_parameters(*values: int) -> int:
    """Declare variadic parameters; dynamic arity is unrepresentable in fixed ports.

    Plain Python takes any count, `total(1, 2, 3)`. Declare fixed arity instead:

    >>> @graph
    >>> def fixed_parameters(x: int, y: int) -> int:
    >>>     return add(x=x, y=y)
    """
    return add(*values)


@graph
def literal_return() -> int:
    """Return a literal; workflows return existing nodes, they never create data.

    Pass the value in as an input instead:

    >>> @graph
    >>> def constant_return(value: int) -> int:
    >>>     return value
    """
    return 42


# TODO: PortModel as return type should def be supported
# -> yes, we will still do
@graph
def namespace_return(config: Configuration) -> Configuration:
    """Return a whole namespace; outputs are flat leaves.

    Select a leaf field instead:

    >>> @graph
    >>> def leaf_return(config: Configuration) -> int:
    >>>     return config.settings.count
    """
    return config


@graph
def nested_outputs(x: int) -> dict[str, dict[str, int]]:
    """Return nested outputs; outputs are flat leaves.

    Keep outputs flat instead:

    >>> @graph
    >>> def flat_outputs(x: int) -> int:
    >>>     value = add(x=x, y=1)
    >>>     return {'inner': value}
    """
    value = add(x=x, y=1)
    return {'outer': {'inner': value}}


@graph
def missing_final_return(value: int) -> int:  # rejected at lowering
    """End without a return; graphs declare their outputs explicitly.

    Declare the output instead:

    >>> @graph
    >>> def final_return(value: int) -> int:
    >>>     return add(x=value, y=1)
    """
    add(x=value, y=1)


@graph
def recursive_call(value: int) -> int:
    """Call itself; the spec would be infinite.

    Factor the step into a task instead:

    >>> @task
    >>> def single_step(value: int) -> int:
    >>>     return value - 1
    """
    return recursive_call(value=value)


@graph
def wrong_type(x: str) -> int:
    """Feed a mistyped value; contracts fail fast at validation.

    Annotate the declared type instead:

    >>> @graph
    >>> def right_type(x: int) -> int:
    >>>     return add(x=x, y=1)
    """
    return add(x=x, y=1)


@graph
def unbound(value: int) -> int:
    """Reference an unbound name; every name must bind first.

    Bind the name first instead:

    >>> @graph
    >>> def bound(x: int) -> int:
    >>>     total = add(x=x, y=1)
    >>>     return add(x=total, y=1)
    """
    return add(x=value, y=missing)  # noqa: F821 - unbound names must fail at lowering
