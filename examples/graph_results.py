###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Shareable example: `Result`-based task failures in source graphs.

A task declares `Result[SuccessType, ErrorEnum]` and returns `Ok(outputs)` or
`Err(error)`. Graph bodies unpack the nullable `(value, error)` pair and branch
on it, so declared failures can be recovered instead of failing the graph.

Import this module (do not run it directly: declarations must be importable)::

    uv run python -c "import sys; sys.path.insert(0, 'examples'); import graph_results; graph_results.main()"

Running the `main()` demo requires a configured AiiDA profile.
"""

from __future__ import annotations

import json

from aiida.engine import Err, ExitCode, ExitCodeEnum, Ok, PortModel, Result, graph, run_get_node, task


class DivisionErrors(ExitCodeEnum):
    """Failures raised while dividing."""

    ZERO_DENOMINATOR = ExitCode(410, 'The denominator is zero.', invalidates_cache=True)


class InputErrors(ExitCodeEnum):
    """Failures raised while validating the input."""

    NEGATIVE_NUMERATOR = ExitCode(411, 'Negative numerator: {value}')


class Quotient(PortModel):
    """The successful result of a division."""

    ratio: float


@task
def divide(numerator: float, denominator: float) -> Result[Quotient, DivisionErrors | InputErrors]:
    """Divide, returning a declared error instead of raising."""
    if denominator == 0:
        return Err(DivisionErrors.ZERO_DENOMINATOR)
    if numerator < 0:
        return Err(InputErrors.NEGATIVE_NUMERATOR.value.format(value=str(numerator)))
    return Ok(Quotient(ratio=numerator / denominator))


@task
def recover(error: DivisionErrors | InputErrors) -> float:
    """Recover from any declared division error."""
    print(f'recovering from {error!r}')
    return 0.0


@task
def read_ratio(value: Quotient) -> float:
    """Consume the successful arm of the result."""
    return value.ratio


@graph
def ratio(numerator: float, denominator: float) -> float:
    """Divide two numbers, recovering with zero on any declared failure."""
    value, error = divide(numerator=numerator, denominator=denominator)
    if error is not None:
        selected = recover(error=error)
    else:
        selected = read_ratio(value=value)
    return selected


def main() -> None:
    """Build the graph IR without running anything, then run three cases."""
    spec = ratio.build()
    print('tasks:', spec.task_names)
    print('exit codes:', sorted(spec.tasks[0].spec.exit_codes))
    # The task IR, including exit codes, survives a JSON round trip.
    assert spec.to_dict() == json.loads(json.dumps(spec.to_dict()))

    for numerator, denominator in [(6.0, 3.0), (1.0, 0.0), (-2.0, 4.0)]:
        outputs, node = run_get_node(ratio, numerator=numerator, denominator=denominator)
        child = node.called[0]
        result = outputs['result'].value
        print(
            f'{numerator} / {denominator} = {result} '
            f'(graph finished ok: {node.is_finished_ok}, '
            f'task exit: {child.exit_status}, {child.exit_message!r})'
        )


if __name__ == '__main__':
    main()
