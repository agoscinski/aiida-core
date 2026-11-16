###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`CalcJob` tasks shared by the graph behavior suites."""

from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.engine.processes.graphs.lowering import graph, task
from aiida.orm import Code

add_calc = task(ArithmeticAddCalculation)


@graph
def unused_step(x: int, y: int, code: Code, value: int) -> int:
    """Run one calculation without using its output, then step the counter."""
    total = add_calc(x=x, y=y, code=code)  # noqa: F841 - placement runs the science, result unused
    return value - 1


@graph
def serial_add_unused(x: int, y: int, code: Code, rounds: int) -> None:
    """Place one calculation per loop iteration with no data dependency on the counter."""
    count = rounds
    while count > 0:
        count = unused_step(x=x, y=y, code=code, value=count)
    return {}


@graph
def add_single(x: int, y: int, code: Code) -> int | float:
    """Add two numbers through a single `CalcJob` placement."""
    total = add_calc(x=x, y=y, code=code)
    return total.sum


@graph
def add_nested(x: int, y: int, code: Code) -> int | float:
    """Add two numbers through a nested graph holding the `CalcJob` placement."""
    return add_inner(x=x, y=y, code=code)


@graph
def add_inner(x: int, y: int, code: Code) -> int | float:
    """Add two numbers through a single `CalcJob` placement."""
    total = add_calc(x=x, y=y, code=code)
    return total.sum


@graph
def add_twice(x: int, y: int, code: Code) -> int | float:
    """Add two numbers through two chained `CalcJob` placements."""
    first = add_calc(x=x, y=y, code=code)
    second = add_calc(x=first.sum, y=y, code=code)
    return second.sum
