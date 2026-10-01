###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Minimal, single-output graph syntax compiled without executing graph bodies.

Run ``uv run python graph_syntax_examples.py`` to print the declarations.
"""

from __future__ import annotations

from aiida.engine import GraphSpec, parse_graph
from aiida.engine.processes.graphs.source import graph, task


@task
def add(x: int, y: int) -> int:
    return x + y


@task
def multiply(x: int, y: int) -> int:
    return x * y


@task
def decrement(x: int) -> int:
    return x - 1


@task
def positive(x: int) -> bool:
    return x > 0


@graph
def single_step(x: int, y: int) -> int:
    return add(x=x, y=y)


@graph
def two_steps(x: int, y: int) -> int:
    first = add(x=x, y=y)
    return multiply(x=first, y=y)


@graph
def diamond(x: int) -> int:
    left = add(x=x, y=1)
    right = multiply(x=x, y=2)
    return add(x=left, y=right)


@graph
def nested(x: int, y: int) -> int:
    first = two_steps(x=x, y=y)
    return add(x=first, y=y)


@graph
def choose(x: int, flag: bool) -> int:
    if flag:
        selected = add(x=x, y=1)
    else:
        selected = multiply(x=x, y=2)
    return selected


@graph
def countdown(x: int, keep_going: bool) -> int:
    while keep_going:
        x = decrement(x=x)
        keep_going = positive(x=x)
    return x


@graph
def add_to_each(values: list[int], offset: int) -> list[int]:
    for value in values:
        result = add(x=value, y=offset)
    return result  # A collection of results, one per item in values.


if __name__ == '__main__':
    for example in (single_step, two_steps, diamond, nested, choose, countdown, add_to_each):
        declaration: GraphSpec = parse_graph(example)
        print(example.__name__, declaration.to_dict())
    # GraphSpec stores task references and port wiring, not Python annotations.
