###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Examples of source graph syntax that the restricted parser rejects.

Run ``uv run python graph_syntax_bad_examples.py`` to see each error, including
its source location and a caret. The graph function bodies are never executed.
"""

from __future__ import annotations

from collections.abc import Callable

from aiida.engine import UnsupportedSyntax, parse_graph
from aiida.engine.processes.graphs.source import graph, taskh


@taskh
def add(x: int, y: int) -> int:
    return x + y


def not_registered(x: int) -> int:
    return x


outside_value = 10  # Globals are not graph inputs, even if Python can see them.


@graph
def constant_assignment(x: int) -> int:
    value = 0
    return add(x=x, y=value)


@graph
def call_unregistered(x: int) -> int:
    return not_registered(x=x)


@graph
def call_positional(x: int) -> int:
    return add(x, 1)


@graph
def wrong_input(x: int) -> int:
    return add(z=x, y=1)


@graph
def read_global(x: int) -> int:
    return add(x=x, y=outside_value)


@graph
def computed_argument(x: int) -> int:
    return add(x=x + 1, y=2)


@graph
def conditional(x: int) -> int:
    if x:
        return add(x=x, y=1)
    return add(x=x, y=2)


@graph
def if_without_else(x: int, flag: bool) -> int:
    if flag:
        selected = add(x=x, y=1)
    return selected


@graph
def while_without_condition_update(x: int, keep_going: bool) -> int:
    while keep_going:
        x = add(x=x, y=1)
    return x


@graph
def for_with_multiple_assignments(values: list[int]) -> int:
    for value in values:
        first = add(x=value, y=1)
        result = add(x=first, y=1)
    return result


@graph
def recursive(x: int) -> int:
    return recursive(x=x)


@graph
def nested_invalid(x: int) -> int:
    return wrong_input(x=x)


EXAMPLES: tuple[Callable[..., int], ...] = (
    constant_assignment,
    call_unregistered,
    call_positional,
    wrong_input,
    read_global,
    computed_argument,
    conditional,
    if_without_else,
    while_without_condition_update,
    for_with_multiple_assignments,
    recursive,
    nested_invalid,
)


if __name__ == '__main__':
    for example in EXAMPLES:
        try:
            parse_graph(example)
        except UnsupportedSyntax as error:
            print(f'\n{example.__name__}:\n{error}')
