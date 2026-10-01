###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Minimal, single-output graph syntax compiled without executing graph bodies.

Run ``uv run python graph_syntax_examples.py parse|show|run`` to print the
GraphSpec, show its task wiring, or execute it using an AiiDA profile.
``show`` is the default. From Python, call ``parse_graph(single_step)`` and pass
the result to ``show_graph(spec)`` or ``run_graph(spec, x=2, y=3)``. Execution
returns AiiDA data nodes; mapped outputs are dictionaries of nodes.
"""

from __future__ import annotations

import argparse
import typing as t
from collections.abc import Callable

from aiida.engine import GraphProcess, GraphSpec, parse_graph, run_get_node
from aiida.engine.processes.graphs.display import format_graph
from aiida.engine.processes.graphs.source import graph, task
from aiida.manage import load_profile


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


def show_graph(declaration: GraphSpec) -> None:
    """Print a readable view of a parsed graph without executing it."""
    print(format_graph(declaration))


def run_graph(declaration: GraphSpec, **inputs: t.Any) -> dict[str, t.Any]:
    """Execute a parsed graph with the given inputs using the default AiiDA profile."""
    load_profile()
    results, node = run_get_node(GraphProcess, **GraphProcess.launch_inputs(declaration, inputs))
    if not node.is_finished_ok:
        msg = f'Graph {declaration.identifier} failed: {node.exit_message}'
        raise RuntimeError(msg)
    return dict(results)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', nargs='?', choices=('parse', 'show', 'run'), default='show')
    args = parser.parse_args()

    examples: tuple[tuple[Callable[..., t.Any], dict[str, t.Any]], ...] = (
        (single_step, {'x': 2, 'y': 3}),
        (two_steps, {'x': 2, 'y': 3}),
        (diamond, {'x': 2}),
        (nested, {'x': 2, 'y': 3}),
        (choose, {'x': 2, 'flag': True}),
        (countdown, {'x': 3, 'keep_going': True}),
        (add_to_each, {'values': [1, 2, 3], 'offset': 10}),
    )
    for example, inputs in examples:
        declaration = parse_graph(example)
        if args.mode == 'parse':
            print(example.__name__, declaration)
        elif args.mode == 'show':
            show_graph(declaration)
        else:
            print(example.__name__, run_graph(declaration, **inputs))
    # GraphSpec stores task references and port wiring, not Python annotations.
