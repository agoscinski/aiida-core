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

from aiida.engine import GraphSpec, parse_graph, register_graph, register_task


@register_task
def add(x: int, y: int) -> int:
    return x + y


@register_task
def multiply(x: int, y: int) -> int:
    return x * y


@register_graph
def single_step(x: int, y: int) -> int:
    return add(x=x, y=y)


@register_graph
def two_steps(x: int, y: int) -> int:
    first = add(x=x, y=y)
    return multiply(x=first, y=y)


@register_graph
def diamond(x: int) -> int:
    left = add(x=x, y=1)
    right = multiply(x=x, y=2)
    return add(x=left, y=right)


@register_graph
def nested(x: int, y: int) -> int:
    first = two_steps(x=x, y=y)
    return add(x=first, y=y)


if __name__ == '__main__':
    for example in (single_step, two_steps, diamond, nested):
        declaration: GraphSpec = parse_graph(example)
        print(example.__name__, declaration.to_dict())
    # GraphSpec stores task references and port wiring, not Python annotations.
