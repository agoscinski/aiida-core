###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for the restricted AST-to-GraphSpec compiler."""

import pytest

from aiida.engine import (
    GraphSpec,
    ProcessTask,
    SubgraphTask,
    UnsupportedSyntax,
    parse_graph,
    register_graph,
    register_task,
)


@register_task
def sum_two(x: int, y: int) -> int:
    return x + y


@register_graph
def chain(x: int, y: int) -> int:
    first = sum_two(x=x, y=y)
    return sum_two(x=first, y=y)


@register_graph
def outer(x: int, y: int) -> int:
    inner = chain(x=x, y=y)
    return sum_two(x=inner, y=x)


@register_graph
def recursive(x: int) -> int:
    return recursive(x=x)


def not_registered(x: int) -> int:
    return x


missing = 1  # A Python global is deliberately not a graph input.


@register_graph
def unknown(x: int) -> int:
    return not_registered(x=x)


@register_graph
def arbitrary_code(x: int) -> int:
    print('this body must not run')
    return sum_two(x=x, y=1)


@register_graph
def unbound(x: int) -> int:
    return sum_two(x=x, y=missing)


@register_graph
def bad_assignment(x: int) -> int:
    a = 0
    return sum_two(x=x, y=a)


def test_wires_tasks_without_running_graph_body():
    spec = parse_graph(chain)
    assert isinstance(spec, GraphSpec)
    assert [item.name for item in spec.tasks] == ['sum_two', 'sum_two_2']
    assert all(isinstance(item, ProcessTask) for item in spec.tasks)
    assert [(edge.source, edge.source_port, edge.target, edge.target_port) for edge in spec.dependencies] == [
        ('sum_two', 'result', 'sum_two_2', 'x')
    ]
    assert spec.inputs == {'x': (('sum_two', 'x'),), 'y': (('sum_two', 'y'), ('sum_two_2', 'y'))}
    assert spec.outputs['result'].task == 'sum_two_2'
    assert GraphSpec.from_dict(spec.to_dict()).to_dict() == spec.to_dict()


def test_nested_graph():
    spec = parse_graph(outer)
    assert isinstance(spec.tasks[0], SubgraphTask)
    assert spec.tasks[0].body.identifier == 'chain'
    assert spec.dependencies[0].source == 'chain'


@pytest.mark.parametrize(
    ('function', 'message'),
    [
        (recursive, 'Recursive graph call'),
        (unknown, 'unregistered call'),
        (arbitrary_code, 'only single-name assignments'),
        (unbound, 'unbound name'),
        (bad_assignment, 'graph assignments must call'),
    ],
)
def test_rejects_unsupported_graphs(function, message):
    with pytest.raises(UnsupportedSyntax, match=message):
        parse_graph(function)


def test_error_points_to_original_source():
    with pytest.raises(UnsupportedSyntax) as error:
        parse_graph(bad_assignment)
    assert f'{__file__}:' in str(error.value)
    assert '        a = 0\n            ^' in str(error.value)
