###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Fan-out tasks over owned containers as independent child processes.

A ``@graph`` body calls :func:`par_for` with a unary task and a
:class:`~aiida.orm.DataList` or :class:`~aiida.orm.DataDict` wire. The lowering
expands the call to a static map placement; the scheduler dispatches one child
process per element in parallel and then a collector calculation that
*creates* the result container. Graphs only ever return that existing
container, so they never create result containers themselves.

Unlike :func:`par_map`, ``par_for`` also accepts ``CalcJob`` tasks: the task
declares exactly one scientific input, which receives the element, while every
other input arrives through one shared ``runtime_config`` entry under the map
placement name. ``@graph`` callables are rejected at build time.
"""

from __future__ import annotations

import typing as t
from collections.abc import Callable

from aiida.common.exceptions import GraphLanguageError
from aiida.orm import DataDict, DataList

__all__ = ('par_for',)

T = t.TypeVar('T')
R = t.TypeVar('R')


@t.overload
def par_for(func: Callable[[T], R], values: DataList, *, after: t.Any = None) -> DataList: ...


@t.overload
def par_for(func: Callable[[T], R], values: DataDict, *, after: t.Any = None) -> DataDict: ...


def par_for(func: t.Any, values: t.Any, *, after: t.Any = None) -> t.Any:
    """Fan out a unary task over an owned container as independent child processes.

    This is an authoring operation, not a runtime function: call it inside a
    ``@graph`` body and the graph compiler expands it to a fan-out placement.
    Calling it anywhere else fails loudly.

    :param func: a unary ``@task`` (function, ``CalcJob``, or ``WorkChain``) with exactly one
        scientific input and one output; execution inputs arrive through one shared
        ``runtime_config`` entry under the map placement name.
    :param values: a :class:`~aiida.orm.DataList` or :class:`~aiida.orm.DataDict` wire;
        lists iterate elements in order, mappings iterate values keeping keys.
    :param after: ordering dependency on earlier placements, like task calls.
    :return: a container of the same kind holding one child output per element.
    """
    msg = (
        '`par_for` declares a fan-out inside a `@graph` body and never runs directly; '
        'call it with a unary `@task` and a `DataList`/`DataDict` wire on one line, '
        'for example `fanned = par_for(increment, values)`.'
    )
    raise GraphLanguageError(msg)
