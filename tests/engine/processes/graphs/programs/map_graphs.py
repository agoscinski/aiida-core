###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tasks and graphs for `par_map` build rejections over `@graph` callables.

`par_map` maps unary tasks only: every graph below maps a `@graph` (or a
graph that itself maps) and fails the build naming the mapped callable.
Task-only maps live in `par_maps`.
"""

from aiida.engine import graph, par_map, task
from aiida.orm import DataList


@task
def increment(x: int) -> int:
    """Add one to the input."""
    return x + 1


@task
def double(x: int) -> int:
    """Double the input."""
    return 2 * x


@graph
def two_step(x: int) -> int:
    """Add one and then double, so mapping exercises a multi-step graph."""
    incremented = increment(x=x)
    return double(x=incremented)


@graph
def map_two_step(values: DataList) -> DataList:
    """Map a two-step graph over an owned list; rejected as not a task."""
    return par_map(two_step, values)


@graph
def add_one(x: int) -> int:
    """Add one inside a nested graph; mapping it is rejected."""
    return increment(x=x)


@graph
def map_add_one(values: DataList) -> DataList:
    """Map a single-task graph over an owned list; rejected as not a task."""
    return par_map(add_one, values)


@graph
def map_aliased(values: DataList) -> DataList:
    """Map an aliased graph; rejected naming the declaration."""
    return par_map(two_step.alias('two_stepped'), values)


@graph
def inner_map(values: DataList) -> DataList:
    """Map a task over an owned list; itself the callable of an outer map."""
    return par_map(increment, values)


@graph
def map_nested(nested: DataList) -> DataList:
    """Map a graph that itself maps, over a list of lists; rejected as not a task."""
    return par_map(inner_map, nested)


@graph
def passthrough_identity(x: int) -> int:
    """Pass one input through with no tasks."""
    return x


@graph
def map_passthrough(values: DataList) -> DataList:
    """Map a task-free graph over a list; rejected as not a task."""
    return par_map(passthrough_identity, values)
