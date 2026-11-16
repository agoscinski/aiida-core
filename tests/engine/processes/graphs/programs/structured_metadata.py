###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Structured `task_metadata` programs: distinct aliases disambiguate shared callees.

Each `SubgraphTask` descends the metadata mapping by its placement name
(`task.name`). Loop, branch, and map control frames are transparent: they
consume no key level and iterations reuse their placement's config.
Repeated calls to the same metadata-bearing alias in one authored scope are
rejected at build time; bind the declaration to distinct Python aliases and
call each alias once instead.
"""

from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.engine.processes.graphs.lowering import graph, task
from aiida.orm import Code

pw_calc = task(ArithmeticAddCalculation)

calc_first = pw_calc
calc_second = pw_calc

#: An authored alias may itself contain `__`; configuration addresses it as one
#: placement name and never parses it as a provenance hierarchy.
my__calc = pw_calc


@graph
def pw_base(x: int | float, y: int | float, code: Code) -> int | float:
    """Run one `CalcJob` placement shared by both scopes."""
    total = pw_calc(x=x, y=y, code=code)
    return total.sum


@graph
def pw_mid(x: int | float, y: int | float, code: Code) -> int | float:
    """Wrap `pw_base` so a three-level path addresses the leaf `CalcJob`."""
    total = pw_base(x=x, y=y, code=code)
    return total


scf = pw_base
nscf = pw_base

scf_deep = pw_mid
nscf_deep = pw_mid


@graph
def direct_two(x: int | float, y: int | float, code: Code) -> int | float:
    """Two direct `CalcJob` placements under distinct aliases."""
    first = calc_first(x=x, y=y, code=code)
    second = calc_second(x=first.sum, y=y, code=code)
    return second.sum


@graph
def nested_two(x: int | float, y: int | float, code: Code) -> int | float:
    """The same nested graph through two aliases; each descends by its alias."""
    first = scf(x=x, y=y, code=code)
    second = nscf(x=first, y=y, code=code)
    return second


@graph
def deep_two(x: int | float, y: int | float, code: Code) -> int | float:
    """Two three-level paths (`scf_deep.pw_base.pw_calc`) with distinct leaves."""
    first = scf_deep(x=x, y=y, code=code)
    second = nscf_deep(x=first, y=y, code=code)
    return second


@graph
def dunder_single(x: int | float, y: int | float, code: Code) -> int | float:
    """One `CalcJob` placement whose authored alias contains `__`."""
    total = my__calc(x=x, y=y, code=code)
    return total.sum


@graph
def loop_reuse(x: int | float, y: int | float, code: Code, limit: int = 1, attempt: int = 0) -> int | float:
    """One `CalcJob` alias outside and inside the loop; iterations reuse its config."""
    total = pw_calc(x=x, y=y, code=code)
    more = attempt < limit
    while more:
        attempt = attempt + 1
        total = pw_calc(x=total.sum, y=y, code=code)
        more = attempt < limit
    return total.sum


@graph
def loop_nested(x: int | float, y: int | float, code: Code, limit: int = 1, attempt: int = 0) -> int | float:
    """One nested-graph alias outside and inside the loop; iterations reuse its config."""
    total = scf(x=x, y=y, code=code)
    more = attempt < limit
    while more:
        attempt = attempt + 1
        total = scf(x=total, y=y, code=code)
        more = attempt < limit
    return total


@graph
def branch_route(x: int | float, y: int | float, code: Code, flag: bool) -> int | float:
    """Each arm calls a distinct alias; only the taken arm dispatches."""
    if flag:
        result = scf(x=x, y=y, code=code)
    else:
        result = nscf(x=x, y=y, code=code)
    return result


@graph
def branch_shared(x: int | float, y: int | float, code: Code, flag: bool) -> int | float:
    """The same alias in both arms shares one configuration; control is transparent."""
    if flag:
        result = pw_calc(x=x, y=y, code=code)
    else:
        result = pw_calc(x=x, y=y, code=code)
    return result.sum
