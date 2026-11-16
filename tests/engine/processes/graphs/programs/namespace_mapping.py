###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Lightweight dynamic-namespace processes for container-forwarding tests.

The fixtures mirror calculation specs with open ports (such as ``pseudos`` mappings)
without depending on plugin packages: `ItemCalcJob` declares a flat dynamic namespace,
`NestedGather` exposes one under ``sub`` like a nested ``pw.pseudos`` mapping, and
`ItemGather` is the runnable `WorkChain` behind both shapes. Expectations live in
``tests/engine/processes/graphs/test_namespace_mapping.py``.
"""

from aiida.engine import WorkChain, calcfunction, task
from aiida.engine.processes.calcjobs import CalcJob
from aiida.engine.processes.graphs.lowering import graph
from aiida.orm import DataDict, Int


@calcfunction
def _scaled_sum(subtotal: Int, factor: Int, shift: Int) -> Int:
    """Scale a gathered subtotal; creating nodes belongs in a `calcfunction`, not the outline."""
    return Int(subtotal.value * factor.value + shift.value)


class ItemGather(WorkChain):
    """Sum the members of dynamic namespaces, scaled and shifted by static ports."""

    @classmethod
    def define(cls, spec) -> None:  # type: ignore[override]
        super().define(spec)
        spec.input_namespace('members', valid_type=Int, dynamic=True, required=True)
        spec.input_namespace('extras', valid_type=Int, dynamic=True, required=False)
        spec.input('factor', valid_type=Int, required=True)
        spec.input('offset', valid_type=Int, required=False, default=lambda: Int(0))
        spec.output('total', valid_type=Int)
        spec.outline(cls.gather)

    def gather(self):
        """Store the scaled sum of every dynamic member plus the static shift."""
        subtotal = sum(member.value for member in self.inputs.members.values())
        extras = self.inputs.extras if 'extras' in self.inputs else {}
        subtotal += sum(member.value for member in extras.values())
        self.out('total', _scaled_sum(Int(subtotal), self.inputs.factor, self.inputs.offset))


class NestedGather(WorkChain):
    """Expose one gather under ``sub``; mirrors a nested dynamic namespace."""

    @classmethod
    def define(cls, spec) -> None:  # type: ignore[override]
        super().define(spec)
        spec.expose_inputs(ItemGather, namespace='sub', include=('members', 'factor', 'offset'))
        spec.output('total', valid_type=Int)
        spec.outline(cls.gather)

    def gather(self):
        """Store the scaled sum of the exposed members plus the static shift."""
        sub = self.inputs.sub
        subtotal = sum(member.value for member in sub.members.values())
        self.out('total', _scaled_sum(Int(subtotal), sub.factor, sub.offset))


class ItemCalcJob(CalcJob):
    """Declare a flat dynamic namespace without running; mirrors open calculation ports."""

    @classmethod
    def define(cls, spec) -> None:  # type: ignore[override]
        super().define(spec)
        spec.input_namespace('members', valid_type=Int, dynamic=True, required=True)
        spec.input('factor', valid_type=Int, required=True)
        spec.output('total', valid_type=Int)


flat_calc = task(ItemCalcJob, inputs={'members': DataDict})
flat_gather = task(ItemGather, inputs={'members': DataDict})
optional_gather = task(ItemGather, inputs={'members': DataDict, 'extras': DataDict})
nested_gather = task(NestedGather, inputs={'sub': {'members': DataDict}})
static_members = task(ItemGather, inputs={'members': {'a': Int}})
static_nested_gather = task(NestedGather, inputs={'sub': {'members': {'a': Int}}})


@task
def make_bundle(count: int) -> DataDict:
    """Build a `DataDict` of ``count`` members for forwarding tests."""
    return DataDict({f'item_{index}': Int(index) for index in range(count)})


@graph
def forward_flat(items: DataDict, factor: int) -> int:
    """Forward one `DataDict` wire into a dynamic namespace."""
    total = flat_gather(members=items, factor=factor)
    return total.total


@graph
def forward_optional(items: DataDict, extras: DataDict, factor: int) -> int:
    """Forward two `DataDict` wires, one into an optional namespace."""
    total = optional_gather(members=items, extras=extras, factor=factor)
    return total.total


@graph
def forward_nested(items: DataDict, factor: int) -> int:
    """Forward one `DataDict` wire into a nested dynamic namespace."""
    total = nested_gather(sub={'members': items, 'factor': factor})
    return total.total


@graph
def forward_chained(count: int, factor: int) -> int:
    """Forward a task-produced `DataDict` into a dynamic namespace."""
    bundle = make_bundle(count=count)
    total = flat_gather(members=bundle, factor=factor)
    return total.total


@graph
def forward_calc(items: DataDict, factor: int) -> int:
    """Forward one `DataDict` wire into a calculation with open ports (build only)."""
    total = flat_calc(members=items, factor=factor)
    return total.total


@graph
def static_nested(a: int, factor: int) -> int:
    """Wire static names into a nested dynamic namespace without forwarding."""
    total = static_nested_gather(sub={'members': {'a': a}, 'factor': factor})
    return total.total
