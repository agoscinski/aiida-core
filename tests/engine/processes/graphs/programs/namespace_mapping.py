###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Processes with nested dynamic input namespaces for specialization tests."""

from aiida.engine import WorkChain, calcfunction, task
from aiida.engine.processes.graphs.lowering import graph
from aiida.orm import Int


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


static_nested_gather = task(NestedGather, inputs={'sub': {'members': {'a': Int}}})


@graph
def static_nested(a: int, factor: int) -> int:
    """Wire static names into a nested dynamic namespace without forwarding."""
    total = static_nested_gather(sub={'members': {'a': a}, 'factor': factor})
    return total.total
