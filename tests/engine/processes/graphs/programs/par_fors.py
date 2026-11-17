###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tasks and graphs for the `par_for` parallel fan-out behavior suite.

`par_for` fans out unary function, `CalcJob`, and `WorkChain` tasks over
`DataList`/`DataDict` wires, dispatching one independent child process per
element with a shared `runtime_config` entry under the map placement name.
`@graph` callables, non-unary tasks, and non-container collections are
rejected at build time.
"""

import typing as t

from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.common.folders import Folder
from aiida.engine import WorkChain, calcfunction, graph, par_for, task
from aiida.engine.processes.calcjobs import CalcJob
from aiida.engine.processes.calcjobs.calcjob import CalcJobProcessSpec
from aiida.orm import Code, DataDict, DataList, Int


@task
def increment(x: int) -> int:
    """Add one to the input."""
    return x + 1


@task
def explode_on_two(x: int) -> int:
    """Fail when the element is two, for failure propagation."""
    if x == 2:
        raise ValueError('deliberately failed')
    return x + 1


@task
def total(values: DataList) -> int:
    """Sum the owned integers."""
    assert isinstance(values, DataList)
    return sum(member.value for member in values)


@task
def two_inputs(x: int, y: int) -> int:
    """Take two inputs; fanning it out is rejected as non-unary."""
    return x + y


@calcfunction
def _increment_result(x: Int) -> Int:
    """Create the incremented node; a `WorkChain` outline cannot create data itself."""
    return Int(x.value + 1)


class Pair(t.TypedDict):
    """Two named outputs; fanning it out is rejected as multi-output."""

    first: int
    second: int


@task
def pair_outputs(x: int) -> Pair:
    """Return two named outputs."""
    return {'first': x, 'second': x}


class IncrementWorkChain(WorkChain):
    """Add one through a `WorkChain` with a single scientific input."""

    @classmethod
    def define(cls, spec) -> None:  # type: ignore[override]
        super().define(spec)
        spec.input('x', valid_type=Int)
        spec.output('result', valid_type=Int)
        spec.outline(cls.produce)

    def produce(self):
        """Store the incremented input."""
        self.out('result', _increment_result(self.inputs.x))


increment_chain = task(IncrementWorkChain)


class IncrementCalculation(CalcJob):
    """Increment one integer through `core.arithmetic.add` with a single scientific input.

    `code` and execution options arrive through the shared `runtime_config` entry.
    """

    @classmethod
    def define(cls, spec: CalcJobProcessSpec) -> None:
        super().define(spec)
        spec.input('code', valid_type=Code)
        spec.input('x', valid_type=Int)
        spec.output('sum', valid_type=Int)
        spec.inputs['metadata']['options']['parser_name'].default = 'core.arithmetic.add'
        spec.inputs['metadata']['options']['input_filename'].default = 'aiida.in'
        spec.inputs['metadata']['options']['output_filename'].default = 'aiida.out'
        spec.inputs['metadata']['options']['resources'].default = {'num_machines': 1, 'num_mpiprocs_per_machine': 1}

    def prepare_for_submission(self, folder: Folder) -> CalcInfo:
        """Write the increment script, run under the configured code."""
        with folder.open(self.options.input_filename, 'w', encoding='utf8') as handle:
            handle.write(f'echo $(({self.inputs.x.value} + 1))\n')
        codeinfo = CodeInfo()
        codeinfo.stdin_name = self.options.input_filename
        codeinfo.stdout_name = self.options.output_filename
        if 'code' in self.inputs:
            codeinfo.code_uuid = self.inputs.code.uuid
        calcinfo = CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.retrieve_list = [self.options.output_filename]
        return calcinfo


increment_calc = task(IncrementCalculation, outputs={'sum': Int})

unselected_calc = task(IncrementCalculation)


@graph
def for_list(values: DataList) -> DataList:
    """Fan out a task over an owned list."""
    return par_for(increment, values)


@graph
def for_mapping(values: DataDict) -> DataDict:
    """Fan out a task over owned mapping values, keeping keys."""
    return par_for(increment, values)


@graph
def for_empty(values: DataList) -> DataList:
    """Fan out a task over an empty list graph input."""
    return par_for(increment, values)


@graph
def for_chained(values: DataList) -> int:
    """Fan out over a list and consume the fanned result in a downstream task."""
    fanned = par_for(increment, values)
    return total(values=fanned)


@graph
def for_fails(values: DataList) -> DataList:
    """Fan out a task that fails on one element."""
    return par_for(explode_on_two, values)


@graph
def for_calc(values: DataList) -> DataList:
    """Fan out a `CalcJob` over an owned list; `code` arrives through `runtime_config`."""
    return par_for(increment_calc, values)


@graph
def for_chain(values: DataList) -> DataList:
    """Fan out a `WorkChain` over an owned list."""
    return par_for(increment_chain, values)


@graph
def for_keyword(values: DataList) -> DataList:
    """Fan out with keyword arguments."""
    return par_for(func=increment, values=values)


@graph
def for_after(x: int, values: DataList) -> DataList:
    """Fan out ordered after an earlier placement."""
    first = increment(x=x)
    return par_for(increment, values, after=first)


@graph
def for_in_branch(values: DataList, flag: bool) -> DataList:
    """Fan out in the taken arm, passing the input through otherwise."""
    if flag:
        fanned = par_for(increment, values)
    else:
        fanned = values
    return fanned


@graph
def for_wrapped(values: DataList) -> DataList:
    """Fan out inside a nested graph."""
    return par_for(increment, values)


@graph
def for_nested(values: DataList) -> DataList:
    """Fan out through a nested graph call."""
    return for_wrapped(values=values)


@graph
def double_graph(x: int) -> int:
    """Add one inside a nested graph; fanning it out is rejected."""
    return increment(x=x)


@graph
def for_graph(values: DataList) -> DataList:
    """Fan out a graph; rejected as not a task."""
    return par_for(double_graph, values)


@graph
def for_aliased_graph(values: DataList) -> DataList:
    """Fan out an aliased graph; rejected naming the declaration."""
    return par_for(double_graph.alias('doubled'), values)


@graph
def for_two_inputs(values: DataList) -> DataList:
    """Fan out a two-input task; rejected as non-unary."""
    return par_for(two_inputs, values)


@graph
def for_pair_outputs(values: DataList) -> DataList:
    """Fan out a two-output task; rejected as multi-output."""
    return par_for(pair_outputs, values)


@graph
def for_plain_function(values: DataList) -> DataList:
    """Fan out an undecorated built-in; rejected as unregistered."""
    return par_for(len, values)


@graph
def for_bad_collection(x: int) -> DataList:
    """Fan out over an integer; rejected as not a container."""
    return par_for(increment, x)


@graph
def for_bare_calc(values: DataList) -> DataList:
    """Fan out a multi-output `CalcJob` without a selected output; rejected with a hint."""
    return par_for(unselected_calc, values)


@graph
def for_missing_collection(values: DataList) -> DataList:
    """Fan out with only the callable; rejected as missing the collection."""
    return par_for(increment)  # type: ignore[call-arg]
