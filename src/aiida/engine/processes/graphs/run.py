###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Checkpointable scheduling decisions for static task graphs."""

from __future__ import annotations

import typing as t
from collections.abc import Mapping
from dataclasses import dataclass, field

from aiida.common.links import LinkType
from aiida.engine.processes.generic.ports import UNSPECIFIED
from aiida.engine.processes.graphs.inputs import at, place
from aiida.engine.processes.graphs.spec import (
    BranchControl,
    GraphSpec,
    GraphTask,
    LoopControl,
    ProcessTask,
    SubgraphTask,
    readable_version,
)
from aiida.engine.processes.ports import OutputPort, PortNamespace
from aiida.orm import EnumData, Node, load_node
from aiida.orm.nodes.data.base import BaseType

__all__ = ('GraphRun',)

STATE_VERSION = '0.1'


def holds(condition: t.Any) -> bool:
    """Evaluate the value of a condition node rather than the node's identity."""
    return bool(condition.value if isinstance(condition, BaseType) else condition)


def returned(node: Node) -> dict[str, t.Any]:
    """Read the leaf outputs of one graph iteration."""
    return {entry.link_label: entry.node for entry in node.base.links.get_outgoing(link_type=LinkType.RETURN).all()}


@dataclass(frozen=True)
class Start:
    """A run to dispatch under an explicit provenance call-link name."""

    instance: str
    task: GraphTask
    inputs: dict[str, t.Any]
    body: GraphSpec | None = None


@dataclass
class Step:
    """The dispatches and notes decided by one scheduling step."""

    starts: list[Start] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def begin(self, instance: str, task: GraphTask, inputs: dict[str, t.Any], body: GraphSpec | None = None) -> None:
        """Record the dispatch of one task instance within this step."""
        self.starts.append(Start(instance=instance, task=task, inputs=inputs, body=body))

    def note(self, text: str) -> None:
        """Record a human-readable scheduling decision within this step."""
        self.notes.append(text)


class GraphRunState(t.TypedDict, total=False):
    """The checkpointable scheduling progress of a graph run."""

    version: str
    instances: dict[str, list[str]]
    dispatched: dict[str, int]
    done: dict[str, int]
    skipped: list[str]
    passed: dict[str, dict[str, t.Any]]


@dataclass
class GraphRun:
    """How far a graph has progressed; dispatch itself belongs to GraphProcess."""

    graph: GraphSpec
    given: dict[str, t.Any] = field(default_factory=dict)
    instances: dict[str, list[str]] = field(default_factory=dict)
    dispatched: dict[str, int] = field(default_factory=dict)
    done: dict[str, int] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)
    passed: dict[str, dict[str, t.Any]] = field(default_factory=dict)

    @property
    def pending(self) -> dict[str, int]:
        """Return the dispatched instances that have not completed yet."""
        return {instance: pk for instance, pk in self.dispatched.items() if instance not in self.done}

    @property
    def finished(self) -> set[str]:
        """Return the tasks whose instances all completed or passed state through."""
        return {
            name
            for name, runs in self.instances.items()
            if name in self.passed or all(instance in self.done for instance in runs)
        }

    def _has_result(self, name: str, pk: int) -> bool:
        """Treat declared Result errors as completed values, not scheduler failures."""
        node = load_node(pk)
        if node.is_finished_ok:
            return True
        task = self.graph.task(name)
        if not isinstance(task, ProcessTask) or task.spec.result_namespace is None or not node.is_failed:
            return False
        if 'error' not in node.outputs:
            return False
        error = node.outputs['error']
        return (
            isinstance(error, EnumData)
            and error.get_member().value in task.spec.exit_codes.values()
            and error.get_member().value.status == node.exit_status
        )

    @property
    def succeeded(self) -> set[str]:
        """Return the finished tasks whose outputs all finished successfully."""
        return {
            name
            for name in self.finished
            if name in self.passed
            or all(self._has_result(name, self.done[instance]) for instance in self.instances[name])
        }

    @property
    def failed(self) -> list[str]:
        """Return the finished tasks with at least one unsuccessful output, in graph order."""
        return [name for name in self.graph.task_names if name in self.finished and name not in self.succeeded]

    @property
    def settled(self) -> set[str]:
        """Return the tasks that need no further scheduling."""
        return self.succeeded | self.skipped

    @property
    def decided(self) -> set[str]:
        """Return the tasks with instances, plus the skipped ones."""
        return set(self.instances) | self.skipped

    def step(self) -> Step:
        """Advance scheduling: continue loops, then dispatch every ready task."""
        step = Step()
        for task in self.graph.tasks:
            if isinstance(task, LoopControl) and task.name in self.instances:
                self._continue_loop(task, step)
        while ready := self.graph.ready(self.settled, self.decided):
            for name in ready:
                self._begin(name, step)
        return step

    def started(self, instance: str, pk: int) -> None:
        """Record the node dispatched for one task instance."""
        self.dispatched[instance] = pk

    def completed(self, instance: str, pk: int) -> None:
        """Record the node produced by one task instance."""
        self.done[instance] = pk

    def outputs(self) -> dict[str, t.Any]:
        """Collect the graph outputs from passthrough inputs and task results."""
        produced = {}
        for output, source in self.graph.outputs.items():
            if source.task is None:
                produced[output] = at(self.given, source.port)
            elif source.task in self.passed:
                produced[output] = at(self.passed[source.task], source.port)
            elif source.task not in self.skipped:
                produced[output] = self._produced_value(source.task, source.port)
        return {name: value for name, value in produced.items() if value is not None}

    def to_dict(self) -> GraphRunState:
        """Return the checkpointable scheduling progress, without the graph or inputs."""
        return {
            'version': STATE_VERSION,
            'instances': {name: list(runs) for name, runs in self.instances.items()},
            'dispatched': dict(self.dispatched),
            'done': dict(self.done),
            'skipped': sorted(self.skipped),
            'passed': {name: dict(outputs) for name, outputs in self.passed.items()},
        }

    @classmethod
    def from_dict(cls, graph: GraphSpec, given: dict[str, t.Any], data: Mapping[str, t.Any]) -> GraphRun:
        """Rebuild the scheduling progress from a checkpoint."""
        readable_version(data.get('version', STATE_VERSION), STATE_VERSION, 'graph run')
        return cls(
            graph=graph,
            given=given,
            instances={name: list(runs) for name, runs in data.get('instances', {}).items()},
            dispatched=dict(data.get('dispatched', {})),
            done=dict(data.get('done', {})),
            skipped=set(data.get('skipped', ())),
            passed={name: dict(outputs) for name, outputs in data.get('passed', {}).items()},
        )

    def _begin(self, name: str, step: Step) -> None:
        """Dispatch one ready task, skipping it when a predecessor was skipped."""
        missing = sorted(self.graph.predecessors(name) & self.skipped)
        if missing:
            step.note(f'task `{name}` will not run, since `{missing[0]}` did not')
            self.skipped.add(name)
            return
        task = self.graph.task(name)
        inputs = self._inputs_for(task)
        if isinstance(task, BranchControl):
            self._begin_branch(task, inputs, step)
        elif isinstance(task, LoopControl):
            self._begin_loop(task, inputs, step)
        else:
            self.instances[name] = [name]
            step.begin(name, task, inputs, task.body if isinstance(task, SubgraphTask) else None)

    def _begin_branch(self, task: BranchControl, inputs: dict[str, t.Any], step: Step) -> None:
        """Dispatch the taken side of a branch, skipping when it has no body."""
        condition = inputs.pop(task.condition_port, None)
        if task.condition_test == 'is_none':
            selected = condition is None
        elif task.condition_test == 'is_not_none':
            selected = condition is not None
        else:
            selected = holds(condition)
        taken = task.body if selected else task.otherwise
        if taken is None:
            self.skipped.add(task.name)
            return
        self.instances[task.name] = [task.name]
        # Each side may consume a different subset of the enclosing inputs.
        step.begin(task.name, task, {key: value for key, value in inputs.items() if key in taken.inputs}, taken)

    def _begin_loop(self, task: LoopControl, inputs: dict[str, t.Any], step: Step) -> None:
        """Start iterating a loop, or pass through never-entered initial state."""
        if not holds(inputs.get(task.condition_port, True)):
            self._pass_initial_state(task, inputs)
            return
        self.instances[task.name] = []
        self._begin_iteration(task, inputs, step)

    def _pass_initial_state(self, task: LoopControl, inputs: dict[str, t.Any]) -> None:
        """Complete a never-entered loop with its initial state, like Python would."""
        self.instances[task.name] = [task.name]
        self.passed[task.name] = {name: inputs[name] for name in task.body.outputs if name in inputs}

    def _continue_loop(self, task: LoopControl, step: Step) -> None:
        """Dispatch the next iteration once the previous one finished successfully."""
        runs = self.instances[task.name]
        if not runs or any(instance not in self.done for instance in runs):
            return
        last = load_node(self.done[runs[-1]])
        if not last.is_finished_ok or not holds(returned(last).get(task.condition_port)):
            return
        if len(runs) >= task.max_iterations:
            msg = f'loop `{task.name}` exceeded {task.max_iterations} iterations with its condition still true.'
            raise RuntimeError(msg)
        self._begin_iteration(task, {**self._inputs_for(task), **returned(last)}, step)

    def _begin_iteration(self, task: LoopControl, inputs: dict[str, t.Any], step: Step) -> None:
        """Dispatch one numbered loop iteration over the loop body."""
        instance = f'{task.name}_iteration_{len(self.instances[task.name])}'
        self.instances[task.name].append(instance)
        step.begin(
            instance, task, {name: value for name, value in inputs.items() if name in task.body.inputs}, task.body
        )

    def _inputs_for(self, task: GraphTask) -> dict[str, t.Any]:
        """Wire graph inputs and dependency outputs into one task's declared inputs."""

        # Namespace dictionaries are mutable during wiring; never mutate the saved declaration.
        def copy(value: t.Any) -> t.Any:
            return {name: copy(item) for name, item in value.items()} if isinstance(value, dict) else value

        inputs = copy(task.inputs)
        for name, targets in self.graph.inputs.items():
            try:
                value = self._value_at(self.given, self.graph.input_spec(), name)
            except KeyError:
                continue
            for target, port in targets:
                if target == task.name:
                    place(inputs, port, value)
        for edge in self.graph.dependencies:
            if edge.target == task.name and edge.carried_between is not None:
                source, target = edge.carried_between
                place(inputs, target, self._produced_value(edge.source, source))
        return inputs

    def _produced_value(self, name: str, path: str) -> t.Any:
        """Represent absent optional output arms as None without inventing data nodes."""
        node = self._produced_by(name)
        outputs = node.outputs
        # The node's link manager is not a Mapping; namespace inputs require one.
        values = {key: outputs[key] for key in outputs}
        task = self.graph.task(name)
        ports = task.output_ports()
        if isinstance(task, ProcessTask) and task.spec.result_namespace is not None and node.is_finished_ok:
            value_port = ports['value']
            if isinstance(value_port, PortNamespace):
                # A successful model can have only null fields, and therefore no CREATE links.
                values['value'] = self._restore_nulls(value_port, values.get('value', {}))
        return self._value_at(values, ports, path)

    @staticmethod
    def _restore_nulls(ports: PortNamespace, values: Mapping[str, t.Any]) -> dict[str, t.Any]:
        """Reconstruct nullable model fields that cannot be represented by data links."""
        result = dict(values)
        for name, port in ports.items():
            value = result.get(name, UNSPECIFIED)
            if isinstance(port, PortNamespace):
                if value is UNSPECIFIED and port.nullable:
                    result[name] = None
                elif value is UNSPECIFIED:
                    result[name] = GraphRun._restore_nulls(port, {})
                else:
                    result[name] = GraphRun._restore_nulls(port, value)
            elif isinstance(port, OutputPort) and value is UNSPECIFIED:
                kinds = port.valid_type or ()
                kinds = (kinds,) if isinstance(kinds, type) else kinds
                if type(None) in kinds:
                    result[name] = None
        return result

    @staticmethod
    def _value_at(value: t.Any, ports: PortNamespace, path: str) -> t.Any:
        """Resolve a path with None for absent optional leaves or namespace arms."""
        for segment in path.split('.') if path else ():
            port = ports[segment]
            try:
                value = value[segment]
            except KeyError:
                if not port.required:
                    return None
                raise
            if isinstance(port, PortNamespace):
                ports = port
        return value

    def _produced_by(self, name: str) -> Node:
        """Return the node produced by the latest instance of one task."""
        return load_node(self.done[self.instances[name][-1]])
