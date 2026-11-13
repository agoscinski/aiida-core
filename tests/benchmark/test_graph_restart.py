###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Matched restart correctness and benchmark harness for `graph_restart_programs`.

The work-chain implementation (:class:`RestartBenchmarkWorkChain`) and the graph
implementation (:data:`restartable`) drive the same real child (:class:`FlakyStep`): two
transient failures and then success on ``recover``, an unclaimable failure on ``fatal``,
and endless transient failures on ``always-fail``. The correctness tests pin that parity
at small budgets, including the deliberate vocabulary split (the work chain reports
``401``/``402`` while the graph reports ``400`` through its raising ``ensure``).

Timing modes, all on the ``recover`` scenario with budget five (three step runs):

* ``workchain``: local ``run_get_node`` of the restart work chain.
* ``graph``: local ``run_get_node`` of the authoring ``@graph`` handle.
* ``graph-direct-ir``: local ``run_get_node`` of ``GraphProcess`` with the stored IR
  launch inputs from ``handle.get_launch_inputs`` (same kwargs, lowering outside timing).
* ``workchain-submitted``: daemon ``submit_and_await`` of the restart work chain.
* ``graph-submitted``: daemon ``submit_and_await`` of ``GraphProcess`` with launch inputs
  resolved from the authoring handle per round.
* ``graph-submitted-direct-ir``: daemon ``submit_and_await`` of ``GraphProcess`` with a
  pre-serialized IR ``Dict`` and bare ``graph_inputs`` (the stored-declaration path).
* ``restart-ir``: local lowering-cache hit plus the ``to_dict``/``from_dict`` round trip
  (the IR assembly/serialization cost, kept separate from execution timing).

Setup -- the launch kwargs and, for the graph side, the warmed ``compile_graph(handle)`` lowering
cache -- stays outside the timed region, so the timer covers warm execution only. Local
modes reuse their kwargs across rounds (so the ``graph-direct-ir`` mode relinks the same
stored declaration ``Dict`` after warmup). Daemon modes differ deliberately: the
``graph-submitted`` mode resolves ``get_launch_inputs`` from the authoring handle per round
inside the timed region to keep the authoring path honest, while the ``direct-ir`` mode
keeps the serialized payload template outside timing but builds a fresh declaration ``Dict``
per round inside it.

Rounds: local execution modes run 10 rounds with 1 warmup; daemon modes run 5 rounds with
1 warmup because each round submits through the broker. Benchmarks run serially; never
alongside other ongoing work. Every mode uses supported launch paths only: no mocked
daemon, no global profile changes, and no skips. If a mode cannot launch, it must fail
loudly so the blocker is visible, never pass as a fake skip.

Run correctness only (benchmarks skip by default via ``--benchmark-skip``)::

    uv run pytest -m presto tests/benchmark/test_graph_restart.py

Run benchmarks only (JSON report for all modes)::

    uv run pytest -m presto tests/benchmark/test_graph_restart.py --benchmark-only \
        --benchmark-json=/tmp/graph-restart-benchmark.json
"""

from __future__ import annotations

import typing as t

import pytest

from aiida.common.links import LinkType
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.lowering import compile_graph
from aiida.engine.processes.graphs.process import GraphProcess
from aiida.orm import Dict, Float, GraphNode, WorkChainNode
from aiida.orm.nodes.process.process import ProcessNode
from tests.benchmark.graph_restart_programs import (
    RECOVER_RUNS,
    RECOVER_VALUE,
    RESTART_BENCHMARKS,
    prepare_restart_inputs,
    prepare_restart_wc_inputs,
    restartable,
    validate_restart_benchmark,
)

pytestmark = pytest.mark.presto

#: Budget the timed benchmarks allow; ``recover`` needs exactly three step runs.
BENCHMARK_BUDGET = 5

#: Local execution timing: ten measured rounds after one warmup round.
LOCAL_ROUNDS = 10
LOCAL_WARMUP = 1

#: Daemon timing: five measured rounds after one warmup round (broker submission is expensive).
DAEMON_ROUNDS = 5
DAEMON_WARMUP = 1

#: Per-round broker wait budget for daemon modes (three sequential child work chains plus helpers).
DAEMON_TIMEOUT = 120

#: Expected task descendants on ``recover``: 3 step work chains, no child graphs, and exactly
#: seven calcfunctions (``zero`` + 3 ``should_restart`` + 2 ``advance`` + ``ensure_recovered``).
_EXPECTED_GRAPH: dict[str, int] = {'graphs': 0, 'calcfunctions': 7, 'workchains': 3}


def _by_type(nodes: t.Iterable[ProcessNode]) -> dict[str, list[ProcessNode]]:
    """Bucket process nodes by class name for structural assertions."""
    bucketed: dict[str, list[ProcessNode]] = {}
    for node in nodes:
        bucketed.setdefault(type(node).__name__, []).append(node)
    return bucketed


def _step_runs(node: ProcessNode) -> list[WorkChainNode]:
    """Return the shared-step descendants in run order (attempt ``0, 1, ...``)."""
    steps = [child for child in node.called_descendants if isinstance(child, WorkChainNode)]
    return sorted(steps, key=lambda step: step.inputs.attempt.value)


def _successful_step_values(node: ProcessNode) -> list[Float]:
    """Return the ``value`` outputs of the shared-step runs that succeeded."""
    return [step.outputs.value for step in _step_runs(node) if 'value' in step.outputs]


def _assert_no_workflow_create(node: ProcessNode) -> None:
    """Assert no workflow node in the tree carries a CREATE link (workflows only RETURN)."""
    for child in (node, *node.called_descendants):
        if isinstance(child, (WorkChainNode, GraphNode)):
            assert child.base.links.get_outgoing(link_type=LinkType.CREATE).all() == []


def _assert_attempt_schedule(node: ProcessNode, attempts: list[int]) -> None:
    """Assert the step runs used exactly the given zero-based attempt numbers in order."""
    assert [step.inputs.attempt.value for step in _step_runs(node)] == attempts


def test_graph_template() -> None:
    """The serialized GraphIR the restart graph executes matches its lowered source template."""
    from aiida.engine.processes.graphs.ir import GraphIR

    (handle, _workchain) = RESTART_BENCHMARKS['restart-recover']
    assert handle is restartable
    assert validate_restart_benchmark() == {'restartable-loop-placements': 1}
    template = compile_graph(handle)
    assert GraphIR.from_dict(template.to_dict()).to_dict() == template.to_dict()


@pytest.mark.usefixtures('aiida_profile_clean')
def test_correctness_recover_matched() -> None:
    """Both implementations recover after two transient failures with identical provenance.

    Work-chain side: finished ok, ``value == 1.5``, three ``FlakyStep`` children with attempts
    ``0, 1, 2``. Graph side: finished ok, ``result == 1.5``, the same three step children with
    the same attempt schedule, a stored declaration identical to the lowered template, no child
    graph node anywhere, and no CREATE link from any workflow node on either side. Both sides
    forward the very ``result`` node they were given from the successful step run (provenance
    passthrough, no new data); the graph's final ``result`` is re-wrapped by its raising
    ``ensure`` calcfunction, exactly like the upstream ``restartable`` program.
    """
    from aiida.engine.processes.graphs.ir import GraphIR

    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']
    shared = Float(RECOVER_VALUE)

    wc_outputs, wc_node = run_get_node(
        workchain, **prepare_restart_wc_inputs(mode='recover', max_iterations=5, result=shared)
    )
    assert not wc_node.is_excepted, wc_node.exception
    assert wc_node.is_finished_ok, (wc_node.exit_status, wc_node.exit_message)
    assert isinstance(wc_node, WorkChainNode)
    assert wc_outputs['value'].value == RECOVER_VALUE
    assert wc_outputs['value'].uuid == shared.uuid
    assert len(_step_runs(wc_node)) == RECOVER_RUNS
    _assert_attempt_schedule(wc_node, [0, 1, 2])
    assert len(wc_node.called) == RECOVER_RUNS
    _assert_no_workflow_create(wc_node)

    graph_outputs, graph_node = run_get_node(
        handle, **prepare_restart_inputs(mode='recover', max_attempts=5, result=shared)
    )
    assert not graph_node.is_excepted, graph_node.exception
    assert graph_node.is_finished_ok, (graph_node.exit_status, graph_node.exit_message)
    assert isinstance(graph_node, GraphNode)
    assert graph_outputs['result'].value == RECOVER_VALUE
    assert [value.uuid for value in _successful_step_values(graph_node)] == [shared.uuid]
    assert [value.uuid for value in _successful_step_values(wc_node)] == [shared.uuid]
    assert len(_step_runs(graph_node)) == RECOVER_RUNS
    _assert_attempt_schedule(graph_node, [0, 1, 2])
    stored = graph_node.base.links.get_incoming(link_type=LinkType.INPUT_WORK).get_node_by_label('graph')
    assert GraphIR.from_dict(stored.get_dict()).to_dict() == compile_graph(handle).to_dict()
    descendants = _by_type(graph_node.called_descendants)
    assert len(descendants.get('GraphNode', [])) == _EXPECTED_GRAPH['graphs']
    assert len(descendants.get('CalcFunctionNode', [])) == _EXPECTED_GRAPH['calcfunctions']
    assert len(descendants.get('WorkChainNode', [])) == _EXPECTED_GRAPH['workchains']
    direct = _by_type(graph_node.called)
    assert len(direct.get('GraphNode', [])) == 0
    assert len(direct.get('WorkChainNode', [])) == RECOVER_RUNS
    _assert_no_workflow_create(graph_node)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_correctness_recover_direct_ir() -> None:
    """The stored-IR launch of the graph recovers exactly like the authoring launch."""
    (handle, _workchain) = RESTART_BENCHMARKS['restart-recover']
    handle.build()
    shared = Float(RECOVER_VALUE)
    launch_inputs = handle.get_launch_inputs(**prepare_restart_inputs(mode='recover', max_attempts=5, result=shared))
    outputs, node = run_get_node(GraphProcess, **launch_inputs)
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert outputs['result'].value == RECOVER_VALUE
    assert [value.uuid for value in _successful_step_values(node)] == [shared.uuid]
    _assert_attempt_schedule(node, [0, 1, 2])


@pytest.mark.usefixtures('aiida_profile_clean')
def test_correctness_fatal_matched() -> None:
    """An unclaimable failure aborts both implementations after a single run.

    The work chain reports ``402`` (``ERROR_UNHANDLED_FAILURE``); the graph reports ``400``
    because ``ensure_recovered`` raises. Both run the step exactly once and store no value.
    A raising ``ensure`` is a controlled task failure (``is_failed``, never ``is_excepted``),
    exactly like the upstream ``restartable`` program when attempts run out.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']

    _wc_outputs, wc_node = run_get_node(workchain, **prepare_restart_wc_inputs(mode='fatal', max_iterations=5))
    assert not wc_node.is_excepted, wc_node.exception
    assert wc_node.is_failed
    assert wc_node.exit_status == 402
    assert len(_step_runs(wc_node)) == 1

    graph_outputs, graph_node = run_get_node(handle, **prepare_restart_inputs(mode='fatal', max_attempts=5))
    assert not graph_node.is_excepted, graph_node.exception
    assert graph_node.is_failed
    assert graph_node.exit_status == 400
    assert dict(graph_outputs) == {}
    assert len(_step_runs(graph_node)) == 1


@pytest.mark.usefixtures('aiida_profile_clean')
def test_correctness_exhausted_matched() -> None:
    """A transient failure with no budget left aborts both implementations after two runs.

    The work chain reports ``401`` (``ERROR_MAXIMUM_ITERATIONS_EXCEEDED``); the graph reports
    ``400`` through ``ensure_recovered``. Both run the step exactly twice, attempts ``0, 1``.
    Neither side is excepted: both are controlled failures.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']

    _wc_outputs, wc_node = run_get_node(workchain, **prepare_restart_wc_inputs(mode='always-fail', max_iterations=2))
    assert not wc_node.is_excepted, wc_node.exception
    assert wc_node.is_failed
    assert wc_node.exit_status == 401
    _assert_attempt_schedule(wc_node, [0, 1])

    graph_outputs, graph_node = run_get_node(handle, **prepare_restart_inputs(mode='always-fail', max_attempts=2))
    assert not graph_node.is_excepted, graph_node.exception
    assert graph_node.is_failed
    assert graph_node.exit_status == 400
    assert dict(graph_outputs) == {}
    _assert_attempt_schedule(graph_node, [0, 1])


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('budget', [1, RECOVER_RUNS])
def test_bounds_minimal_budget(budget: int) -> None:
    """Boundary budgets behave identically on run count, differing only in vocabulary.

    Budget three recovers with exactly three runs and ``value == 1.5`` on both sides. Budget
    one runs the step once and fails: the work chain reports ``401`` (a handler claimed the
    failure but the budget is spent) while the graph reports ``400`` via ``ensure_recovered``.
    Neither failing side is excepted.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']

    wc_outputs, wc_node = run_get_node(workchain, **prepare_restart_wc_inputs(mode='recover', max_iterations=budget))
    graph_outputs, graph_node = run_get_node(handle, **prepare_restart_inputs(mode='recover', max_attempts=budget))

    if budget == RECOVER_RUNS:
        assert not wc_node.is_excepted, wc_node.exception
        assert wc_node.is_finished_ok, (wc_node.exit_status, wc_node.exit_message)
        assert wc_outputs['value'].value == RECOVER_VALUE
        assert not graph_node.is_excepted, graph_node.exception
        assert graph_node.is_finished_ok, (graph_node.exit_status, graph_node.exit_message)
        assert graph_outputs['result'].value == RECOVER_VALUE
    else:
        assert not wc_node.is_excepted, wc_node.exception
        assert wc_node.is_failed
        assert wc_node.exit_status == 401
        assert not graph_node.is_excepted, graph_node.exception
        assert graph_node.is_failed
        assert graph_node.exit_status == 400
        assert dict(graph_outputs) == {}
    _assert_attempt_schedule(wc_node, list(range(budget)))
    _assert_attempt_schedule(graph_node, list(range(budget)))


def _local_cases() -> list:
    """Return the local execution benchmark cases."""
    return [
        pytest.param('workchain', marks=pytest.mark.benchmark(group='restart'), id='restart-workchain'),
        pytest.param('graph', marks=pytest.mark.benchmark(group='restart'), id='restart-graph'),
        pytest.param('graph-direct-ir', marks=pytest.mark.benchmark(group='restart'), id='restart-graph-direct-ir'),
    ]


@pytest.mark.timeout(1200)
@pytest.mark.parametrize('implementation', _local_cases())
def test_benchmark_restart_local(benchmark, implementation) -> None:
    """Time warm ``run_get_node`` for one restart implementation (10 rounds, 1 warmup).

    All setup -- the launch kwargs and, for the graph side, the warmed ``compile_graph(handle)``
    lowering cache -- happens outside the timed region for every implementation alike.
    The authoring ``graph`` mode and the ``graph-direct-ir`` mode therefore time the same
    warm execution shape; the direct mode additionally reuses its stored declaration
    ``Dict`` across rounds (relinked after warmup) rather than re-serializing per round.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']
    if implementation == 'workchain':
        target = workchain
        kwargs: dict[str, t.Any] = prepare_restart_wc_inputs(mode='recover', max_iterations=BENCHMARK_BUDGET)
    elif implementation == 'graph':
        handle.build()
        target = handle
        kwargs = prepare_restart_inputs(mode='recover', max_attempts=BENCHMARK_BUDGET)
    else:
        handle.build()
        target = GraphProcess
        kwargs = handle.get_launch_inputs(**prepare_restart_inputs(mode='recover', max_attempts=BENCHMARK_BUDGET))

    def _run():
        return run_get_node(target, **kwargs)

    outputs, node = benchmark.pedantic(_run, iterations=1, rounds=LOCAL_ROUNDS, warmup_rounds=LOCAL_WARMUP)

    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    descendants = _by_type(node.called_descendants)
    benchmark.extra_info['provenance'] = {
        'workload': 'restart-recover',
        'implementation': implementation,
        'finished_ok': node.is_finished_ok,
        'graph_nodes': len(descendants.get('GraphNode', [])),
        'workchains': len(descendants.get('WorkChainNode', [])),
        'calcfunctions': len(descendants.get('CalcFunctionNode', [])),
        'calcjobs': len(descendants.get('CalcJobNode', [])),
    }
    if implementation == 'workchain':
        assert outputs['value'].value == RECOVER_VALUE
    else:
        assert outputs['result'].value == RECOVER_VALUE


@pytest.mark.timeout(600)
def test_benchmark_restart_ir(benchmark) -> None:
    """Time the IR assembly/serialization cost separately from execution.

    Each round hits the warmed ``compile_graph(handle)`` cache and runs the ``to_dict`` /
    ``from_dict`` round trip that the stored-declaration (``direct-ir``) launch paths pay.
    The equality check stays outside the timed region so the timer measures the round
    trip itself, not validation overhead (no second serialization inside ``_run``).
    """
    from aiida.engine.processes.graphs.ir import GraphIR

    (handle, _workchain) = RESTART_BENCHMARKS['restart-recover']
    handle.build()

    def _run():
        template = compile_graph(handle)
        payload = template.to_dict()
        restored = GraphIR.from_dict(payload)
        return restored, payload

    restored, payload = benchmark.pedantic(_run, iterations=1, rounds=LOCAL_ROUNDS, warmup_rounds=LOCAL_WARMUP)
    assert restored.to_dict() == payload


def _daemon_cases() -> list:
    """Return the daemon (submitted) benchmark cases."""
    return [
        pytest.param(
            'workchain-submitted', marks=pytest.mark.benchmark(group='restart-daemon'), id='restart-workchain-daemon'
        ),
        pytest.param('graph-submitted', marks=pytest.mark.benchmark(group='restart-daemon'), id='restart-graph-daemon'),
        pytest.param(
            'graph-submitted-direct-ir',
            marks=pytest.mark.benchmark(group='restart-daemon'),
            id='restart-graph-direct-ir-daemon',
        ),
    ]


@pytest.mark.timeout(600)
@pytest.mark.usefixtures('started_daemon_client')
@pytest.mark.parametrize('submission', ['workchain', 'graph', 'graph-direct-ir'])
def test_correctness_recover_submitted(submit_and_await, submission) -> None:
    """Daemon submission recovers without any timing harness, one submission mode per case.

    ``workchain`` submits the restart work chain; ``graph`` submits ``GraphProcess`` with
    launch inputs resolved from the authoring handle (``submit_and_await`` only accepts a
    process class, builder, or node, so the handle itself is not submittable -- the helper
    path is the supported authoring submission); ``graph-direct-ir`` submits the same
    process with a pre-serialized declaration ``Dict`` and bare ``graph_inputs``. Every
    mode asserts the same matched provenance as the local run: three step runs on attempts
    ``0, 1, 2``, ``value == 1.5`` passthrough of the shared ``result`` node, and no CREATE
    link from any workflow node.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']
    template = compile_graph(handle)
    shared = Float(RECOVER_VALUE)

    if submission == 'workchain':
        node = submit_and_await(
            workchain,
            timeout=DAEMON_TIMEOUT,
            **prepare_restart_wc_inputs(mode='recover', max_iterations=5, result=shared),
        )
        assert not node.is_excepted, node.exception
        assert node.is_finished_ok, (node.exit_status, node.exit_message)
        assert node.outputs.value.value == RECOVER_VALUE
        assert node.outputs.value.uuid == shared.uuid
        _assert_attempt_schedule(node, [0, 1, 2])
        _assert_no_workflow_create(node)
        return

    if submission == 'graph':
        node = submit_and_await(
            GraphProcess,
            timeout=DAEMON_TIMEOUT,
            **handle.get_launch_inputs(**prepare_restart_inputs(mode='recover', max_attempts=5, result=shared)),
        )
    else:
        graph_kwargs = prepare_restart_inputs(mode='recover', max_attempts=5, result=shared)
        node = submit_and_await(
            GraphProcess,
            graph=Dict(dict=template.to_dict()),
            graph_inputs=dict(graph_kwargs),
            timeout=DAEMON_TIMEOUT,
        )
    assert not node.is_excepted, node.exception
    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    assert node.outputs.result.value == RECOVER_VALUE
    assert [value.uuid for value in _successful_step_values(node)] == [shared.uuid]
    _assert_attempt_schedule(node, [0, 1, 2])
    _assert_no_workflow_create(node)


@pytest.mark.timeout(1800)
@pytest.mark.usefixtures('started_daemon_client')
@pytest.mark.parametrize('implementation', _daemon_cases())
def test_benchmark_restart_daemon(benchmark, submit_and_await, implementation) -> None:
    """Time warm daemon submission for one restart implementation (5 rounds, 1 warmup).

    Uses the ``started_daemon_client`` / ``submit_and_await`` fixtures (ZeroMQ under presto);
    no global profile is modified and no unrelated daemon is touched. The ``graph-submitted``
    mode resolves ``get_launch_inputs`` from the authoring handle per round inside the timed
    region to keep the authoring path honest; the ``direct-ir`` mode keeps the serialized
    payload template outside timing but builds a fresh declaration ``Dict`` per round inside
    it, submitting bare ``graph_inputs`` instead.
    """
    (handle, workchain) = RESTART_BENCHMARKS['restart-recover']
    template = compile_graph(handle)
    wc_kwargs = prepare_restart_wc_inputs(mode='recover', max_iterations=BENCHMARK_BUDGET)
    graph_kwargs = prepare_restart_inputs(mode='recover', max_attempts=BENCHMARK_BUDGET)

    if implementation == 'workchain-submitted':

        def _run():
            return submit_and_await(workchain, timeout=DAEMON_TIMEOUT, **wc_kwargs)

    elif implementation == 'graph-submitted':

        def _run():
            return submit_and_await(GraphProcess, timeout=DAEMON_TIMEOUT, **handle.get_launch_inputs(**graph_kwargs))

    else:
        payload = template.to_dict()

        def _run():
            declaration = Dict(dict=payload)
            return submit_and_await(
                GraphProcess, graph=declaration, graph_inputs=dict(graph_kwargs), timeout=DAEMON_TIMEOUT
            )

    node = benchmark.pedantic(_run, iterations=1, rounds=DAEMON_ROUNDS, warmup_rounds=DAEMON_WARMUP)

    assert node.is_finished_ok, (node.exit_status, node.exit_message)
    descendants = _by_type(node.called_descendants)
    benchmark.extra_info['provenance'] = {
        'workload': 'restart-recover',
        'implementation': implementation,
        'finished_ok': node.is_finished_ok,
        'graph_nodes': len(descendants.get('GraphNode', [])),
        'workchains': len(descendants.get('WorkChainNode', [])),
        'calcfunctions': len(descendants.get('CalcFunctionNode', [])),
        'calcjobs': len(descendants.get('CalcJobNode', [])),
    }
    if implementation == 'workchain-submitted':
        assert node.outputs.value.value == RECOVER_VALUE
    else:
        assert node.outputs.result.value == RECOVER_VALUE
