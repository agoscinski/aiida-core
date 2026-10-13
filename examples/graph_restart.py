###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Shareable example: a `BaseRestartWorkChain` modeled as a graph `while` loop.

Mapping to `BaseRestartWorkChain` concepts:

===========================  ==================================================
WorkChain                    Graph equivalent
===========================  ==================================================
`run_calculation`            `run_relaxation` task; each `Err` exits the child
                             with a real status (410/411), like a failed sub
                             process
`inspect_process` +          `poll_*` subgraphs match on the error arm, the
`@process_handler`s           same way handlers match on exit codes
`report.do_break = True`     `retry = False`
`max_iterations`             `max_attempts` input, checked in `can_retry`
unhandled failure exit code  `ok = False` with the best energy seen so far
`ctx` across restarts        loop state (`attempt`, `best`, `retry`, `ok`)
===========================  ==================================================

Two language constraints shape this pattern. A `while` body may only rebind
one leaf per call, and loop outputs must be leaves, so a `(value, error)` pair
cannot cross a loop boundary directly. Each attempt is therefore projected
through three single-output subgraphs. For deterministic tasks the repeated
attempts agree (and AiiDA's caching dedupes identical attempts). Aborts surface
as data (`ok = False`) rather than a process exit status, so the calling graph
decides what to do with a failed restart sequence. Handler priorities/overrides
and pause/resume have no equivalent.

Import this module (do not run it directly: declarations must be importable)::

    uv run python -c "import sys; sys.path.insert(0, 'examples'); import graph_restart; graph_restart.main()"

Running the `main()` demo requires a configured AiiDA profile.
"""

from __future__ import annotations

import json

from aiida.engine import Err, ExitCode, ExitCodeEnum, Ok, PortModel, Result, graph, run_get_node, task

CONVERGED_WITHIN_STEPS = 5
"""A relaxation counts as converged once it finishes within this many steps."""


class RelaxErrors(ExitCodeEnum):
    """Declared failures of one relaxation attempt."""

    NOT_CONVERGED = ExitCode(410, 'Not converged after {steps} ionic steps.')
    WALLTIME_EXCEEDED = ExitCode(411, 'Walltime exceeded on attempt {attempt}.', invalidates_cache=True)


class Relaxed(PortModel):
    """The successful result of one relaxation attempt."""

    energy: float
    steps: int


@task
def run_relaxation(attempt: int, kill_on: int) -> Result[Relaxed, RelaxErrors]:
    """Run one relaxation attempt, simulating slow convergence.

    A declared `Err` exits the child process with the corresponding status,
    exactly like a failed sub process in a restart work chain.
    """
    if attempt == kill_on:
        # Mirrors a killed sub process: nothing usable came back.
        return Err(RelaxErrors.WALLTIME_EXCEEDED.value.format(attempt=str(attempt)))
    steps = 9 - 2 * attempt
    if steps > CONVERGED_WITHIN_STEPS:
        return Err(RelaxErrors.NOT_CONVERGED.value.format(steps=str(steps)))
    return Ok(Relaxed(energy=-100.0 + attempt, steps=steps))


@task
def exceeds_threshold(value: Relaxed) -> bool:
    """Return whether the relaxation needs more steps."""
    return value.steps > CONVERGED_WITHIN_STEPS


@task
def within_threshold(value: Relaxed) -> bool:
    """Return whether the relaxation converged."""
    return value.steps <= CONVERGED_WITHIN_STEPS


@task
def can_retry(error: RelaxErrors, attempt: int, max_attempts: int) -> bool:
    """Return whether a failed attempt deserves another restart, like a handler."""
    return error is RelaxErrors.NOT_CONVERGED and attempt < max_attempts


@task
def take_energy(value: Relaxed) -> float:
    """Carry a successful energy forward."""
    return value.energy


@task
def keep_best(best: float) -> float:
    """Carry the previous best energy forward unchanged."""
    return best


@task
def always_false() -> bool:
    """Report a failed attempt."""
    return False


@task
def increment(previous: int) -> int:
    """Count one more attempt."""
    return previous + 1


@task(outputs=('attempt', 'best', 'retry', 'ok'))
def initial_state() -> tuple[int, float, bool, bool]:
    """Seed the loop state, like the first `ctx` of the work chain."""
    return 0, 0.0, True, False


@graph
def poll_retry(attempt: int, max_attempts: int, kill_on: int):
    """Project one attempt to its restart decision, a single leaf for the loop."""
    value, error = run_relaxation(attempt=attempt, kill_on=kill_on)
    if error is None:
        retry = exceeds_threshold(value=value)
    else:
        retry = can_retry(error=error, attempt=attempt, max_attempts=max_attempts)
    return retry


@graph
def poll_best(attempt: int, best: float, kill_on: int):
    """Project one attempt to the best energy so far, a single leaf for the loop."""
    value, error = run_relaxation(attempt=attempt, kill_on=kill_on)
    if error is None:
        selected = take_energy(value=value)
    else:
        selected = keep_best(best=best)
    return selected


@graph
def poll_ok(attempt: int, max_attempts: int, kill_on: int):
    """Project one attempt to its success flag, a single leaf for the loop."""
    value, error = run_relaxation(attempt=attempt, kill_on=kill_on)
    if error is None:
        ok = within_threshold(value=value)
    else:
        ok = always_false()
    return ok


@graph
def restart_relaxation(max_attempts: int, kill_on: int = 99):
    """Restart the relaxation until it converges, gives up, or aborts."""
    attempt, best, retry, ok = initial_state()
    while retry:
        attempt = increment(previous=attempt)
        retry = poll_retry(attempt=attempt, max_attempts=max_attempts, kill_on=kill_on)
        best = poll_best(attempt=attempt, best=best, kill_on=kill_on)
        ok = poll_ok(attempt=attempt, max_attempts=max_attempts, kill_on=kill_on)
    return {'energy': best, 'converged': ok}


def attempt_history(node) -> list[tuple[int | None, str | None]]:
    """Collect `(exit_status, exit_message)` of every attempt, oldest first.

    Failed attempts are real failed nodes: each `Err` sets the child exit
    status, which is what the restart decision matched on. The three `poll_*`
    projections run the same attempt once each, so group by attempt number.
    """
    attempts = {}

    def visit(process) -> None:
        for child in process.called:
            if child.process_label == 'run_relaxation':
                attempts.setdefault(child.inputs.attempt.value, (child.exit_status, child.exit_message))
            else:
                visit(child)

    visit(node)
    return [attempts[number] for number in sorted(attempts)]


def main() -> None:
    """Build the graphs without running anything, then run three scenarios."""
    spec = restart_relaxation.build()
    print('tasks:', spec.task_names)
    assert spec.to_dict() == json.loads(json.dumps(spec.to_dict()))

    # Converges on the second attempt: exit 410, then success.
    outputs, node = run_get_node(restart_relaxation, max_attempts=5)
    print(f'max_attempts=5 -> energy={outputs["energy"].value} converged={outputs["converged"].value}')
    print(f'  attempts: {attempt_history(node)}')
    assert node.is_finished_ok

    # A single attempt is not enough: the sequence gives up but stays finished.
    outputs, node = run_get_node(restart_relaxation, max_attempts=1)
    print(f'max_attempts=1 -> energy={outputs["energy"].value} converged={outputs["converged"].value}')
    print(f'  attempts: {attempt_history(node)}')
    assert node.is_finished_ok

    # The scheduler kills the second attempt: exit 411 aborts the sequence.
    outputs, node = run_get_node(restart_relaxation, max_attempts=5, kill_on=2)
    print(f'kill_on=2 -> energy={outputs["energy"].value} converged={outputs["converged"].value}')
    print(f'  attempts: {attempt_history(node)}')
    assert node.is_finished_ok


if __name__ == '__main__':
    main()
