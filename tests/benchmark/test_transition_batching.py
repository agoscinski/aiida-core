###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Paired SQL transition benchmarks with unchanged backend durability policies.

Run with ``uv run pytest -m presto tests/benchmark/test_transition_batching.py
-o addopts='' --benchmark-enable -s``. Select PostgreSQL with ``--db-backend=psql``. Every
pair rotates its ordering and creates new workflows; the test profile and
repository are fresh. Times include local workflow execution, not daemon or
scheduler latency. Commit counts include read-only transaction completions.
"""

import json
import statistics
import time

import pytest
from sqlalchemy import event

from aiida.orm import Int
from tests.benchmark.test_engine import WorkchainLoop
from tests.engine.test_transition_batching import OutputCalculation, ParentWorkChain


@pytest.mark.parametrize('process_class', [WorkchainLoop, OutputCalculation, ParentWorkChain])
@pytest.mark.benchmark(group='transition-batching')
def test_paired_transition_batching(benchmark, backend, process_class, runner, monkeypatch):
    """Compare equivalent grouping without deriving speedups from waiting time."""
    session = backend.get_session()
    engine = session.get_bind()
    samples = {False: [], True: []}
    commits = []
    pending_commit = None

    def before_commit(connection):
        nonlocal pending_commit
        pending_commit = time.monotonic()

    def after_commit(session):
        nonlocal pending_commit
        if pending_commit is not None:
            commits.append(time.monotonic() - pending_commit)
            pending_commit = None

    event.listen(engine, 'commit', before_commit)
    event.listen(session, 'after_commit', after_commit)
    pairs = 0

    def run_pair():
        nonlocal pairs
        order = (False, True) if pairs % 2 == 0 else (True, False)
        pairs += 1
        for batch in order:
            commits.clear()
            inputs = {'iterations': Int(4)} if process_class is WorkchainLoop else {}
            with monkeypatch.context() as policy:
                policy.setattr(runner, '_batch_transitions', batch)
                start = time.monotonic()
                result = runner.run_get_node(process_class, **inputs)
                elapsed = time.monotonic() - start
            acknowledgement = sum(commits)
            # Capture metrics before correctness checks perform additional queries.
            samples[batch].append(
                {
                    'commits': len(commits),
                    'elapsed_seconds': elapsed,
                    'acknowledgement_seconds': acknowledgement,
                    'execution_seconds': elapsed - acknowledgement,
                }
            )
            assert result.node.is_finished_ok
            assert result.node.is_sealed
            assert result.node.checkpoint is None
            if process_class is not WorkchainLoop:
                assert result.result['result'].value == 7

    try:
        benchmark.pedantic(run_pair, rounds=10, iterations=1, warmup_rounds=1)
    finally:
        event.remove(engine, 'commit', before_commit)
        event.remove(session, 'after_commit', after_commit)
    summary = {
        str(batch): {key: statistics.median(sample[key] for sample in values) for key in values[0]}
        for batch, values in samples.items()
    }
    assert summary['True']['commits'] < summary['False']['commits']
    benchmark.extra_info.update(summary)
    print(f'\n{process_class.__name__}: {json.dumps(summary, sort_keys=True)}')
