# Matched graph runtime benchmarks

This directory contains only opt-in timing harnesses, separate from correctness
and provenance tests under `tests/engine/processes/graphs/`.

| Timing harness | Correctness tests |
| --- | --- |
| `test_graph_engine.py` | `test_matched.py` |
| `test_graph_engine_inline.py` | `test_matched_inline.py` |
| `test_graph_restart.py` | `test_matched_restart.py` |

Both suites reuse the workloads in `tests/engine/processes/graphs/programs/matched*.py`.
The engine comparisons also reuse the existing upstream WorkChain baseline in
`tests/benchmark/test_engine.py`, which is unchanged.

## Running

Validate workloads from the repository root:

```bash
uv run pytest -m presto tests/engine/processes/graphs/test_matched.py \
  tests/engine/processes/graphs/test_matched_inline.py \
  tests/engine/processes/graphs/test_matched_restart.py
```

Measure runtimes, serially and without concurrent test or evaluation runs:

```bash
uv run python -m pytest -p tests.conftest -m presto utils/benchmarks/graph_engine \
  --benchmark-only --benchmark-json=/tmp/graph-engine-benchmarks.json
```

Use `python -m pytest` so repository modules are importable, and explicitly load
`tests.conftest` to reuse backend options, temporary profiles, daemon fixtures,
and cleanup rather than duplicating them here. Pass an individual module path to
select one study. For local-only timing, add `-k 'not daemon'`.
`-m presto` selects SQLite and ZeroMQ without external PostgreSQL or RabbitMQ.

These timing modules are outside the configured default `testpaths`; they run
only when explicitly selected, and are skipped unless `--benchmark-only` is
passed. Their module docstrings describe workload differences and measurement
boundaries. Historical reports and raw logs under `reports/graph-restart-study/`
retain their original paths and measurements.
