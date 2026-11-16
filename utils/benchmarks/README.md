# Graph benchmarks and authoring evaluations

These opt-in studies are separate from the regular regression suite:

- `graph_engine/`: matched WorkChain/graph runtime timing harnesses.
- `graph_restart_eval/`: isolated agent-authoring evaluation, including task
  prompts, fixtures, grading, and the evaluation runner. See its own README.

Correctness tests and their shared workloads live in
`tests/engine/processes/graphs/test_matched*.py` and `programs/matched*.py`.
Existing upstream benchmarks in `tests/benchmark/` are unchanged.

Recorded results and immutable execution logs remain under `reports/`.
Historical reports refer to the paths used when they were measured; timing
harnesses formerly in `tests/benchmark/` now live in
`utils/benchmarks/graph_engine/`.
