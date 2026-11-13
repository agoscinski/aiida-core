# Graph restart evaluation — `BaseRestartWorkChain` vs direct `GraphIR`

Fairness probe (n=1, see disclaimer): two agents start from scratch and solve
the **same** transient-failure retry task, one with `BaseRestartWorkChain`
(arm A), one by constructing `GraphIR` directly (arm B, no `@graph` sugar).
Judged on correctness (shared hidden acceptance tests) plus time/tokens.

Status: **harness ready — NO agent runs or timings have been collected.**
`config.json` is filled with the GO values (same budgets for both arms);
no agent has been launched. No commit/push.

## Layout

- `TASK_COMMON.md` — shared problem statement (same for both arms; includes
  the isolation rules each agent must obey).
- `ARM_A_BRWC.md` / `ARM_B_GRAPHIR.md` — the ONLY allowed method per arm
  (high-level method constraints only; no solution supplied to either arm).
- `fixtures/flaky_child.py` — shared flaky collaborator (agents may read,
  never modify; hidden tests import the identical file).
- `run_eval.py` — orchestrator: builds isolated worktrees, pre-warms
  `.venv` via `uv sync` for BOTH arms before either solve starts, launches
  one fresh `pi --mode json` process group per arm, records transcripts +
  metrics + run cards.
- `parse_events.py` — parses `pi --mode json` JSONL: authoritative
  `message_end` usage (`input/output/cacheRead/cacheWrite/totalTokens`),
  provider/model, stop reason; wall time comes from the harness monotonic
  clock; `--check-cap N` does a post-hoc budget check.
- `tests_hidden/` — shared hidden acceptance tests plus `conftest.py`
  (loads `aiida.tools.pytest_fixtures` outside `tests/`, SQLite + ZeroMQ
  defaults) and `verify_arm.py` (the ONLY supported verification entry
  point). **LOCAL ONLY, git-ignored, never copied into agent worktrees,
  never committed, run only AFTER both agents finish.**
- `reports/` — untracked local run cards, prompts, transcripts, metrics,
  session logs (outside both arm worktrees and outside this conversation).
- `config.json` — GO values: `model=gpt-6.1-sol`, `provider=openai-codex`,
  `thinking=medium`, `timeout_s_each_arm=1800`,
  `max_total_tokens_each_arm=1000000` (same for both arms).

## Ready command (at GO)

```sh
# config.json already holds the GO values (no placeholders left).
# Confirm the model resolves: pi --list-models gpt-6.1-sol
# Launch (from this worktree root):
uv run python utils/benchmarks/graph_restart_eval/run_eval.py \
  --config utils/benchmarks/graph_restart_eval/config.json
```

Optional overrides: `--model ... --thinking ... --timeout-s 1800
--revision <sha> --arms a,b`. One arm only for smoke-testing the harness:
`--arms a` (still counts as a real run for that arm — re-run both for the eval).

## After both agents finish (and only then)

```sh
uv run python utils/benchmarks/graph_restart_eval/tests_hidden/verify_arm.py \
  --solution-dir <arm-a worktree>/eval-scratch-a/restart_solution --arm a
uv run python utils/benchmarks/graph_restart_eval/tests_hidden/verify_arm.py \
  --solution-dir <arm-b worktree>/eval-scratch-b/restart_solution --arm b
```

This runs the hidden tests with
`pytest -p aiida.tools.pytest_fixtures --broker-backend zmq -m presto`
(SQLite storage, ZeroMQ broker — no external services).

Never show agents the hidden tests, the other arm's worktree, transcripts, or
metrics.

## Isolation measures (how no context is shared)

- **Separate OS processes**: each arm is a NEW `pi` subprocess
  (`pi --mode json ...` with the full prompt as its first positional message),
  never a worker tool-context/subagent of this session.
- **Separate process groups, no orphans**: each child starts with
  `start_new_session=True`; timeout and token-cap kills signal the ENTIRE
  subprocess group (`killpg`), so tool children cannot outlive `pi`.
- **No inherited agent context**: both commands use
  `--no-extensions -e builtin:codemode --no-mcp` with explicit
  `--tools read,bash,edit,write,codemode --exclude-tools 'mcp__*'`, so no
  extensions, MCP servers, skills-gated tools, or session-continuation state
  leak from the parent into either child.
- **Scrubbed child environment**: every `PI_*` variable (including
  `PI_SESSION_ID`, `PI_SESSION_FILE`, `PI_CODING_AGENT_SESSION_DIR`,
  `PI_PROVIDER`, `PI_MODEL`, `PI_REASONING_LEVEL`) is removed from the child
  environment (`--model`, `--thinking`, `--session-dir`, `--session-id` are
  passed explicitly). Provider credentials are inherited so the child can
  authenticate, and are never written to any log (only the redacted argv is
  recorded; `--api-key` is never used).
- **Separate repository checkouts**: `git worktree add --detach <revision>`
  per arm at the SAME recorded revision; arm cwd is a fresh
  `eval-scratch-<arm>/` dir inside its own worktree.
- **Pre-warmed environments, same caches**: `uv sync` runs once per worktree
  for BOTH arms BEFORE either agent launches (outside the timed solve),
  identically for both arms, so neither arm pays install time and both share
  the same warm `.venv`/package caches.
- **Fresh sessions, no history**: explicit `--session-dir reports/sessions-arm-<arm>`
  (fresh per arm) + `--session-id graph-restart-eval-<arm>-v1`; never
  `--continue/--resume/--fork`, no conversation history carried over.
  Persisted session logs stay under `reports/` (outside both arm cwds and
  outside this worktree's session) alongside the full-stdout JSONL transcript.
- **Prompt-level prohibitions**: each prompt forbids reading the other arm's
  files, `reports/`, `*.jsonl`, `tests_hidden/`/`*verify*` paths, and any git
  history or other worktrees; agents work only inside their scratch.
- **No report JSONL in arm reach**: pre-flight asserts zero `*.jsonl` plus zero
  `*hidden*`/`*verify*` files under each arm cwd before launch; hidden tests
  live only in the git-ignored `tests_hidden/` of THIS worktree and are never
  copied. Reference/current transcripts and run cards also live only in
  `reports/` (untracked and invisible inside the fresh arm worktrees).
- **Frozen identical inputs**: `manifest.json` records SHA-256 of
  `TASK_COMMON.md`, both arm files, `fixtures/flaky_child.py`, and the hidden
  test files before either arm launches. Common tests are frozen (already
  vetted); do not modify them.
- **Identical task except method**: both prompts contain `TASK_COMMON.md` +
  `fixtures/flaky_child.py` verbatim; prompts differ ONLY in the arm file and
  the scratch path.
- **Same budgets**: same `--model gpt-6.1-sol`, `--provider openai-codex`,
  `--thinking medium`, harness `--timeout-s 1800`, and
  `max_total_tokens_each_arm=1000000` for both arms (monotonic wall clock;
  tokens from authoritative `message_end` usage). The token cap is sampled at
  `message_end` boundaries and can overshoot by up to one response before the
  child group is terminated; the cap and the overshoot policy are recorded on
  the run card.

## Reading the results

Per arm, `reports/` holds `run-card-arm-<arm>.json` (method, model, thinking,
revision, `pi --version`, transcript/metrics/prompt/session-file SHA-256,
prewarm timing, harness timing incl. timeout/cap flags), `metrics-arm-<arm>.json`
(token totals, provider/model, stop reason, tool-call counts, wall seconds),
`events-arm-<arm>.jsonl` (full raw stdout JSONL), `stderr-arm-<arm>.log`,
`prompt-arm-<arm>.md`, `sessions-arm-<arm>/` (raw pi
session logs), and `manifest.json` (model/thinking/tools/timeout/token-cap,
revision, frozen input hashes, repo status). Compare correctness
(hidden tests) against time/tokens.

## Disclaimer

n=1 per method: a single paired run cannot establish a general productivity
difference between the APIs. Treat the outcome as one anecdotal data point
(model, thinking level, timeout, and revision are recorded on each run card
for context), not a benchmark claim.
