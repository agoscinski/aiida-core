# CalcJob overhead: findings and actionable items

**Date:** 2026-08-11
**Question:** why does a 1024-calcjob run take ~4-6 minutes when each calcjob only
sleeps 10 s remotely?

## How this was measured

All numbers come from the `bench_pg_async_zmq_*` configuration: psql storage,
zeromq broker, `core.ssh_async` transport, `core.slurm` scheduler against
docker-slurm, 5 MB x 3 output files per calcjob.

| run | config | aiida | total |
|---|---|---|---|
| `results/20260810_220651` | 1024 cj / 8 workers | 2.9.0rc1 (PyPI) | 379.7 s |
| `results/20260810_224920` | 1024 cj / 8 workers | 2.9.0.dev0 (checkout) | 255.8 s |
| `results/20260811_013054` | 1024 cj / 8 workers | 2.9.0.dev0 | 274.8 s |
| `results/20260811_013914` | 32 cj / 1 worker | 2.9.0.dev0 | 36.8 s |
| `results/20260811_022146` | 32 cj / 1 worker, commit stacks | 2.9.0.dev0 | 37.5 s |

Instrumentation added to this repo (all opt-in, inert otherwise):

- `--profile-worker-seconds N` -- py-spy wall-clock profile of a single daemon
  worker while the other workers run untouched (`profile_worker.py`). Workers
  opt into being traced via `PR_SET_PTRACER`, so no root and no system-wide
  `ptrace_scope` change is needed.
- `--slow-callback SECONDS` -- logs asyncio callbacks that block a worker's
  event loop, per worker (`profiling/sitecustomize.py`).
- `--db-stats` -- counts every SQL statement and commit per worker, with
  per-statement-shape counts and timings.
- `--commit-stacks` / `--statement-stack-sample N` -- attributes commits (and
  sampled statements) to the aiida/plumpy call site that issued them.

## Headline

The daemon worker's event loop is **saturated, not waiting**: it sits in `epoll`
0.5% of the time. Of its wall time, ~70% is synchronous SQLAlchemy + psycopg on
the event-loop thread, and only ~11% is asyncssh. SSH transport is not the
bottleneck.

Each calcjob costs **~723 SQL statements and ~116 commits**, and that cost is
**flat with scale** (772 statements / 128 commits per calcjob at 32x1 vs 723 /
116 at 1024x8). There is no concurrency-dependent blowup; the queueing at 1024
jobs is the aggregate statement rate saturating a single-threaded loop.

---

## Actionable items

### 1. Read-only `QueryBuilder` iteration issues a commit -- 34 commits/calcjob

**Evidence:** `iterall` is the second-largest commit source, 33.9 commits per
calcjob. Dominant caller chain: `Process.on_entered` -> `update_outputs` ->
`get_outgoing` -> `get_stored_link_triples` -> `QueryBuilder.iterall` -- a pure
read that writes nothing.

`aiida/storage/psql_dos/orm/querybuilder/main.py:224` wraps iteration in
`self._backend.transaction()`, and that context manager
(`aiida/storage/psql_dos/backend.py:255-272`) always ends in `session.commit()`.
The wrapper exists to stop `ModelWrapper` from committing mid-cursor and
invalidating it -- the guard against commits is itself a commit.

**Action:** use a read-only guard for the iteration path instead of a
committing transaction (e.g. `session.begin_nested()` released without an outer
commit, or a flag that suppresses `ModelWrapper` flushes for the cursor's
lifetime).

**Risk:** low. No writes are involved; the concern is only cursor invalidation,
which is what the guard already addresses.

**Verify:** `--db-stats` on 32x1; expect commits/calcjob to drop by ~34 and the
`SELECT db_dbnode.attributes` count to fall with it.

---

### 2. `Process.on_entered` commits 3-4 times per state transition

**Evidence:** `aiida/engine/processes/process.py:470-478` runs on every plumpy
state transition and triggers, in order:

1. `update_outputs()` -> read query -> commit (item 1)
2. `node.set_process_state(...)` -> `attributes.set` -> `_flush` -> `save` -> commit
3. `_save_checkpoint()` -> attribute write -> commit
4. `set_process_state_change_timestamp(node)` -> `set_global_variable` -> commit

**Action:** wrap the body of `on_entered` in a single transaction so state,
checkpoint and timestamp land in one commit.

**Risk:** medium. Ordering matters -- the existing comment notes that outputs and
state must be updated before the superclass broadcasts state changes, because
parent processes may read the node. Batching must not delay the write past the
broadcast.

**Verify:** commits/calcjob should fall to roughly one per transition
(~30 rather than ~127 with item 1 included).

---

### 3. `set_process_state_change_timestamp` writes a shared row per transition

**Evidence:** 9.2 commits per calcjob via
`aiida/engine/utils.py:295` -> `PsqlDosBackend.set_global_variable`
(`backend.py:413`), updating one global `db_dbsetting` row.

Every transition of every process in every worker writes the *same* row. Beyond
volume, this is a cross-worker serialization point in Postgres.

**Action:** throttle it (write at most every N seconds per worker) or move it
off the hot path. It is a "last activity" marker; sub-second precision is not
needed.

**Risk:** low-medium -- `verdi process list`/status freshness depends on it, so
the throttle interval is a UX decision.

**Verify:** commits/calcjob drops by ~9; check `db_dbsetting` update count in
the statement-shape table.

---

### 4. `ModelWrapper` commits on every single attribute write

**Evidence:** the largest commit source, 35.8 commits/calcjob, via
`_flush_if_stored` (`storage/psql_dos/orm/entities.py:94`) -> `_flush` ->
`save` (`storage/psql_dos/orm/utils.py:119,154`).

Each attribute assignment on a stored node is its own durable transaction, so
nothing batches, and each commit expires the session (see item 5).

**Action:** the structural fix behind items 2 and 3 -- allow callers to group
attribute writes, or defer the flush to an explicit transaction boundary.

**Risk:** high; this is core ORM behaviour that plugin code depends on. Treat
items 1-3 as the practical wins and this as the upstream design discussion.

---

### 5. Session expiry causes ~181 redundant single-column re-SELECTs/calcjob

**Evidence:** statement shapes per calcjob (1024x8 run):

| per calcjob | statement |
|---|---|
| 188 | `SELECT db_dbnode.<all columns> WHERE id = ?` |
| 181 | `SELECT db_dbnode.attributes WHERE id = ?` |
| 52 + 52 | `SAVEPOINT` / `RELEASE SAVEPOINT` |
| 47 | `UPDATE db_dbnode SET mtime, attributes` |
| 28 | `SELECT db_dbcomputer.*` |
| 20 | `SELECT db_dbnode.extras WHERE id = ?` |
| 11 | `SELECT db_dbuser.*` |

`aiida/storage/psql_dos/backend.py:172` builds the session with
`expire_on_commit=True`, so every commit invalidates all loaded ORM objects and
the next attribute access re-reads from Postgres. py-spy confirms the cost:
`_load_expired` is 30.8% of the worker's main thread.

**Action:** *do not* flip `expire_on_commit` as a fix -- with several workers on
one database it would serve stale values. Fix the commit count (items 1-4) and
this traffic collapses on its own.

**Verify:** re-run `--db-stats` after items 1-2; the attribute re-SELECT count
should fall roughly in proportion to the commit count.

---

### 6. Immutable rows are re-SELECTed dozens of times per calcjob

**Evidence:** 28 `SELECT db_dbcomputer.*`, 9 `SELECT db_dbcomputer.metadata`
and 11 `SELECT db_dbuser.*` per calcjob. Neither row changes during a run.

**Action:** cache `Computer` and `User` per worker (or per authinfo) rather than
re-reading them from the DB on each access. Partly a consequence of item 5, so
re-measure after items 1-2 before designing a cache.

**Risk:** low, if scoped to the daemon worker's lifetime.

---

### 7. Move database I/O off the event-loop thread

**Evidence:** psycopg's `wait()` accounted for 12.4 s of *self* time (10.4%) in
a 120 s profile -- the loop blocking on a DB socket. Every such block stalls all
~128 co-scheduled calcjobs on that worker.

**Action:** longer-term. Either run storage calls in a thread executor or move
to psycopg3's async interface. Only worth designing after items 1-4, which
reduce the statement volume that makes this hurt.

---

## Benchmark hygiene (this repo)

### 9. Pin the benchmark to the aiida-core checkout, not PyPI

`pixi.toml` pinned `aiida-core = "==2.9.0rc1"`, so every run before
`20260810_224920` measured the released wheel rather than `aiida-core/`. Any
result compared across those runs must account for that.

### 10. The phase instrumentation is not free

`SleepAndGenerateCalculation` writes `transport_timestamps` extras around every
transport task: ~8 of the 12 extras UPDATEs and part of the 20 extras SELECTs
per calcjob are ours, each triggering its own commit and expiry. It is a few
percent of 723 statements, but the extras rows specifically are largely
self-inflicted. Consider a control run with the wrapper disabled before quoting
per-phase numbers as aiida's cost.

---

## Open questions

- 23% of commits fall outside the top-40 recorded signatures; raising the cap
  would close the attribution gap.
- Where do the 52 SAVEPOINT pairs per calcjob come from? They follow
  `backend.transaction()` but were not separately attributed.
- What is the floor? A transport-only baseline (same 3 x 5 MB put/get over
  `core.ssh_async`, no AiiDA) has not been measured yet, so we do not know how
  much of the 43 s median upload is physically necessary.
