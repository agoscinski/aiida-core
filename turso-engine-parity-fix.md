# Fix: `core.turso_dos` engine parity with `core.sqlite_dos`

## Context

Benchmarking `core.turso_dos` against `core.sqlite_dos` (`run_benchmark_sqlite_and_turso.sh`,
1000 processes, 4 daemon workers, 200 slots) showed turso at **1.01 s/process** vs sqlite at
**0.55 s/process** (`output.log`). This was surprising, since Turso was expected to be at least
on par for concurrent writes.

## Root cause analysis

### 1. The benchmark never actually used Turso/libSQL

Since no `--database-url` was passed, `benchmark_daemon_configs.py` auto-generated a plain
`sqlite:////var/...db` URL. `create_sqlalchemy_engine` only engages the `sqlalchemy-libsql`
dialect for `libsql://` / `sqlite+libsql://` / `ws(s)://` / `http(s)://` URLs, so both runs
used the stock pysqlite driver on a local file. The comparison was effectively
*sqlite vs sqlite with a differently configured engine*.

### 2. Missing connect listeners on the turso engine (fixed here)

`core.sqlite_dos` builds its engine via `aiida.storage.sqlite_zip.utils.create_sqla_engine`,
which attaches three `connect` event listeners that the turso engine was missing:

| Listener | Effect of it missing |
|---|---|
| `PRAGMA case_sensitive_like=ON` | **Performance**: SQLite's LIKE optimization (turning `node_type LIKE 'process.%'` into an index range scan) only applies with case-sensitive LIKE. The `QueryBuilder` emits such prefix-LIKE filters constantly (benchmark wait loop, link traversal, process queries), so every one became a full table scan. |
| `PRAGMA foreign_keys=ON` | **Correctness**: foreign key constraints were silently unenforced. |
| `register_json_contains` | **Correctness**: queries using the `contains` operator would fail with "no such function: json_contains". |

### 3. Run conditions

One run each, back to back, on a fanless M2 Air: the turso run started right after ~9 minutes of
sustained 4-worker load, so thermal throttling / variance plausibly accounts for part of the 2×
gap. Re-run with reversed or interleaved order to size this component.

## Changes

- `src/aiida/storage/turso_dos/utils.py`: `create_sqlalchemy_engine` now attaches the same three
  connect listeners as `create_sqla_engine`. `json_contains` registration is guarded with
  `hasattr(dbapi_connection, 'create_function')`, because remote libSQL DBAPI connections cannot
  register Python-side functions (the `contains` QueryBuilder operator remains unsupported there).
- `src/aiida/storage/turso_dos/backend.py`: `__str__` referenced `self._db_url`, which was never
  assigned and raised `AttributeError`; it now reads `database_url` from the profile storage config.

## Follow-up: actually engaging libSQL (2026-07-15, later)

Even after the engine parity fix, the "turso" benchmark runs still used plain pysqlite, for two
independent reasons that are now fixed:

1. **URL scheme**: `benchmark_daemon_configs.py` auto-generated a `sqlite:///` URL, which routes to
   the stock pysqlite driver. It now generates `sqlite+libsql:///` so the `sqlalchemy-libsql`
   dialect is engaged (and `cleanup_local_turso_storage` accepts both schemes).
2. **Missing dependencies**: the pixi `default` environment installed `aiida-core` without the
   `turso` extra, so `sqlalchemy-libsql` was not even installed (a libsql URL would have raised
   `ImportError`). `pixi.toml` now uses `extras = ["turso"]` for the base feature.

With `sqlalchemy-libsql` 0.2.0 the DBAPI is `libsql_experimental` (the embedded Rust libSQL
engine), for local files as well — so `sqlite+libsql:///path.db` genuinely benchmarks the libSQL
engine without needing a `sqld` server.

Two driver differences surfaced during verification:

- **No busy timeout**: unlike pysqlite (5 s default), `libsql_experimental` fails immediately with
  `database is locked` under concurrent writers — 8 parallel writer processes crashed within
  seconds. `create_sqlalchemy_engine` now attaches a connect listener setting
  `PRAGMA busy_timeout=5000`. Without this, the daemon benchmark would crash.
- **Rollback-journal lock convoys**: even with the busy timeout, the daemon benchmark still died
  with `database is locked` during submission. The libSQL driver defaults to the `DELETE` journal
  mode, where every commit takes an exclusive lock on the whole database; with 8 workers plus the
  submitter committing continuously, lock waits exceeded the 5 s timeout (pysqlite tolerates the
  same workload, so libSQL's lock handling under sustained DELETE-mode contention is simply
  worse). Fix: `PRAGMA journal_mode=WAL` + `PRAGMA synchronous=NORMAL` on connect, applied only to
  local database files (triple-slash URLs) since journal mode must not be pushed to remote/managed
  Turso databases. Validation: 9 processes doing mixed read/insert/read-then-update transactions
  against one libsql file for 45 s completed ~63k transactions (~1400 tx/s) with zero lock errors.
- **No `create_function`**: the libSQL connection cannot register Python functions, so
  `json_contains` (the `contains` operator of the `QueryBuilder`) is unavailable on libsql URLs.
  The registration is skipped gracefully; plain `sqlite://` URLs keep it.

An 8-process concurrent-insert microbenchmark (2000 single-transaction inserts) shows embedded
libSQL performing in the same range as pysqlite (within run-to-run noise), so no concurrency
speedup should be expected from switching the local-file engine either.

## Follow-up 2: embedded libSQL cannot run the daemon — driver thread-safety bug (2026-07-15, evening)

Even with WAL + busy timeout, the daemon benchmark kept failing with `database is locked`
(always on the `db_dbsetting` single-row hotspot updated by `set_process_state_change_timestamp`
on every process state change). Systematic elimination:

1. Raising `busy_timeout` to 60 s did not help — failures ignored the timeout.
2. A lock observer probing the database every 500 ms during the failing benchmark saw the write
   lock **free the entire time** — so no writer was actually holding the lock.
3. Control experiment: the identical benchmark through the same backend with a plain
   `sqlite:///` URL (pysqlite + the same WAL/busy-timeout pragmas) **passes cleanly**.
4. Cross-process WAL busy-wait works correctly at the raw libsql driver level (a writer blocked
   by a 2 s foreign transaction succeeds after 2.03 s).
5. **Minimal reproduction**: one process, four threads, each with its *own* libsql connection
   and a 10 s busy timeout, doing single-row insert+commit loops → two threads crash with
   `database is locked` within 15 s, and throughput is wildly skewed (233/162/58/21 inserts).
   The identical test with pysqlite: zero errors and ~40× the throughput (89k vs 474 inserts).

Conclusion: `libsql_experimental` 0.0.55 is **not thread-safe across connections within one
process** (apparently shared per-process state for the same database file). AiiDA daemon workers
are inherently multi-threaded (asyncio event loop + kiwipy communicator thread), so the embedded
libSQL engine cannot serve the daemon. This is a driver bug, not something engine configuration
can work around (busy timeouts, `NullPool`, pragma ordering were all tried).

Consequences:

- `benchmark_daemon_configs.py` defaults to a `sqlite:///` URL again (pysqlite). An explicit
  libsql `--database-url` can still be passed, e.g. for a remote `sqld` server (untested; the
  Hrana network path avoids local file locking entirely).
- `create_sqlalchemy_engine` keeps `NullPool` for libsql URLs (avoids cross-thread connection
  reuse, which is also unsafe) and the WAL/busy-timeout listeners (which benefit the pysqlite
  path and are correct regardless).
- Re-test when `libsql_experimental` matures or when `sqlalchemy-libsql` switches DBAPI.

## Expectations going forward

Even with a real Turso setup (`sqlite+libsql://` pointing at a `sqld` server), a speedup over
local SQLite is unlikely for this workload: writes are still serialized server-side, every
statement pays a network round trip, and AiiDA's ORM issues many small transactions per process
checkpoint. In this benchmark the bottleneck is process/transport/broker overhead, not database
write-lock contention.
