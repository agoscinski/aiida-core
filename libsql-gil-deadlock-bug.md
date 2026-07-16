# libsql_experimental: GIL-vs-database-lock deadlock under multi-threaded writers

**Package:** `libsql-experimental` 0.0.55 (Python binding, [tursodatabase/libsql-experimental-python](https://github.com/tursodatabase/libsql-experimental-python))
**Symptom:** spurious `ValueError: database is locked` when multiple threads in one process write
to the same local database file — each thread using its **own** connection, with busy timeouts
configured. The same workload across multiple *processes* works fine, and the identical test with
Python's builtin `sqlite3` passes with zero errors and ~40× the throughput.

Found while prototyping a Turso storage backend for AiiDA: the AiiDA daemon workers are
multi-threaded (asyncio event loop + kiwipy communicator thread), and crashed with
`database is locked` within a minute despite `PRAGMA busy_timeout=60000` — while an external
observer process confirmed the write lock was not actually held.

## Minimal reproduction

```python
import tempfile, threading, time
from pathlib import Path
import libsql_experimental as libsql

db = str(Path(tempfile.mkdtemp()) / 'db.sqlite')

setup = libsql.connect(db)
setup.execute('PRAGMA journal_mode=WAL')
setup.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)')
setup.commit()
del setup

def worker(idx):
    conn = libsql.connect(db)              # own connection per thread
    conn.execute('PRAGMA busy_timeout=10000')
    deadline = time.time() + 15
    n = 0
    while time.time() < deadline:
        conn.execute("INSERT INTO t (v) VALUES (?)", (f'w{idx}',))
        conn.commit()
        n += 1
    print(f'thread {idx}: {n} inserts')

threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
for t in threads: t.start()
for t in threads: t.join()
```

Observed (Python 3.11, macOS arm64): two of four threads die with `ValueError: database is
locked` within ~15 s; per-thread insert counts are wildly skewed (233/162/58/21). The identical
script using `sqlite3.connect(db, timeout=10)` completes with zero errors and ~89,000 total
inserts (vs ~474).

## Root cause

`Connection.execute`, `Cursor.execute`, `commit`, `rollback`, and the fetch methods all run the
blocking database call as

```rust
rt().block_on(async { ... })          // src/lib.rs
```

inside a `#[pymethods]` function — **while holding the Python GIL**. The builtin `sqlite3` module
releases the GIL around `sqlite3_step` (`Py_BEGIN_ALLOW_THREADS`); this binding never releases it.

That makes SQLite's busy-wait machinery deadlock with the GIL:

1. Thread B executes an `INSERT` (implicit `BEGIN` + insert → acquires the write lock), returns
   to Python, and yields the GIL before reaching its `conn.commit()` line (CPython switches
   threads every ~5 ms).
2. Thread A grabs the GIL and starts its own `INSERT`. The write lock is held by B, so A enters
   the busy-wait retry loop — **still holding the GIL**, because `block_on` never releases it.
3. B can never execute `COMMIT` (it needs the GIL that A holds). A can never acquire the write
   lock (B holds it). A burns its entire busy timeout and fails with `database is locked`.
4. Only when A gives up does B get the GIL back and commit — so the lock holder was never stuck,
   which is why the error is spurious.

## Evidence

Timing instrumentation of the repro with a 2 s busy timeout shows the deadlock signature exactly:

```
t+ 0.03s thread 1: SLOW-OK               took 4.19s   (innocent commit stalled behind the waits)
t+ 0.03s thread 0: FAIL(database is locked) took 2.09s
t+ 2.12s thread 2: FAIL(database is locked) took 2.10s
t+ 4.33s thread 3: FAIL(database is locked) took 2.08s
```

- Every failure takes **exactly the busy timeout** (2.08–2.10 s).
- Failures are perfectly **serialized back-to-back**: each waiter blocks the whole process for
  its full timeout (it holds the GIL), then the next thread begins its own doomed wait.

Consistency checks ruling out other causes:

- Cross-process WAL busy-wait works correctly: a writer blocked by a foreign process's 2 s
  transaction succeeds after 2.03 s. (No GIL is shared between processes.)
- Single-threaded multi-connection and 8-process concurrent-writer stress tests all pass.
- `PRAGMA busy_timeout` is honored by the driver (readback confirms; waits match the setting).
- Rollback correctly releases locks across processes.

## Proposed upstream fix

Release the GIL around every blocking database call, mirroring CPython's `sqlite3` module. In
PyO3 terms: `py.allow_threads(|| rt().block_on(...))`.

This requires restructuring `execute()` (src/lib.rs) so that no Python objects are touched inside
the unlocked region:

1. While holding the GIL: convert the Python parameters to `libsql::Value`s, clone the
   `Arc<ConnectionGuard>`, and compute the `stmt_is_dml` / autocommit / needs-`BEGIN` flags.
2. Inside `py.allow_threads`: run the `BEGIN` (if needed), `prepare`, and `query`/`execute`
   futures via `rt().block_on` on the cloned connection, returning the `Statement`/`Rows` and
   change count.
3. While holding the GIL again: store the results into the cursor's `RefCell`s.

The same treatment applies to `commit`, `rollback`, `executescript`, and the fetch methods (for
fetches, `take()` the `Rows` out of the `RefCell`, iterate inside `allow_threads`, convert rows
to Python objects after re-acquiring the GIL).

Note the crate already asserts cross-thread usability (`unsafe impl Send/Sync for Connection`,
and the SQLAlchemy dialect `sqlalchemy-libsql` passes `check_same_thread=False` for file
databases), so releasing the GIL is consistent with the advertised threading contract — today
that contract is what turns the missing GIL release into a deadlock.

## Workaround until fixed

None found at the configuration level. Busy timeouts (any value), WAL mode, `NullPool` (fresh
connection per checkout), and pragma ordering were all tried — the deadlock is structural. The
only safe modes with the current driver are: single-threaded processes, one process per
connection, or serializing all database access through a single thread. For AiiDA's daemon this
means the embedded libSQL engine is unusable; the plain pysqlite driver handles the identical
workload without issues.
