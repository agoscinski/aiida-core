# Plan: Archive-as-profile vertical slice

Status: implemented — slice validated on `chore/archive-as-profile-v1` (see `## Results` below).
Author: discussion on `setup-v3-migration-v4` / migrator composition.

## Hypothesis

`SqliteZipBackend` is registered as `core.sqlite_zip` storage yet only half-usable via
`verdi storage`: `version_head/version_profile/initialise` work, `migrate(profile)` raises
(`use migrate() directly`). If archives are legitimately read-only profiles, the profile-shaped
path (`verdi storage migrate -p <archive-profile>`) should work by delegating to the existing
file pipeline — without changing `verdi archive migrate` or the migrator constructors.

## Goals

1. Establish whether the archive/profile split is accidental or load-bearing.
2. One thin, shippable behavior change to test it: `SqliteZipBackend.migrate(profile)` migrates
   the archive file in place to head.
3. Keep both CLIs working: `verdi storage migrate` (profile, to head) and
   `verdi archive migrate in out [--version]` (file, version-targeted).
4. Record what a deeper unification would actually require (if anything).

## Non-goals

- No `BaseMigrator`/`BaseStorageBackend` extraction in this slice.
- No ORM delegate extraction.
- No CLI unification (`verdi storage` vs `verdi archive`).
- No change to migrator constructors (`SqliteZipMigrator(database_path)` stays file-bound,
  `SqliteZipBackend(profile)` stays profile-bound).
- No temp-profile concept, no live-backend migration.

## Current state (reference)

- Entry point exists: `core.sqlite_zip = SqliteZipBackend` (`pyproject.toml`), so archive profiles
  can be created and queried read-only.
- Dispatch paths:
  - `verdi storage migrate` → `profile.storage_cls.migrate(profile)` (`cmd_storage.py:112`).
    psql/sqlite_dos go via `cls.migrator(profile).migrate()` to head.
    `SqliteZipBackend.migrate` raises `NotImplementedError`.
  - `verdi archive migrate in out` → `get_format().migrate(...)` →
    `ArchiveFormatSqlZip.migrate` → file `sqlite_zip.migrator.migrate(inpath, outpath, version)`.
- `SqliteZipBackend.initialise(profile)` already auto-migrates a behind archive in place
  (temp dir + move); `migrate(profile)` does not expose the same operation explicitly.

## Slice scope

Implement `SqliteZipBackend.migrate(profile) -> None`:

- Resolve `filepath` from `profile.storage_config`.
- `current = get_current_archive_version(filepath)`, `target = cls.version_head()`.
- `validate_archive_versions(current, target)`; no-op if equal.
- Else `migrate(filepath, tmp_path, target)` + atomic move (mirror `initialise()` logic).
- Raise `StorageMigrationError`/`CorruptStorage` as the file pipeline does; no new exception types.

Explicitly out: legacy `0.x` re-enablement, compression CLI flags on the profile path,
daemon-running guards beyond what `verdi storage migrate` already does.

## Steps (ordered)

1. Read `SqliteZipBackend.initialise` in-place migration block and extract a private
   `_migrate_archive_in_place(filepath, target)` helper used by both `initialise` and `migrate`.
2. Implement `migrate(profile)` via that helper (to head only).
3. Add tests next to `tests/storage/sqlite_zip/migrations/`:
   - behind-archive profile migrates to head and validates;
   - already-at-head is a no-op (file bytes unchanged, cf. `test_migrate_no_migration_needed`);
   - corrupt/missing version raises `CorruptStorage`.
4. Manually verify `verdi storage version` + `verdi storage migrate` against a scratch archive
   profile, and confirm `verdi archive migrate` is untouched.
5. Write down the decision: does this remove real friction, or does it expose the next inconsistency
   (e.g. daemon guards, `maintain`, read-only `bulk_*`, import-then-migrate flow)?

## Test implications

- Reuse existing fixtures/helpers in `tests/storage/sqlite_zip/migrations/`; no new harness.
- Keep file-pipeline tests as the engine coverage; profile tests assert delegation only.
- Mark nothing psql-specific; sqlite-only, no RMQ.

## Risks / open questions

- In-place migration of a user-supplied archive file via a profile command may surprise; `initialise`
  already does it, but `migrate` makes it explicit — confirm messaging/docs.
- `StorageBackend.migrate` contract says no-op if at head, raise if uninitialised — archive "uninitialised"
  (missing/corrupt file) maps to `CorruptStorage`, confirm callers handle it (`cmd_storage.py` catches
  `ConfigurationError/StorageMigrationError` only).
- If this slice reveals `maintain`/`_clear`/import flows also need profile shaping, stop and re-scope
  rather than growing the slice.

## Follow-ups (only if slice validates)

- Shared profile-migrator protocol for psql/sqlite_dos (`BaseDosMigrator`), archive stays out.
- ORM accessor delegate shared by sqlite_dos/sqlite_zip backends.
- Docs note on when to use `verdi storage` vs `verdi archive` for archives.

## Follow-up results (implemented on `chore/archive-as-profile-v1`)

All three follow-ups are done, no behavior change, pre-commit (ruff + mypy) clean,
`tests/storage/sqlite_{zip,dos}/` 31 passed, `tests/tools/archive/` 120 passed,
`tests/cmdline/commands/test_storage.py` 19 passed (psql suites need a live cluster: deselected here, for CI).

- `BaseDosMigrator` (`src/aiida/storage/migrator.py`, next to `AlembicMigrator`, not exported as
  public API): owns engine/connection lifecycle, Alembic delegation, container/UUID checks, and the
  initialise/validate/migrate policy. `PsqlDosMigrator`/`SqliteDosMigrator` keep only their hooks
  (`_create_engine`, `orm_metadata`, `get_container`, `alembic_migrator`) plus psql-only legacy
  handling (`_check_version_table`/`is_database_initialised`/`initialise_database` report/`initialise_repository`
  via `CONTAINER_DEFAULTS`/`_migrate_legacy_branches`). `check_legacy` joined the shared
  `get_schema_version_profile` signature (sqlite ignores it, documented). Archive stays out: file-bound.
  Net effect: -250 lines across both migrators, and `PsqlDosBackend.migrator: type[BaseDosMigrator]`
  let sqlite drop its `# type: ignore[assignment]`.
- `SqliteOrmMixin` (`sqlite_zip/backend.py`, internal): the 9 identical `query`/`get_backend_entity`/
  collection accessors, mixed into `SqliteZipBackend` and `SqliteDosStorage` (mixin-first MRO so the
  sqlite accessors still win over `PsqlDosBackend`'s sqla ones). No public API change.
- Docs: `docs/source/howto/archive_profile.md` gained "Migrate a mounted archive"
  (`verdi -p archive storage migrate`, in place to head, vs `verdi archive migrate in out [--version]`,
  file-to-file and version-targeted).

Two findings worth keeping on record:

- Chesterton's fence: the old `DbSetting` import in `sqlite_dos/backend.py` was load-bearing —
  `sqlite_zip/models.py` copies every table in psql metadata at import time, and that import was what
  registered `db_dbsetting` before the copy loop. Removing it broke `initialise_database` (`no such table`).
  Fixed at the copy site: `settings` added to the "import all models" list in `models.py` (side-effect
  import, commented), which also repairs the latent order-dependency for zip-only imports.
- SQLite `connect()` failures now surface as `UnreachableStorage` (via the shared `connection` property),
  matching the documented migrator contract; previously the raw `OperationalError` propagated.

## Results (slice implemented)

Implemented on `chore/archive-as-profile-v1`: `SqliteZipBackend.migrate(profile)` migrates the
archive file in place to head via the existing file pipeline, sharing one private
`_migrate_archive_in_place(filepath, current, target)` helper with `initialise()`.
No change to `verdi archive migrate`, migrator constructors, or CLI dispatch.

Tests (`tests/storage/sqlite_zip/test_backend.py`, 3 new — full `tests/storage/sqlite_zip/` suite:
20 passed, pre-commit clean):

- behind-archive profile (`export_main_0000_simple.aiida` copied to tmp) migrates to head,
  passes `validate_storage`, and opens as a backend;
- already-at-head `migrate` is a byte-identical no-op;
- corrupt inputs (non-archive file, zip without `export_version`) raise `CorruptStorage`.

Manual verification: scratch copy of the `main_0000` archive migrated `main_0000 -> main_0001`
via `SqliteZipBackend.migrate`, second call a no-op; both `verdi storage migrate` and
`verdi archive migrate` help/dispatch paths confirmed untouched.

Decision: the split was accidental, not load-bearing — `verdi storage migrate -p <archive-profile>`
now works instead of raising `NotImplementedError`, so the slice removes real friction.
One inconsistency surfaced for follow-up: corrupt/missing archives raise
`CorruptStorage`/`UnreachableStorage`, but `cmd_storage.py:storage_migrate` only catches
`ConfigurationError`/`StorageMigrationError` (unlike `storage version`, which maps those to
exit 3/4). Either `migrate()` should map "uninitialised archive" to `StorageMigrationError`,
or the CLI catch should widen — left out of the slice on purpose.
`maintain` (`NotImplementedError`) and `bulk_*/_clear` (`ReadOnlyError`) stay read-only by
design; no re-scope triggered. Follow-ups above are unblocked.
