# AiiDA monorepo

`aiida-core/` owns the workflow engine, storage, and shared interfaces.
`aiida-atomistic/` depends on core and owns the atomistic data types and tools.
The split preserves plugin entry-point names so existing provenance can still
be loaded when the companion package is installed. Core exposes lazy public
imports for the moved types; it does not install the companion by default.

## Replay the migration

Use a clean worktree at the pre-migration revision with `pixi.toml` and the two
migration scripts in `utils/`. Omit `--execute` to preview a phase.

```sh
uv run --no-project python utils/migrate_to_monorepo.py --phase move --execute
# Commit the staged renames before running any rewrites.
uv run --no-project python utils/migrate_to_monorepo.py --phase rewrite --execute
# Stage and commit the core layout changes.

uv run --no-project python utils/extract_atomistic.py --phase move --execute
# Commit the staged renames before scaffolding or rewriting.
uv run --no-project python utils/extract_atomistic.py --phase scaffold --execute
uv run --no-project python utils/extract_atomistic.py --phase rewrite --execute
# Stage and commit the atomistic package and its integration changes.
```

Before committing a move, `git diff --cached --name-status -M100%` should show
only `R100` entries. If the large-file hook flags byte-identical moved files,
bypass it for that move commit only and run checks after the rewrite. The
rewrite phase regenerates lockfiles when the package index is reachable;
verify them with `uv lock --project aiida-core --check` and
`uv lock --project aiida-atomistic --check`.
