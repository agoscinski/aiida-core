# AiiDA monorepo

This repository hosts multiple Python subpackages. Each subpackage lives in its
own directory with its own `pyproject.toml`, dependencies, tests, docs, changelog
and version. Developer tooling (CI, linting, environments, helper scripts) lives
at the repository root and is shared.

```
repo/
├── aiida-core/          # the workflow manager (first subpackage)
│   ├── src/aiida/
│   ├── tests/
│   ├── docs/
│   ├── pyproject.toml   # deps, entry points, pytest/cov/mypy/flit config
│   ├── uv.lock
│   └── ...
├── aiida-foo/           # future subpackage (same shape)
├── pixi.toml            # shared dev environment
├── ruff.toml            # shared style (moved verbatim from package)
├── mypy.toml            # shared type rules (referenced explicitly)
├── uv.toml              # shared uv floor (auto-discovered)
├── mypy.toml            # shared type-checking config (planned, see below)
├── .pre-commit-config.yaml
├── .github/             # shared actions + reusable workflows, per-package CI
└── utils/               # shared helper scripts
```

## Principles

- **Entry point names are the API contract.** Node type strings stored in
  databases derive from entry point group + name, not module paths
  (`aiida/orm/utils/node.py: get_type_string_from_class`). Moving a class to a
  new distribution is provenance-safe as long as entry point names and class
  names stay identical (and both distributions are installed when loading).
- **Share everything mechanical; scope everything release-related.** Lint
  rules, CI templates, environments and helper logic are shared. Dependencies,
  versions, changelogs, locks and PyPI releases are per package.
- **Core never hard-imports subpackages.** Downstream code (`aiida.orm`, CLI,
  REST) reaches moved types through entry points, never module paths.
  A module-level `__getattr__` with an "install aiida-…" error is the
  friendly pattern where a name used to live.

## Shared as-is (single source of truth at root)

| File | Shared via |
|---|---|
| `[tool.ruff]` + subsections | root `ruff.toml` moved verbatim from the package (auto-discovered upward; hooks additionally pinned `--config`); `exclude` paths re-prefixed per package on move; `[lint.isort] known-first-party` keeps `aiida` import grouping without a visible `src/` layout |
| `[tool.mypy]` strictness + `[[overrides]]` strict lists | root `mypy.toml`, hook passes `--config-file=mypy.toml` (mypy accepts any path but does not auto-discover it; overrides are module-keyed so all packages graduate in one file) |
| `[tool.uv] required-version` | root `uv.toml` (verified: uv discovers parent dirs even for `--project` runs, so gating still applies) |
| Publishing (`pypa/gh-action-pypi-publish`, OIDC, no secrets) | reused verbatim |
| Pytest marker vocabulary (`nightly`, `requires_*`, `presto`, …) | shared names so `-m` selections compose across packages |
| `install-package` action, `reusable-pytest.yml` workflow | `package:`/`test-path:` inputs |
| Shipped pytest fixtures (`aiida.tools.pytest_fixtures`) | installed with `aiida-core`, imported by downstream test suites |
| CI services/images, `setup.sh`, pixi env, issue templates, `utils/` scripts | used directly (utils take a package dir where needed) |

## Copy once per package (tiny, stable templates)

- `[tool.pytest.ini_options]` markers + `filterwarnings` (no extends mechanism; `testpaths` per package)
- `[tool.coverage.run]` flags (adjust omit paths), `[tool.flit.sdist] exclude` pattern
- `[tool.flit.module]` (1 line), `build-system` (2 lines), `[tool.uv] required-version` (1 line)
- `requires-python` + classifiers (single support window), dynamic version from `__init__.py`, authors/urls/license values, `dev` dependency-group shape
- Release flow: `release.yml` + `check_release_tag.py` + `patch-release.sh`, parameterized by package dir

## Must stay per-package

- Dependencies, entry-point registrations, `uv.lock`, `environment.yml`, docs, tests, changelog, version
- **PyPI trusted-publisher registration** (one-time per package name) and **tag scheme** (`v*` must become per-package prefixes like `atomistic-v*`, otherwise one tag releases everything)
- **Entry-point group definitions** — the cross-package API contract, owned by core

## Adding a package checklist

1. Create `<pkg>/` with `pyproject.toml` (copy the template sections above), `src/`, `tests/`.
2. `uv lock --project <pkg>`; add marker/filterwarning copies if the suite needs them.
3. Keep every moved entry point **name** and **class name** identical (provenance rule above).
4. Remove hard imports of the moved modules from `aiida-core` (lazy `__getattr__` shims with install hints where names used to live).
5. Add a thin caller workflow (`paths: ['<pkg>/**']`) over `reusable-pytest.yml`; register PyPI trusted publishing and a tag prefix.
6. Extend `CODEOWNERS` and the `utils/` package-dir handling to the new path.
