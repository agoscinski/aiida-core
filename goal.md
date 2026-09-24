## Goal

Enforce the public API rule **structurally** for the pilot packages `common`, `orm`, `transports`:

> Any resource importable directly from the top level (`aiida`) or a second-level
> package (`aiida.orm`, …) is public. Anything deeper is internal.

Implementation code that today lives at depth ≥ 3 moves to `aiida._core.<pkg>/…`,
preserving the relative nesting. After the move, old deep import paths stop working
(no compatibility shims).

```python
from aiida import load_profile                      # OK, top-level import
from aiida.orm import QueryBuilder                   # OK, second-level import
from aiida.orm.nodes.data.structure import StructureData  # BREAKS (moved to aiida._core.orm.nodes…)
from aiida.manage.configuration.config import Config      # unchanged, out of pilot scope
```

Target layout (pilot packages only; everything else untouched):

```
aiida
├── _core                      # NEW, private implementation (underscore = internal)
│   ├── common                 # moved, nesting preserved
│   │   ├── hashing.py         # (was aiida/common/hashing.py)
│   │   └── …
│   ├── orm
│   │   ├── autogroup.py       # (was aiida/orm/autogroup.py)
│   │   ├── convert.py
│   │   ├── implementation/    # wholesale
│   │   ├── nodes/             # wholesale, e.g. nodes/data/structure.py
│   │   └── utils/             # wholesale
│   └── transports
│       ├── cli.py
│       ├── util.py
│       └── plugins/           # wholesale
├── common
│   ├── __init__.py            # public facade, re-exports __all__
│   ├── links.py               # STAYS (public flat module)
│   └── … (other public flat modules stay)
├── orm
│   ├── __init__.py
│   ├── querybuilder.py        # STAYS
│   └── … (other public flat modules stay)
└── transports
    ├── __init__.py
    └── transport.py           # STAYS
```

## Agreed decisions

1. **No shims.** Old deep paths are deleted, not aliased. External deep imports break loudly (`ModuleNotFoundError`).
2. **Nesting preserved.** `aiida/orm/nodes/data/structure.py` → `aiida/_core/orm/nodes/data/structure.py` (pure `git mv` + import rewrite).
3. **Breakage accepted**, including entry-point paths and stored node type strings derived from `__module__` (existing DBs with old type strings will not resolve — accepted, see risks).
4. **Pilot scope: `common`, `orm`, `transports` only.** No other package is touched.
5. **Success criterion:** `pytest -m presto` passes at ~90% (remainder = known sandbox-environment issues, to be itemised against the pre-move baseline).

**Result: 3149 passed / 323 failed = 90.7% ✓** (3 memory-leak tests excluded — they hang
identically on the pristine tree). Every failure category was reproduced byte-identically
on the pristine tree: ssh-transport tests (no ssh server), `test_get_size_on_disk` + file-upload
tests (sandbox FS), `/usr/bin/bash` vs homebrew paths, login-shell test. Verified via
per-chunk runs plus targeted identical selections (including identical durations).

## Stay-vs-move rule

A file/subpackage **stays** iff it is part of the public namespace, i.e. star-imported
by the second-level `__init__.py` (`from .x import *`, contributing to `__all__`).
Everything else under the three packages **moves** to `aiida._core/<pkg>/`, keeping
its relative path. The second-level `__init__.py` files stay and become facades that
re-export from both remaining siblings (`.x`) and `aiida._core.…`.

Per-package lists (derived from current `__init__.py` star imports):

| Package | Stays (public flat modules) | Moves to `aiida._core/<pkg>/` |
|---|---|---|
| `common` | `datastructures`, `exceptions`, `extendeddicts`, `links`, `log`, `progress_reporter`, `utils` | `constants`, `docs`, `escaping`, `files`, `folders`, `hashing`, `lang`, `pydantic`, `timezone`, `typing`, `warnings` |
| `orm` | `authinfos`, `comments`, `computers`, `entities`, `extras`, `fields`, `groups`, `logs`, `pydantic`, `querybuilder`, `users` (+ `__init__`) | files: `autogroup`, `convert`; subpackages wholesale: `implementation/`, `nodes/`, `utils/` |
| `transports` | `transport` (+ `__init__`) | files: `cli`, `util`; subpackage wholesale: `plugins/` |

Note: `orm/nodes`, `orm/utils`, `transports/plugins` are star-imported *and* nested —
they move wholesale and the public names are re-exported via `aiida._core.…` imports
in the second-level `__init__.py`.

## Execution steps

Do one package per commit, in this order: `common` → `transports` → `orm`
(simplest first; `orm` last because entry points and type strings make it riskiest).

Per package:

1. **Baseline.** Record `pytest -m presto` result on a clean tree before touching anything.
2. **`git mv`** the move-list to `aiida/_core/<pkg>/…` (create `aiida/_core/__init__.py` and
   `aiida/_core/<pkg>/__init__.py` first). No code changes in this step.
3. **Rewrite imports repo-wide** (`src/`, `tests/`, `utils/`):
   - moved paths gain the `_core` segment, e.g. `aiida.orm.nodes` → `aiida._core.orm.nodes`,
     `aiida.common.hashing` → `aiida._core.common.hashing`,
     `aiida.transports.plugins` → `aiida._core.transports.plugins`;
   - public `__init__.py` facades import from `aiida._core.…` where the implementation moved.
4. **Tooling: `utils/autogenerate_all_imports.py`.** It currently rejects `__all__` in
   underscore packages and generates sibling-relative (`from .x import *`) imports only.
   Extend it to generate the `_core`-aware facades (or document the manual pattern), then
   regenerate and verify `__all__` is unchanged for all three packages.
5. **Entry points (`pyproject.toml`).** Rewrite paths pointing at moved modules
   (e.g. `aiida.orm.nodes.data…:…` → `aiida._core.orm.nodes.data…:…`,
   `aiida.transports.plugins…` → `aiida._core.transports.plugins…`).
6. **Docs.** Update `docs/source/reference/api/public.rst` and any Sphinx references to
   moved deep paths.
7. **Verify.**
   - Public surface unchanged: every name in the three `__all__`s still imports from
     top/second level; snapshot-diff `__all__` before/after.
   - Old deep paths fail: `from aiida.orm.nodes… import …` raises `ModuleNotFoundError`.
   - `uv run pre-commit` clean (ruff, mypy) — absolute imports of `_core` must satisfy lint rules.
   - `pytest -m presto` ≈ 90%; diff failures against the step-1 baseline and itemise
     (expected: only pre-existing sandbox issues).

## Known risks (accepted, must be itemised in the final report)

- **Deep imports break (intended).** Old paths like `aiida.orm.nodes…` raise
  `ModuleNotFoundError`. No shims were left behind.
- **Stored provenance survives.** Verified: node type strings are decoupled from module
  paths (entry-point names + prefix stripping). After updating `pyproject.toml` entry
  points, `ENTRY_POINT_GROUP_TO_MODULE_PATH_MAP` and the `prefixes` tuple in
  `orm/utils/node.py` in sync, `get_type_string_from_class` returns byte-identical
  strings (e.g. `data.core.structure.StructureData.`). Old databases keep loading.
  Pickled checkpoints referencing old `__module__` paths are the remaining casualty.
- **Plugins:** any external code importing deep paths breaks without deprecation — intended.
- **Docs build:** Sphinx pages referencing moved modules are not covered by presto;
  `.rst` references were deliberately left stale (reverted mechanical rewrite — user-facing
  snippets must not be rewritten to `_core` paths). Docs pass is follow-up.

## Open questions (need answers before/in parallel with step 2)

1. Package name is `transports` (plural) — confirm pilot means `aiida.transports`.
2. Should `tests/` import sites be rewritten to `_core` paths (recommended, to hit the 90%
   target) or left to fail as "internal users get what they get"?
3. Is a short migration note (changelog + docs warning) wanted given accepted DB breakage,
   or is silent breakage acceptable in this branch?

## Implementation notes (learned while doing it)

- **Facade import order matters.** The second-level `__init__.py` must list absolute
  `_core` imports before relative ones (isort/ruff `I001` enforces this). This only works
  if `_core` modules never name-bind from a partially initialized facade: all
  `from aiida.orm import <Name>` in moved code were converted to module-path imports
  (`from aiida.orm.computers import Computer`, …). `utils/autogenerate_all_imports.py`
  was extended (`CORE_MOVED` map) to generate this layout.
- **Config touchpoints beyond code:** mypy `exclude` paths in `.pre-commit-config.yaml` +
  `aiida._core.common.*` added to the strict override in `pyproject.toml`; pytest
  `filterwarnings` entries for `AiidaDeprecationWarning` rewritten (class moved).
- **Test sandbox caveats:** suite must run with an isolated `AIIDA_PATH` (the dev machine's
  real `~/.aiida/config.json` is config-version 11, incompatible with this v2.9.0 tree).
  `tests/transports/test_all_plugins.py` (ssh variants) and `test_remote.py::test_get_size_on_disk`
  fail identically on the pristine tree — pre-existing environment failures, counted toward
  the ~10% allowance.
