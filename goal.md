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

- **Stored provenance:** node type strings / `__module__`-derived plugin strings in
  existing databases reference old paths (`aiida.orm.nodes…`) and will not resolve.
  Accepted per decision 3/4, but the failure mode must be confirmed, not assumed.
- **Plugins:** any external code importing deep paths breaks without deprecation — intended.
- **Docs build:** Sphinx pages referencing moved modules are not covered by presto;
  build docs separately or list as follow-up.

## Open questions (need answers before/in parallel with step 2)

1. Package name is `transports` (plural) — confirm pilot means `aiida.transports`.
2. Should `tests/` import sites be rewritten to `_core` paths (recommended, to hit the 90%
   target) or left to fail as "internal users get what they get"?
3. Is a short migration note (changelog + docs warning) wanted given accepted DB breakage,
   or is silent breakage acceptable in this branch?
