# Separating user and core APIs

Every class, function, and variable is assigned to exactly one API. User code imports from the regular `aiida` tree; AiiDA's implementation imports core-only symbols from the internal tree. This is a classification of symbols, not a public facade over every core symbol.

## Option B: separate `aiida._core` tree

```text
aiida
├── _core
│   ├── common
│   │   └── traversal_rules.py
│   ├── engine
│   └── orm
│       ├── entities.py
│       └── pydantic.py
├── common
│   └── links.py
├── engine
└── orm
    └── authinfos.py
```

```python
# User API
from aiida.common.links import LinkType
from aiida.orm.authinfos import AuthInfo

# Core API
from aiida._core.common.traversal_rules import GraphTraversalRules
from aiida._core.orm.entities import Entity
from aiida._core.orm.pydantic import OrmModel
```

The two package branches make the dependency rule visible and enforceable:

```text
user API -> core API       allowed
core API -> user API       forbidden
```

## Option C: package-local private modules

```text
aiida
├── common
│   ├── _traversal_rules.py
│   └── links.py
├── engine
└── orm
    ├── _entities.py
    ├── _pydantic.py
    └── authinfos.py
```

```python
# User API
from aiida.common.links import LinkType
from aiida.orm.authinfos import AuthInfo

# Core API
from aiida.common._traversal_rules import GraphTraversalRules
from aiida.orm._entities import Entity
from aiida.orm._pydantic import OrmModel
```

This keeps related code local and follows existing repository conventions such as `aiida.common.datastructures._calcjob`. The boundary is weaker, however, because user and core modules share the same parent package.

## Exploration in this repository

I prototyped the split around `AuthInfo`, `Entity`, and `OrmModel`. The standalone reproducer is in `module_split_cycle_demo` and can be run with:

```console
uv run python module_split_cycle_demo/run_demo.py
```

### What Option B reveals

Consider an incomplete split in which `Entity` is moved to core but its dependency `OrmModel` remains in the user tree:

```python
# User API: intended direction
from aiida._core.orm.entities import Entity

# Core API: forbidden reverse direction
from aiida.orm.pydantic import OrmModel
```

A cold import of `Entity` follows this path:

```text
aiida._core.orm.entities
  -> aiida.orm.pydantic
  -> initialize aiida.orm.__init__
  -> aiida.orm.authinfos
  -> aiida._core.orm.entities (partially initialized)
```

The failure does not mean that separating the packages created an arbitrary architectural problem. It reveals that `OrmModel` was classified incorrectly or that its migration is incomplete. Since `Entity` requires `OrmModel` at import time, both belong to core:

```text
user AuthInfo -> core Entity -> core OrmModel
```

The failure is import-order dependent. Importing `AuthInfo` first can succeed because Python registers `aiida.orm` in `sys.modules` before executing its initializer. A later import of `aiida.orm.pydantic` does not execute that initializer again. Importing core `Entity` first fails because `aiida.orm` has not yet been initialized and its eager initializer loops back to the incomplete core module. A core API that only works after importing the user API is not independently usable and remains architecturally cyclic.

### Why Option C does not reveal the same problem

Importing `aiida.orm._entities` always initializes its parent package, `aiida.orm`, first. The eager package initializer therefore establishes the import order that happens to work:

```text
initialize aiida.orm
  -> authinfos
  -> _entities
  -> pydantic
```

Option C has not necessarily removed the reverse dependency. Python's parent-package initialization order merely masks it. Since both modules are siblings under `aiida.orm`, an import from a private module into a user module also looks like an ordinary local dependency and is harder to prohibit mechanically.

### Import surface

Neither directory layout reduces eager imports by itself. That requires separate work on aggregate package initializers such as `aiida.orm.__init__` and imports of the form `from aiida.orm import ...`. Lightweight initializers would reduce runtime import-order failures, but they would not remove the need for a one-way architectural dependency rule.

## Decision

**Choose Option B: a separate `aiida._core` tree.**

The goal is to expose and eliminate circular architectural dependencies, not merely to preserve an import order that happens to work. Option B provides a clear boundary on which automated checks can operate. Its migration must follow dependency closures rather than moving isolated files.

Migration requirements:

1. Classify each symbol as either user API or core API.
2. Move a core symbol together with the lower-level symbols it requires at import time.
3. Keep `_core` package initializers lightweight.
4. Prohibit imports from `aiida._core` into the user-facing `aiida.common`, `aiida.engine`, and `aiida.orm` trees.
5. Test each supported `_core` entry point in a fresh interpreter so import-order dependencies cannot remain hidden.
6. Treat any required compatibility exports as an explicit, temporary migration concern rather than making every core symbol part of both APIs.
