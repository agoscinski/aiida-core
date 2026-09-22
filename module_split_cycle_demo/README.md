# Option B circular-import demonstration

Yes: this is an architectural problem that Option B reveals. The strict directory boundary makes an invalid dependency from core back to the user API visible. The existing mixed layout permits the same concepts to depend on each other without expressing which dependency direction is intended.

This example assigns every symbol to exactly one side:

| Symbol | Role | Complete Option B location |
|---|---|---|
| `AuthInfo` | User-facing ORM class | `complete_aiida.orm.authinfos` |
| `Entity` | Internal base class | `complete_aiida._core.orm.entities` |
| `OrmModel` | Internal support class | `complete_aiida._core.orm.pydantic` |

There are no public facades for `Entity` or `OrmModel`. They are core API only. Conversely, `AuthInfo` exists only in the user API.

Run all cases from the repository root:

```console
uv run python module_split_cycle_demo/run_demo.py
```

The three examples use distinct package names so all source files can use easy-to-read absolute imports.

## 1. Existing mixed layout

```text
existing_layout/existing_aiida/orm
├── __init__.py       # exports the user-facing AuthInfo
├── authinfos.py      # defines AuthInfo
├── entities.py       # defines internal Entity
└── pydantic.py       # defines internal OrmModel
```

The public and internal symbols are mixed in the same package:

```python
# existing_aiida/orm/authinfos.py
from existing_aiida.orm import entities

class AuthInfo(entities.Entity): ...

# existing_aiida/orm/entities.py
from existing_aiida.orm.pydantic import OrmModel

class Entity(OrmModel): ...
```

Importing the user API succeeds:

```text
existing_aiida.orm
  -> authinfos.AuthInfo
  -> entities.Entity
  -> pydantic.OrmModel
```

## 2. Incomplete Option B split

Only `Entity` is classified and moved to core. Its internal dependency `OrmModel` is accidentally left in the user package:

```text
partial_option_b/partial_aiida
├── _core/orm
│   └── entities.py       # Entity; imports user orm.pydantic
└── orm
    ├── __init__.py       # eagerly imports AuthInfo
    ├── authinfos.py      # AuthInfo; imports core Entity
    └── pydantic.py       # OrmModel is still misplaced here
```

The relevant absolute imports are:

```python
# partial_aiida/orm/authinfos.py: intended user -> core dependency
from partial_aiida._core.orm.entities import Entity

# partial_aiida/_core/orm/entities.py: invalid core -> user dependency
from partial_aiida.orm.pydantic import OrmModel
```

A cold core import follows this cycle:

```text
partial_aiida._core.orm.entities.Entity
  -> partial_aiida.orm.pydantic.OrmModel
  -> initialize partial_aiida.orm.__init__
  -> partial_aiida.orm.authinfos.AuthInfo
  -> partial_aiida._core.orm.entities.Entity (partially initialized)
```

Python raises:

```text
ImportError: cannot import name 'Entity' from partially initialized module
'partial_aiida._core.orm.entities'
```

No compatibility facade is involved. The cycle comes directly from contradictory dependency directions:

```text
user AuthInfo -> core Entity
core Entity -> user OrmModel
```

The first direction is intended; Option B reveals the second as a boundary violation.

### Why importing `AuthInfo` first succeeds

The failure is import-order dependent. Python places a module in `sys.modules` *before* executing its body. When the user API is imported first, `partial_aiida.orm` is therefore already registered as an initializing module:

```text
import partial_aiida.orm.AuthInfo
  -> register partial_aiida.orm in sys.modules
  -> execute orm.__init__
  -> execute orm.authinfos
  -> execute _core.orm.entities
  -> request orm.pydantic
  -> orm is already initializing, so do not execute orm.__init__ again
  -> execute orm.pydantic and define OrmModel
  -> define Entity
  -> define AuthInfo
```

Nothing asks for `Entity` until after it has been defined, so this order succeeds.

When `Entity` is imported first, public `orm` has not been registered yet. Importing `orm.pydantic` must initialize `orm`, whose `AuthInfo` immediately asks for `Entity` from the still-executing core module:

```text
import partial_aiida._core.orm.entities.Entity
  -> register core entities, but Entity is not defined yet
  -> request orm.pydantic
  -> initialize orm
  -> initialize authinfos
  -> request Entity from the registered but incomplete core entities module
  -> ImportError
```

The dependency structure is the same in both cases; only one order happens to work. Since `_core` is intended to be an independently importable core API, this order dependence is an architectural defect rather than a safe cycle.

## 3. Complete Option B split

Classifying `OrmModel` as internal and moving it with `Entity` removes the reverse dependency:

```text
complete_option_b/complete_aiida
├── _core/orm
│   ├── entities.py       # Entity
│   └── pydantic.py       # OrmModel
└── orm
    ├── __init__.py       # exports AuthInfo
    └── authinfos.py      # AuthInfo; imports core Entity
```

```python
# complete_aiida/orm/authinfos.py
from complete_aiida._core.orm.entities import Entity

# complete_aiida/_core/orm/entities.py
from complete_aiida._core.orm.pydantic import OrmModel
```

The dependency graph is now one-way:

```text
user AuthInfo -> core Entity -> core OrmModel
```

Both the user API and core API can be imported in a fresh interpreter.

## Lesson

Option B exposes dependencies that point in the wrong architectural direction. It works when every symbol is classified and the moved modules form a dependency-closed core:

```text
user API -> core API       allowed
core API -> user API       forbidden
```

The broken example is useful because the failure identifies an internal symbol that was classified incorrectly or has not yet been migrated.
