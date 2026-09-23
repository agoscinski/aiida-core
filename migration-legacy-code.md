# Legacy `Code` migration — high-level summary

- The deprecated, storable `Code` plugin and its `core.code` entry point are removed. The former `AbstractCode` is now the single abstract, non-storable `Code` base class; `InstalledCode` and `PortableCode` inherit from it without the legacy methods.
- Older databases and archives can contain nodes stored using the former concrete `Code` plugin (`data.core.code.Code.`). Migration `main_0003` converts them: `is_local=False` becomes `InstalledCode` (`remote_exec_path` → `filepath_executable`), while `is_local=True` becomes `PortableCode` (`local_executable` → `filepath_executable`). Other attributes and portable-code repository files are retained.
- Because the conversion changes each node's class and an attribute, its previously stored `_aiida_hash` is no longer valid. The migration removes the hash rather than recomputing it; run `verdi node rehash` afterwards if you need these nodes for hash-based caching. The conversion covers PostgreSQL, SQLite, and archives upgraded on import.
- Loading an unmigrated legacy node now raises `IncompatibleStorageSchema` rather than silently treating it as `Data`. Queries and code loaders continue to find the modern code plugins through the abstract `Code` base class.
- Legacy documentation and examples were updated to point to the modern code types.

## Separate design question (not part of `main_0003`)

`Code` also carries batch-job invocation settings, making it closer to a configured scheduler command than a general interface to a binary. If it should become a `Computer`-like entity instead of a `Data` node, changing its inheritance alone is not enough: CalcJobs currently record codes through `INPUT_CALC` provenance links, which require `Data` sources. That design would need another way to retain an immutable, linked record of the executable configuration, plus changes to storage, archives, queries, and the engine. The smaller alternative is to keep the code node as provenance `Data` and separate batch-script behavior from it. This architectural choice is independent of the legacy-node conversion above.
