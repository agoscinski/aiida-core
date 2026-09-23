# Code base-class design and engine follow-up

The original `AbstractCode` combined abstract methods with shared behavior. This migration removes the deprecated concrete `Code` plugin and renames that original base to `Code`, without adding a second abstract base. Concrete plugins inherit `Code`; queries, CalcJob validation, and loaders use it too. The `core.code.abstract` entry point is removed. External plugins that subclassed or imported `AbstractCode` must update their imports and inheritance.

This API change does not change concrete database node types. Existing nodes stored with the legacy concrete `Code` plugin are converted by `main_0003` to `InstalledCode` or `PortableCode`.

## Separate concern: what `Code` represents

The public `Code` API is not just an interface to an executable. It also carries batch-job invocation settings such as MPI defaults, quoting, and `prepend_text`/`append_text` for generated job scripts—closer to a configured scheduler command (a “SchedulerCode”) than a general binary interface. Review whether executable identity, invocation defaults, and job-script configuration belong in one abstraction. This is distinct from consolidating the two abstract classes and should not be changed as part of the legacy-node migration.

## Candidate engine-facing `CodeExecutionProtocol` (not implemented)

The engine's actual dependencies are in `src/aiida/engine/processes/calcjobs/calcjob.py` and `src/aiida/engine/daemon/execmanager.py`:

- `CalcJob` accepts `Code` as an input, links it as provenance `Data`, uses `code.computer` to select/check a computer, and reloads it by UUID. These are **ORM/provenance requirements**, not an execution interface.
- During presubmit it calls `can_run_on_computer(computer)` and `validate_working_directory(folder)`; combines `prepend_text`/`append_text` with computer and CalcJob text; resolves `with_mpi` against CalcJob and plugin options; calls `get_prepend_cmdline_params(mpi_args, extra_mpirun_params)` and `get_executable_cmdline_params(code_info.cmdline_params)`; and reads `use_double_quotes`/`wrap_cmdline_params` for the job template.
- During upload, `execmanager` checks `isinstance(code, PortableCode)` and copies files from `code.base.repository`, then makes `code.filepath_executable` executable. That special case is **not** covered by the abstract `Code` contract.

An initial, behavior-preserving engine-facing contract would therefore reflect those exact capabilities, rather than only `get_executable()`:

```python
class CodeExecutionProtocol(Protocol):
    def can_run_on_computer(self, computer: Computer) -> bool: ...
    def validate_working_directory(self, folder: Folder) -> None: ...
    def get_prepend_cmdline_params(
        self, mpi_args: list[str] | None = None, extra_mpirun_params: list[str] | None = None
    ) -> list[str]: ...
    def get_executable_cmdline_params(self, cmdline_params: list[str] | None = None) -> list[str]: ...

    @property
    def with_mpi(self) -> bool | None: ...
    @property
    def prepend_text(self) -> str: ...
    @property
    def append_text(self) -> str: ...
    @property
    def use_double_quotes(self) -> bool: ...
    @property
    def wrap_cmdline_params(self) -> bool: ...
```

This describes **current script-generation behavior**, not a general-purpose binary interface; the job's resources and scheduler directives remain the responsibility of `CalcJob`/`Computer`/the scheduler. Portable-code staging needs its own capability or adapter instead of a concrete-class check. Place `CodeExecutionProtocol` in a shared engine module (for example, `aiida.engine.code_protocols`), since both CalcJob presubmit and daemon upload consume codes. The ORM must not import or inherit it: the single abstract `Code(Data)` base and its concrete subclasses satisfy the protocol structurally, while the engine imports it for execution-facing types. ORM/CalcJob provenance still requires a stored `Data` node and is separate from the protocol.

The final `Code` is the original `AbstractCode` renamed, with shared behavior intact; it stays abstract and is not a storable plugin. `CodeExecutionProtocol` is only the engine-facing contract, not another ORM base or data-plugin entry point. This new `Code` must not be confused with the removed *legacy concrete* `Code` plugin.
