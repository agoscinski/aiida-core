.. _topics:workflows:graphs:

Graphs of tasks
===============

Workflows are plain Python functions using a tiny embedded language: assignments, calls, and returns, plus ``if`` and ``while`` over task results.
Graph bodies never execute; AiiDA walks their source and lowers the supported constructs into a ``GraphSpec``, a static JSON-serializable DAG of tasks, dependencies, and typed ports, rejecting everything else with a source location.
The engine runs the spec rather than the source, which is what makes provenance, checkpointing, caching, and daemon dispatch possible.
An ``if`` becomes a branch dispatching exactly one side as a child graph; a ``while`` becomes a loop dispatching numbered child runs that thread state between iterations.

A graph declares dependencies between tasks. AiiDA parses the graph's Python source without executing its body, then schedules each task as a child process when its dependencies have finished.
Save declarations in an importable Python module, installed wherever the daemon runs. Notebook, ``__main__``, and nested function declarations are not yet supported.

Declaring and launching
----------------------

For example, save this as ``my_workflow.py``:

.. code:: python

    from aiida.engine import graph, task

    @task
    def add(x: int, y: int) -> int:
        return x + y

    @graph
    def pipeline(x: int, y: int) -> int:
        first = add(x=x, y=y)
        return add(x=first, y=y)

Launch it from another module or an interactive interpreter with a loaded AiiDA profile:

.. code:: python

    from aiida.engine import run_get_node, submit
    from my_workflow import pipeline

    outputs, node = run_get_node(pipeline, x=2, y=3)
    assert outputs['result'].value == 8
    submitted = submit(pipeline, x=2, y=3)
    declaration = pipeline.build()

Calling a graph directly is not supported. ``build()`` returns a ``GraphSpec`` without running tasks or the graph body.
Graph inputs are the function parameters, with literal defaults filled when absent, and graph outputs reference task outputs or input leaves.
Calls take named or positional arguments without unpacking. Assignments bind one name, unpack one tuple of task outputs, or share one placement across names. Arbitrary Python code, imports, comprehensions, and fan-out loops are rejected with a source location.

Task functions receive the runtime types their annotations declare: Python values for Python annotations and nodes for ORM annotations.
Their leaf outputs are converted to data nodes. Graphs return these existing nodes rather than creating new data themselves.
Every task is recorded with its own process node and a call link named after its placement.
Task functions can also be called or passed to ``run`` and ``submit`` independently.

Static named outputs
--------------------

Task outputs must be statically declared. A scalar return annotation declares a single ``result`` output.
Explicit output names declare multiple leaf outputs, returned as a tuple in the same order:

.. code:: python

    @task(outputs=('sum', 'product'))
    def arithmetic(x: int, y: int) -> tuple[int, int]:
        return x + y, x * y

    @graph
    def statistics(x: int, y: int):
        pair = arithmetic(x=x, y=y)
        return {'total': pair.sum, 'product': pair.product}

The return mapping declares named leaf outputs; it is not an output namespace.
Dynamic ports, bare ``PortModel`` task/graph returns, and runtime-keyed output collections are not supported.
Structured task outputs are supported in the successful arm of ``Result``, described below.
A task annotated ``-> None`` has no outputs and can run for its side effects.

Declared task failures
---------------------

Tasks that can fail declare a Rust-like ``Result[SuccessType, ErrorEnum]`` return
annotation. The success type may be a ``PortModel``, including nested fields;
the error type may be a union of ``ExitCodeEnum`` alternatives. Define error
enums by subclassing ``ExitCodeEnum`` with AiiDA ``ExitCode`` values, then return
``Ok(outputs)`` on success or ``Err(error)`` on failure. Graph calls expose a
nullable pair, ``(value, error)``:

.. code-block:: python

    from aiida.engine import Err, ExitCode, ExitCodeEnum, Ok, PortModel, Result, graph, task

    class DivisionErrors(ExitCodeEnum):
        ZERO_DENOMINATOR = ExitCode(410, 'The denominator is zero.', invalidates_cache=True)

    class InputErrors(ExitCodeEnum):
        NEGATIVE_NUMERATOR = ExitCode(411, 'The numerator is negative.')

    class Quotient(PortModel):
        ratio: float

    @task
    def divide(numerator: float, denominator: float) -> Result[Quotient, DivisionErrors | InputErrors]:
        if denominator == 0:
            return Err(DivisionErrors.ZERO_DENOMINATOR)
        if numerator < 0:
            return Err(InputErrors.NEGATIVE_NUMERATOR)
        return Ok(Quotient(ratio=numerator / denominator))

    @task
    def recover(error: DivisionErrors | InputErrors) -> float:
        return 0.0

    @task
    def read_ratio(value: Quotient) -> float:
        return value.ratio

    @graph
    def ratio(numerator: float, denominator: float) -> float:
        value, error = divide(numerator=numerator, denominator=denominator)
        if error is not None:
            selected = recover(error=error)
        else:
            selected = read_ratio(value=value)
        return selected

The builder extracts the result arms as ``SuccessType | None`` and
``ErrorEnum | None`` (or ``ErrorA | ErrorB | None`` for an error union). Tuple
unpacking binds these two arms; assigning the call to one name also allows
``result.value`` and ``result.error`` selection. Model fields can be selected
with dotted access, for example ``value.ratio`` or ``value.settings.count``.

Only the present arm creates data nodes and provenance links. The absent arm
is supplied to downstream inputs as ``None``; no synthetic null nodes are
created. Error inputs annotated with the enum or its union receive the original
enum member, not its storage node. Tasks can consume both arms directly using
annotations such as ``value: Quotient | None`` and
``error: DivisionErrors | InputErrors | None``. Graph conditions support ``if error``,
``if error is None`` and ``if error is not None``, as well as null tests on the
value arm. Use a presence test rather than truthiness for values such as zero.

Named multiple outputs use ``Result[tuple[...], ErrorEnum]`` with
``@task(outputs=...)``; the names live under the ``value`` arm. A task without
successful outputs uses ``Result[None, ErrorEnum]`` and returns ``Ok(None)``.

Error enums subclass ``ExitCodeEnum``, so non-``ExitCode`` values, non-positive
statuses, and non-string messages already fail when the enum class is defined.
They must be non-empty, with unique positive exit statuses across all
alternatives. Statuses and labels must not conflict with the
engine's built-in exit codes. For unions, labels are qualified as
``EnumName_MEMBER`` to distinguish enums with the same member name.
Declarations are registered on the task process spec and serialized under
``exit_codes`` in its ``TaskSpec`` IR, including ``invalidates_cache``. The
nullable result shapes and error enum types are persisted in
``result_namespace`` and survive a JSON round trip.

``Err`` accepts an error enum member or its ``ExitCode`` value. For messages with
placeholders, return ``Err(MyErrors.ERROR.value.format(...))``. The exit status
and cache-invalidation flag must match a declared error. Undeclared failures and
unwrapped return values from ``Result`` tasks raise a validation error. Tasks
without ``Result`` cannot return exit codes.

A declared failure sets the child task's exit status and message and produces
its error output. It counts as a completed ``Result`` for scheduling: graph
logic can inspect the error and recover. Undeclared failures, exceptions, and
missing required outputs still prevent dependent tasks from running and end the
graph with ``ERROR_TASK_FAILED``.

Structured inputs
-----------------

``PortModel`` declares fixed input namespaces, with nested fields, help text, defaults, and nullable values:

.. code:: python

    from aiida.engine import PortModel

    class Settings(PortModel):
        count: int = 2
        nullable: int | None

    class Configuration(PortModel):
        settings: Settings
        label: str = 'test'

    @task
    def read_config(config: Configuration) -> int:
        return config.settings.count

    @graph
    def configured(config: Configuration) -> int:
        first = read_config(config=config)
        return add(x=first, y=config.settings.count)

Launch with a mapping or model instance, for example ``run_get_node(configured, config={'settings': {'nullable': None}})``.
Models declare namespaces; tasks receive attribute-accessible namespace mappings, not reconstructed model instances.
A nullable field still requires its key unless it has a default. Missing required graph inputs are reported together by ``MissingRequiredInputsError``.
Whole namespaces and selected fields are wired without flattening ordinary dictionary-valued leaves or replacing original input-node identities.

Subgraphs, branches, and loops
-----------------------------

Calling another declared graph inside a graph places it as a subgraph. It runs as a child graph process:

.. code:: python

    @graph
    def outer(x: int, y: int) -> int:
        inner = pipeline(x=x, y=y)
        return add(x=inner, y=x)

An ``if`` dispatches exactly one side as a child graph. Its condition may be a bound name or a direct task call, which lowers like the bound form. Each side must assign one new name shared by both, with compatible leaf outputs:

.. code:: python

    @graph
    def choose(x: int, flag: bool) -> int:
        if flag:
            chosen = add(x=x, y=1)
        else:
            chosen = add(x=x, y=2)
        return chosen

A ``while`` condition may be a bound name or a direct task call, re-evaluated every iteration. Its body consists of task assignments, rebinding existing loop state, and must update the condition:

.. code:: python

    @task
    def subtract(x: int) -> int:
        return x - 1

    @task
    def positive(x: int) -> bool:
        return x > 0

    @graph
    def count(x: int, keep_going: bool) -> int:
        while keep_going:
            x = subtract(x=x)
            keep_going = positive(x=x)
        return x

Loop iterations are child graph runs. A loop that is never entered returns its initial state, like the equivalent Python loop.
The default limit is 1000 iterations; exceeding it with the condition still true fails the graph.

Explicit ordering
-----------------

Use the reserved ``after`` call argument when a task must wait for another without consuming its output:

.. code:: python

    @task
    def prepare(x: int) -> None:
        # Perform a provenance-aware side effect.
        pass

    @graph
    def ordered(x: int) -> int:
        prepared = prepare(x=x)
        result = add(x=x, y=1, after=prepared)
        return result

``after=(first, second)`` waits for both placements. ``after`` cannot be a task-function parameter.
Ordering dependencies are part of the serialized graph, checked for cycles, and preserved across checkpoints.

Persistence and failures
------------------------

Specifications store importable executor names and reject unsupported format versions or task kinds.
The graph checkpoint records dispatched and completed tasks, loop iterations, and passed-through initial loop state, so completed work is not dispatched again on resume.
An unexpected task failure prevents dependent tasks from starting and ends the graph with ``ERROR_TASK_FAILED``.
Declared ``Result`` errors instead remain available for graph-level branching.
Normal AiiDA task caching remains available; graphs do not introduce recovery handlers, retry policies, monitors, or partial-rerun APIs.
