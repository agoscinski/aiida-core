###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Structured scope-local resolution and validation of per-placement `CalcJob` metadata.

Configuration is a nested mapping descending by authored placement name::

    task_metadata = {'scf': {'pw_base': {'pw_calc': {'options': ...}}}}

A nested `SubgraphTask` descends the mapping by its `task.name`. `BranchControl`,
`LoopControl`, and `MapControl` bodies are transparent: they contribute no path segment,
so every static branch validates before execution and loop iterations and map items
reuse their placement's configuration. The leaf is the actual `CalcJob` `task.name` and
holds the ordinary `CalcJob` metadata mapping unchanged.
"""

from __future__ import annotations

import typing as t
from collections.abc import Mapping

from aiida.common.exceptions import GraphValidationError
from aiida.engine.processes.calcjobs import CalcJob
from aiida.engine.processes.graphs.ir import (
    BranchControl,
    GraphIR,
    LoopControl,
    MapControl,
    ProcessTask,
    SubgraphTask,
)
from aiida.engine.processes.ports import InputPort, PortNamespace

if t.TYPE_CHECKING:
    from aiida.engine.processes.process import Process

__all__ = ('resolve_metadata_path', 'validate_task_metadata')


def _dotted(path: tuple[str, ...]) -> str:
    """Return the dotted display of a metadata path, e.g. `'scf.pw_base.pw_calc'`."""
    return '.'.join(path)


def _skeleton(path: tuple[str, ...]) -> str:
    """Return the nested config skeleton locating a leaf, for missing-entry messages."""
    inner = '{...}'
    for segment in reversed(path):
        inner = f'{{{segment!r}: {inner}}}'
    return f'task_metadata={inner}'


def _missing_message(path: tuple[str, ...], owner: str) -> str:
    """Return the error for a required leaf with no configuration."""
    return (
        f"task_metadata is missing configuration for '{_dotted(path)}' "
        f"(a `CalcJob` dispatched as '{owner}'); add {_skeleton(path)}."
    )


def _non_mapping_message(path: tuple[str, ...], *, leaf: bool, value: object) -> str:
    """Return the error for a path segment holding a non-mapping value."""
    if leaf:
        return (
            f"task_metadata path '{_dotted(path)}' must be a mapping of metadata inputs, not `{type(value).__name__}`."
        )
    return f"task_metadata path '{_dotted(path)}' must be a mapping of placements, not `{type(value).__name__}`."


def resolve_metadata_path(provided: Mapping[str, t.Any], path: tuple[str, ...]) -> dict[str, t.Any]:
    """Return the leaf metadata mapping stored under a structural path, unchanged.

    :param provided: the launch `task_metadata` mapping.
    :param path: the structural path: enclosing `subgraph` placement names plus the
        `CalcJob` placement name as the leaf.
    :raises GraphValidationError: if a segment is absent or a non-mapping blocks descent.
    """
    current: t.Any = provided
    for depth, segment in enumerate(path):
        if not isinstance(current, Mapping):
            raise GraphValidationError(_non_mapping_message(path[:depth], leaf=False, value=current))
        if segment not in current:
            raise GraphValidationError(_missing_message(path, path[-1]))
        current = current[segment]
    if not isinstance(current, Mapping):
        raise GraphValidationError(_non_mapping_message(path, leaf=True, value=current))
    return dict(current)


def _is_calcjob(task: ProcessTask) -> bool:
    """Return whether a placement dispatches a `CalcJob` directly."""
    return issubclass(task.spec.process_class, CalcJob)


def _index(
    body: GraphIR,
    prefix: tuple[str, ...],
    leaves: dict[tuple[str, ...], list[type[Process]]],
    scopes: set[tuple[str, ...]],
) -> None:
    """Record every `CalcJob` leaf path and every nested scope path under one template.

    `SubgraphTask` names contribute a segment; branch, loop, and graph-mapped bodies are
    transparent and share their placement's level. Non-`CalcJob` placements take no
    metadata and contribute nothing to the index.
    """
    scopes.add(prefix)
    for task in body.tasks:
        if isinstance(task, ProcessTask):
            if _is_calcjob(task):
                path = (*prefix, task.name)
                if task.spec.process_class not in leaves.setdefault(path, []):
                    leaves[path].append(task.spec.process_class)
        elif isinstance(task, SubgraphTask):
            _index(task.body, (*prefix, task.name), leaves, scopes)
        elif isinstance(task, BranchControl):
            _index(task.body, prefix, leaves, scopes)
            _index(task.otherwise, prefix, leaves, scopes)
        elif isinstance(task, LoopControl):
            _index(task.body, prefix, leaves, scopes)
        elif isinstance(task, MapControl):
            if task.spec is not None:
                if issubclass(task.spec.process_class, CalcJob):
                    path = (*prefix, task.name)
                    if task.spec.process_class not in leaves.setdefault(path, []):
                        leaves[path].append(task.spec.process_class)
            elif task.body is not None:
                _index(task.body, prefix, leaves, scopes)


def _valid_at(
    prefix: tuple[str, ...],
    leaves: dict[tuple[str, ...], list[type[Process]]],
) -> list[str]:
    """Return the sorted placement names addressable directly under one scope path."""
    names: set[str] = set()
    for path in leaves:
        if len(path) > len(prefix) and path[: len(prefix)] == prefix:
            names.add(path[len(prefix)])
    return sorted(names)


def _under(path: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    """Return whether a leaf path lies at or below a scope path."""
    return len(path) >= len(prefix) and path[: len(prefix)] == prefix


def _check_provided(
    node: Mapping[object, t.Any],
    prefix: tuple[str, ...],
    leaves: dict[tuple[str, ...], list[type[Process]]],
    scopes: set[tuple[str, ...]],
    errors: list[str],
    broken: set[tuple[str, ...]],
) -> None:
    """Check one provided mapping level against the indexed template, recording problems.

    Unknown paths, leaf/scope conflicts, and non-mapping values fail here; leaves already
    broken by an ancestor error are skipped by the later per-leaf pass.
    """
    for key, value in node.items():
        if not isinstance(key, str):
            parent = _dotted(prefix) if prefix else '<root>'
            errors.append(f'task_metadata at {parent!r} requires string placement names, got {key!r}.')
            continue
        path = (*prefix, key)
        in_leaves = path in leaves
        in_scopes = path in scopes and any(len(leaf) > len(path) and _under(leaf, path) for leaf in leaves)
        # `scopes` always contains `prefix` itself; only a scope with leaves below counts.
        if in_leaves and in_scopes:
            errors.append(
                f"task_metadata path '{_dotted(path)}' is both a `CalcJob` leaf "
                'and a nested scope; rename one placement so the entry addresses exactly one.'
            )
            broken.update(leaf for leaf in leaves if _under(leaf, path))
            continue
        if in_scopes:
            if not isinstance(value, Mapping):
                errors.append(_non_mapping_message(path, leaf=False, value=value))
                broken.update(leaf for leaf in leaves if _under(leaf, path))
                continue
            _check_provided(value, path, leaves, scopes, errors, broken)
            continue
        if in_leaves:
            if not isinstance(value, Mapping):
                errors.append(_non_mapping_message(path, leaf=True, value=value))
                broken.add(path)
            continue
        parent = _dotted(prefix) if prefix else '<root>'
        errors.append(
            f"task_metadata has unknown path '{_dotted(path)}'; no such placement under "
            f"'{parent}'; valid placements are {_valid_at(prefix, leaves)}."
        )


def _validate_leaf_mapping(
    namespace: PortNamespace, value: Mapping[str, t.Any], path: tuple[str, ...], owner: str, errors: list[str]
) -> None:
    """Check provided metadata keys against one `CalcJob` metadata namespace, without filling.

    Unknown keys fail unless the namespace level is dynamic; provided values run through
    their port's validation. Required ports are never enforced here: defaults and dispatch
    fill them, so only the stored mapping's own entries are judged. The validated mapping
    itself is never modified.
    """
    for key, entry in value.items():
        if key not in namespace:
            if namespace.dynamic:
                continue
            valid = sorted(namespace.keys())
            errors.append(
                f"task_metadata path '{_dotted(path)}' has unknown metadata key '{key}' "
                f"for `CalcJob` '{owner}'; valid keys are {valid}."
            )
            continue
        port = namespace[key]
        if isinstance(port, PortNamespace):
            if not isinstance(entry, Mapping):
                errors.append(_non_mapping_message((*path, key), leaf=False, value=entry))
                continue
            _validate_leaf_mapping(port, entry, (*path, key), owner, errors)
            continue
        if not isinstance(port, InputPort):
            errors.append(
                f"task_metadata path '{_dotted(path)}' names metadata port '{key}' "
                f"of unexpected type `{type(port).__name__}` for `CalcJob` '{owner}'."
            )
            continue
        try:
            runtime = port.prepare(entry).runtime
        except (TypeError, ValueError):
            runtime = entry
        error = port.validate(runtime)
        if error is not None:
            errors.append(f"task_metadata path '{_dotted((*path, key))}' is invalid for `CalcJob` '{owner}': {error}.")


def _validate_leaf(
    calc_class: type[Process], path: tuple[str, ...], leaf: Mapping[str, t.Any], errors: list[str]
) -> None:
    """Check one leaf mapping against its `CalcJob` metadata spec, keeping the contents."""
    namespace = calc_class.spec().inputs['metadata']
    assert isinstance(namespace, PortNamespace)
    _validate_leaf_mapping(namespace, leaf, path, calc_class.__name__, errors)


def validate_task_metadata(body: GraphIR, provided: object) -> None:
    """Validate structured `task_metadata` against a graph template before execution.

    Every transitively reachable `CalcJob` leaf needs a mapping at its structural path;
    unknown paths, non-mapping values, and leaf entries violating the `CalcJob` metadata
    spec all fail. All problems report together with dotted paths.

    :param body: the compiled root template.
    :param provided: the launch `task_metadata` mapping, or `None` for none given.
    :raises GraphValidationError: describing every missing, unknown, or invalid entry.
    """
    errors: list[str] = []
    broken: set[tuple[str, ...]] = set()
    given = {} if provided is None else provided
    if not isinstance(given, Mapping):
        msg = f'`task_metadata` must be a mapping of placements, not `{type(given).__name__}`.'
        raise GraphValidationError(msg)
    leaves: dict[tuple[str, ...], list[type[Process]]] = {}
    scopes: set[tuple[str, ...]] = set()
    _index(body, (), leaves, scopes)
    _check_provided(given, (), leaves, scopes, errors, broken)
    for path in sorted(leaves):
        if path in broken:
            continue
        try:
            leaf = resolve_metadata_path(given, path)
        except GraphValidationError as exception:
            errors.append(str(exception))
            continue
        for calc_class in leaves[path]:
            _validate_leaf(calc_class, path, leaf, errors)
    if errors:
        msg = 'invalid `task_metadata`:\n- ' + '\n- '.join(dict.fromkeys(errors))
        raise GraphValidationError(msg)
