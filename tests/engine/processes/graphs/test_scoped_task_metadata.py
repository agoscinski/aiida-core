###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Scope-aware `CalcJob` metadata: exact labels, scope rules, and leaf fallback."""

import pytest

from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.common.exceptions import GraphValidationError
from aiida.common.links import LinkType
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.ir import ProcessTask
from aiida.engine.processes.graphs.lowering import compile_graph, graph, task
from aiida.engine.processes.graphs.tasks import _merge_task_metadata, _resolve_task_metadata
from aiida.orm import Code

pytestmark = pytest.mark.presto

pw_calc = task(ArithmeticAddCalculation)


@task
def add(x: int, y: int) -> int:
    """Add two numbers without a `CalcJob`."""
    return x + y


@graph
def pw_base(x: int | float, y: int | float, code: Code) -> int | float:
    """Run one `CalcJob` placement shared by both scopes."""
    total = pw_calc(x=x, y=y, code=code)
    return total.sum


@graph
def scf(x: int | float, y: int | float, code: Code) -> int | float:
    """Wrap the shared base under the `scf` scope."""
    return pw_base(x=x, y=y, code=code)


@graph
def nscf(x: int | float, y: int | float, code: Code) -> int | float:
    """Wrap the shared base under the `nscf` scope."""
    return pw_base(x=x, y=y, code=code)


@graph
def scf_nscf(x: int | float, y: int | float, code: Code) -> int | float:
    """Reuse the same nested graph twice so one inner name dispatches in two scopes."""
    first = scf(x=x, y=y, code=code)
    second = nscf(x=first, y=y, code=code)
    return second


@graph
def repeated_base(x: int | float, y: int | float, code: Code) -> int | float:
    """Call one nested graph twice so dedup labels separate the two scopes."""
    first = pw_base(x=x, y=y, code=code)
    second = pw_base(x=first, y=y, code=code)
    return second


def _options(node) -> dict:
    """Return the `CALL_CALC` children keyed by their hierarchical call label."""
    return {entry.link_label: entry.node for entry in node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all()}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_distinct_leaf_names_still_work(aiida_code_installed) -> None:
    """Two different placement names keep receiving different scheduler options."""
    from tests.engine.processes.graphs.programs import calcjobs

    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        calcjobs.add_twice,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'add_calc': {'options': {'max_wallclock_seconds': 100}},
            'add_calc_2': {'options': {'max_wallclock_seconds': 200}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['add_calc'].get_option('max_wallclock_seconds') == 100
    assert children['add_calc_2'].get_option('max_wallclock_seconds') == 200


@pytest.mark.usefixtures('aiida_profile_clean')
def test_reused_nested_graph_exact_labels(aiida_code_installed) -> None:
    """Exact hierarchical labels give the same inner name different scheduler options."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        scf_nscf,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'scf__pw_base__pw_calc': {'options': {'max_wallclock_seconds': 111}},
            'nscf__pw_base__pw_calc': {'options': {'max_wallclock_seconds': 222}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['scf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 111
    assert children['nscf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 222


@pytest.mark.usefixtures('aiida_profile_clean')
def test_reused_nested_graph_scope_rules(aiida_code_installed) -> None:
    """One `<scope>__*` rule covers every `CalcJob` under a reused subgraph."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        scf_nscf,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'scf__*': {'options': {'max_wallclock_seconds': 333}},
            'nscf__*': {'options': {'max_wallclock_seconds': 444}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['scf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 333
    assert children['nscf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 444


@pytest.mark.usefixtures('aiida_profile_clean')
def test_exact_label_wins_over_scope_and_leaf(aiida_code_installed) -> None:
    """Exact per-placement entries beat scope rules, which beat the shared leaf name."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        scf_nscf,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'pw_calc': {'options': {'max_wallclock_seconds': 10}},
            'scf__*': {'options': {'max_wallclock_seconds': 20}},
            'nscf__*': {'options': {'max_wallclock_seconds': 30}},
            'nscf__pw_base__pw_calc': {'options': {'max_wallclock_seconds': 40}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['scf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 20
    assert children['nscf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 40


@pytest.mark.usefixtures('aiida_profile_clean')
def test_leaf_fallback_still_applies_everywhere(aiida_code_installed) -> None:
    """Old leaf keys keep working when no exact or scope entry exists."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        scf_nscf, x=2, y=3, code=code, task_metadata={'pw_calc': {'options': {'max_wallclock_seconds': 555}}}
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['scf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 555
    assert children['nscf__pw_base__pw_calc'].get_option('max_wallclock_seconds') == 555


@pytest.mark.usefixtures('aiida_profile_clean')
def test_dedup_scopes_separate(aiida_code_installed) -> None:
    """Two calls to one nested graph separate through dedup scope prefixes."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        repeated_base,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'pw_base__*': {'options': {'max_wallclock_seconds': 111}},
            'pw_base_2__*': {'options': {'max_wallclock_seconds': 222}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _options(node)
    assert children['pw_base__pw_calc'].get_option('max_wallclock_seconds') == 111
    assert children['pw_base_2__pw_calc'].get_option('max_wallclock_seconds') == 222


def test_ambiguous_glob_keys_rejected() -> None:
    """Only exact labels and explicit `<scope>__*` rules resolve; other stars fail."""
    task_spec = compile_graph(pw_base).task('pw_calc')
    assert isinstance(task_spec, ProcessTask)
    with pytest.raises(GraphValidationError, match=r'__\*'):
        _merge_task_metadata(task_spec, {'scf*': {'options': {}}}, {}, instance='scf__pw_base__pw_calc')
    with pytest.raises(GraphValidationError, match=r'__\*'):
        _resolve_task_metadata(task_spec, {'*': {'options': {}}}, instance='scf__pw_base__pw_calc')


def test_scope_key_covers_loop_iterations() -> None:
    """Scope matching ignores `_iteration_N` segments so one rule covers every pass."""
    task_spec = compile_graph(pw_base).task('pw_calc')
    assert isinstance(task_spec, ProcessTask)
    resolved = _resolve_task_metadata(
        task_spec,
        {'scf__*': {'options': {'max_wallclock_seconds': 99}}},
        instance='scf__loop_1_iteration_3__pw_calc',
    )
    assert resolved == {'options': {'max_wallclock_seconds': 99}}
    resolved_loop = _resolve_task_metadata(
        task_spec,
        {'myloop__*': {'options': {'max_wallclock_seconds': 77}}},
        instance='myloop_iteration_2__pw_calc',
    )
    assert resolved_loop == {'options': {'max_wallclock_seconds': 77}}


def test_deduplicated_leaf_does_not_inherit_another_placement() -> None:
    """An absent second placement entry must not silently reuse the first one's options."""
    from tests.engine.processes.graphs.programs import calcjobs

    second = compile_graph(calcjobs.add_twice).task('add_calc_2')
    assert isinstance(second, ProcessTask)
    with pytest.raises(GraphValidationError, match='add_calc_2'):
        _resolve_task_metadata(second, {'add_calc': {'options': {}}}, instance='scope__add_calc_2')


def test_missing_metadata_names_instance() -> None:
    """A missing entry names both the placement and the hierarchical instance."""
    task_spec = compile_graph(pw_base).task('pw_calc')
    assert isinstance(task_spec, ProcessTask)
    with pytest.raises(GraphValidationError, match='scf__pw_base__pw_calc'):
        _merge_task_metadata(task_spec, {}, {}, instance='scf__pw_base__pw_calc')
