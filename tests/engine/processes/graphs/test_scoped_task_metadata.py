###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Structured, scope-local `task_metadata`: placement names descend, control is transparent.

Contract:
- Keys use the callee alias names. Module-level ``scf = pw_base; nscf = pw_base``
  places ``SubgraphTask`` names ``scf`` and ``nscf``; each descends the mapping
  by ``task.name`` (``{'scf': {'pw_calc': {...}}, 'nscf': {'pw_calc': {...}}}``).
  A further nesting level descends again (``{'scf_deep': {'pw_base': {'pw_calc': ...}}}``).
- Loop, branch, and map control frames are transparent: no synthetic
  ``branch_N``/``loop_N`` keys, iterations reuse their placement's config.
- A repeated metadata-bearing alias in the same authored scope is rejected at
  build time with guidance to use distinct Python aliases. Provenance naming
  (hierarchical ``__`` call labels) is unchanged and separate from addressing.
- Pure task-function and pure-graph duplicates keep numeric suffixes unchanged.
"""

import pytest

from aiida import orm
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.common.exceptions import GraphLanguageError, GraphValidationError
from aiida.common.links import LinkType
from aiida.engine.launch import run_get_node
from aiida.engine.processes.graphs.lowering import compile_graph, graph, task
from aiida.engine.processes.graphs.run import GraphRun
from aiida.engine.processes.graphs.tasks import _merge_task_metadata
from tests.engine.processes.graphs.programs import structured_metadata

pytestmark = pytest.mark.presto


@task
def pure_add(x: int, y: int) -> int:
    """Add two numbers without a `CalcJob`."""
    return x + y


@graph
def pure_twice(x: int, y: int) -> int:
    """Call a pure task twice in one scope."""
    first = pure_add(x=x, y=y)
    second = pure_add(x=first, y=y)
    return second


@graph
def dup_nested(x: int | float, y: int | float, code) -> int | float:
    """Repeat one `CalcJob`-bearing graph alias in a single scope (rejected)."""
    first = structured_metadata.pw_base(x=x, y=y, code=code)
    second = structured_metadata.pw_base(x=first, y=y, code=code)
    return second


@graph
def dup_calc(x: int | float, y: int | float, code) -> int | float:
    """Repeat one direct `CalcJob` alias in a single scope (rejected)."""
    first = structured_metadata.pw_calc(x=x, y=y, code=code)
    second = structured_metadata.pw_calc(x=first.sum, y=y, code=code)
    return second.sum


def _calc_options(node) -> dict[str, int | None]:
    """Return the `CalcJob` children keyed by hierarchical call label with their option."""
    return {
        entry.link_label: entry.node.get_option('max_wallclock_seconds')
        for entry in node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all()
    }


def _stored_counts() -> tuple[int, int]:
    """Return the stored (`CalcJob`, `Graph`) node counts for rejection isolation checks."""
    calcjobs = orm.QueryBuilder().append(orm.CalcJobNode).count()
    graphs = orm.QueryBuilder().append(orm.GraphNode).count()
    return calcjobs, graphs


@pytest.mark.usefixtures('aiida_profile_clean')
def test_direct_aliases_different_options(aiida_code_installed) -> None:
    """Two direct `CalcJob` aliases receive different scheduler options."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        structured_metadata.direct_two,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'calc_first': {'options': {'max_wallclock_seconds': 100}},
            'calc_second': {'options': {'max_wallclock_seconds': 200}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _calc_options(node)
    assert children['calc_first'] == 100
    assert children['calc_second'] == 200


@pytest.mark.usefixtures('aiida_profile_clean')
def test_nested_aliases_different_options(aiida_code_installed) -> None:
    """The same nested graph through two aliases receives different options per scope."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        structured_metadata.nested_two,
        x=2,
        y=3,
        code=code,
        task_metadata={
            'scf': {'pw_calc': {'options': {'max_wallclock_seconds': 111}}},
            'nscf': {'pw_calc': {'options': {'max_wallclock_seconds': 222}}},
        },
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _calc_options(node)
    assert children['scf__pw_calc'] == 111
    assert children['nscf__pw_calc'] == 222


@pytest.mark.usefixtures('aiida_profile_clean')
def test_deep_hierarchy_descends_three_levels(aiida_code_installed) -> None:
    """A doubly nested graph addresses its leaf through the full `scf.pw_base.pw_calc` path."""
    from aiida.engine.processes.graphs.tasks import resolve_metadata_path

    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    metadata = {
        'scf_deep': {'pw_base': {'pw_calc': {'options': {'max_wallclock_seconds': 111}}}},
        'nscf_deep': {'pw_base': {'pw_calc': {'options': {'max_wallclock_seconds': 222}}}},
    }
    # The leaf is the ordinary `CalcJob` metadata mapping, passed through unchanged.
    assert resolve_metadata_path(metadata, ('scf_deep', 'pw_base', 'pw_calc')) == {
        'options': {'max_wallclock_seconds': 111}
    }
    assert resolve_metadata_path(metadata, ('nscf_deep', 'pw_base', 'pw_calc')) == {
        'options': {'max_wallclock_seconds': 222}
    }
    outputs, node = run_get_node(structured_metadata.deep_two, x=2, y=3, code=code, task_metadata=metadata)
    assert node.is_finished_ok
    assert outputs['result'].value == 8
    children = _calc_options(node)
    assert children == {'scf_deep__pw_base__pw_calc': 111, 'nscf_deep__pw_base__pw_calc': 222}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_authored_dunder_alias_is_one_placement(aiida_code_installed) -> None:
    """An authored alias containing `__` addresses one placement; it is never parsed as a hierarchy."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    outputs, node = run_get_node(
        structured_metadata.dunder_single,
        x=2,
        y=3,
        code=code,
        task_metadata={'my__calc': {'options': {'max_wallclock_seconds': 888}}},
    )
    assert node.is_finished_ok
    assert outputs['result'].value == 5
    assert _calc_options(node) == {'my__calc': 888}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_loop_iterations_reuse_config(aiida_code_installed) -> None:
    """Loop iterations reuse their placement's configuration; no per-iteration keys."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    _outputs, node = run_get_node(
        structured_metadata.loop_reuse,
        x=2,
        y=3,
        code=code,
        limit=1,
        task_metadata={'pw_calc': {'options': {'max_wallclock_seconds': 333}}},
    )
    assert node.is_finished_ok
    children = _calc_options(node)
    assert len(children) == 2
    assert set(children.values()) == {333}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_loop_nested_reuses_scope_config(aiida_code_installed) -> None:
    """A nested-graph alias outside and inside a loop shares one scope configuration."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    _outputs, node = run_get_node(
        structured_metadata.loop_nested,
        x=2,
        y=3,
        code=code,
        limit=1,
        task_metadata={'scf': {'pw_calc': {'options': {'max_wallclock_seconds': 343}}}},
    )
    assert node.is_finished_ok
    children = _calc_options(node)
    assert len(children) == 2
    assert set(children.values()) == {343}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_branch_routes_correct_config(aiida_code_installed) -> None:
    """Only the taken arm dispatches, under the branch scope's provenance prefix.

    Configuration addressing stays structural (``scf.pw_calc``); provenance labels
    keep the enclosing control scope (``branch_1__scf__pw_calc``). The engine is
    unchanged: these expectations pin the existing hierarchical call labels.
    """
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    metadata = {
        'scf': {'pw_calc': {'options': {'max_wallclock_seconds': 444}}},
        'nscf': {'pw_calc': {'options': {'max_wallclock_seconds': 555}}},
    }
    _, taken = run_get_node(structured_metadata.branch_route, x=2, y=3, code=code, flag=True, task_metadata=metadata)
    assert taken.is_finished_ok
    assert _calc_options(taken) == {'branch_1__scf__pw_calc': 444}
    _, skipped = run_get_node(structured_metadata.branch_route, x=2, y=3, code=code, flag=False, task_metadata=metadata)
    assert skipped.is_finished_ok
    assert _calc_options(skipped) == {'branch_1__nscf__pw_calc': 555}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_branch_shared_alias_reuses_config(aiida_code_installed) -> None:
    """The same alias in both arms shares one configuration; both arms are exercised."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    metadata = {'pw_calc': {'options': {'max_wallclock_seconds': 666}}}
    _, taken = run_get_node(structured_metadata.branch_shared, x=2, y=3, code=code, flag=True, task_metadata=metadata)
    assert taken.is_finished_ok
    assert list(_calc_options(taken).values()) == [666]
    _, skipped = run_get_node(
        structured_metadata.branch_shared, x=2, y=3, code=code, flag=False, task_metadata=metadata
    )
    assert skipped.is_finished_ok
    assert list(_calc_options(skipped).values()) == [666]


@pytest.mark.usefixtures('aiida_profile_clean')
def test_all_missing_rejected_before_children(aiida_code_installed) -> None:
    """An empty mapping names every missing placement path and stores no child."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    before = _stored_counts()
    with pytest.raises(GraphValidationError, match='scf'):
        run_get_node(structured_metadata.nested_two, x=2, y=3, code=code, task_metadata={})
    assert _stored_counts() == before


@pytest.mark.usefixtures('aiida_profile_clean')
def test_missing_entry_rejected_before_children(aiida_code_installed) -> None:
    """A missing nested entry fails loudly naming the placement path, before dispatch."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    before = _stored_counts()
    with pytest.raises(GraphValidationError, match='nscf'):
        run_get_node(
            structured_metadata.nested_two,
            x=2,
            y=3,
            code=code,
            task_metadata={'scf': {'pw_calc': {'options': {}}}},
        )
    assert _stored_counts() == before


@pytest.mark.usefixtures('aiida_profile_clean')
def test_unknown_path_rejected_before_children(aiida_code_installed) -> None:
    """A misspelled placement path fails loudly instead of running with defaults."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    before = _stored_counts()
    with pytest.raises(GraphValidationError, match='scff'):
        run_get_node(
            structured_metadata.nested_two,
            x=2,
            y=3,
            code=code,
            task_metadata={
                'scff': {'pw_calc': {'options': {}}},
                'nscf': {'pw_calc': {'options': {}}},
            },
        )
    assert _stored_counts() == before


@pytest.mark.usefixtures('aiida_profile_clean')
def test_malformed_metadata_rejected_before_children(aiida_code_installed) -> None:
    """A non-mapping leaf fails loudly with an actionable path, before dispatch."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    before = _stored_counts()
    with pytest.raises(GraphValidationError, match='pw_calc'):
        run_get_node(
            structured_metadata.nested_two,
            x=2,
            y=3,
            code=code,
            task_metadata={
                'scf': {'pw_calc': ['options']},
                'nscf': {'pw_calc': {'options': {}}},
            },
        )
    assert _stored_counts() == before


@pytest.mark.usefixtures('aiida_profile_clean')
def test_scheduler_checkpoint_reuses_completed_calcjob(aiida_code_installed) -> None:
    """Mid-run `GraphRun` state round-trips the structural path; the done `CalcJob` never resubmits.

    Provenance instances (``scf__pw_calc``) differ from structural config paths
    ((``scf``, ``pw_calc``)); the checkpoint rebuilds the path from scope lineage,
    not by parsing the label.
    """
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    spec = compile_graph(structured_metadata.nested_two)
    given = {'x': orm.Int(2), 'y': orm.Int(3), 'code': code}
    metadata: dict = {
        'scf': {'pw_calc': {'options': {'max_wallclock_seconds': 111}}},
        'nscf': {'pw_calc': {'options': {'max_wallclock_seconds': 222}}},
    }
    state = GraphRun(spec, given=given)
    first_step = state.step()
    assert [start.instance for start in first_step.starts] == ['scf__pw_calc']
    (first_start,) = first_step.starts
    assert first_start.metadata_path == ('scf', 'pw_calc')
    merged = _merge_task_metadata(
        first_start.task, metadata, dict(first_start.inputs), metadata_path=('scf', 'pw_calc')
    )
    assert merged['metadata'] == {'options': {'max_wallclock_seconds': 111}}
    _, first_node = run_get_node(ArithmeticAddCalculation, **merged)
    assert first_node.get_option('max_wallclock_seconds') == 111
    state.started('scf__pw_calc', first_node.pk)
    state.completed('scf__pw_calc', first_node.pk)

    saved = state.to_dict()
    restored = GraphRun.from_dict(spec, given, saved)
    assert restored.to_dict() == saved
    assert restored.done == state.done
    second_step = restored.step()
    assert len(second_step.starts) == 1
    (second_start,) = second_step.starts
    assert second_start.instance == 'nscf__pw_calc'
    assert second_start.metadata_path == ('nscf', 'pw_calc')
    merged_second = _merge_task_metadata(
        second_start.task, metadata, dict(second_start.inputs), metadata_path=('nscf', 'pw_calc')
    )
    assert merged_second['metadata'] == {'options': {'max_wallclock_seconds': 222}}
    _, second_node = run_get_node(ArithmeticAddCalculation, **merged_second)
    assert second_node.get_option('max_wallclock_seconds') == 222
    restored.started('nscf__pw_calc', second_node.pk)
    restored.completed('nscf__pw_calc', second_node.pk)
    assert restored.step().starts == []
    assert set(restored.done) == {'scf__pw_calc', 'nscf__pw_calc'}


@pytest.mark.usefixtures('aiida_profile_clean')
def test_loop_checkpoint_reuses_placement_config(aiida_code_installed) -> None:
    """Loop iterations share one structural path across a scheduler checkpoint.

    The first dispatch (``pw_calc``) and the loop dispatch
    (``loop_1_iteration_0__pw_calc``) carry different provenance labels but the same
    config path ((``pw_calc``,)); the checkpoint preserves the completed run.
    """
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    spec = compile_graph(structured_metadata.loop_reuse)
    given = {'x': orm.Int(2), 'y': orm.Int(3), 'code': code, 'limit': 1, 'attempt': 0}
    metadata: dict = {'pw_calc': {'options': {'max_wallclock_seconds': 777}}}
    state = GraphRun(spec, given=given)
    first_step = state.step()
    assert [start.instance for start in first_step.starts] == ['pw_calc']
    (first_start,) = first_step.starts
    assert first_start.metadata_path == ('pw_calc',)
    merged = _merge_task_metadata(first_start.task, metadata, dict(first_start.inputs), metadata_path=('pw_calc',))
    _, first_node = run_get_node(ArithmeticAddCalculation, **merged)
    assert first_node.get_option('max_wallclock_seconds') == 777
    state.started('pw_calc', first_node.pk)
    state.completed('pw_calc', first_node.pk)

    restored = GraphRun.from_dict(spec, given, state.to_dict())
    assert restored.to_dict() == state.to_dict()
    assert restored.done == {'pw_calc': first_node.pk}
    second_step = restored.step()
    assert len(second_step.starts) == 1
    (second_start,) = second_step.starts
    # The loop iteration carries the branch/loop scope prefix in provenance...
    assert second_start.instance != 'pw_calc'
    assert second_start.instance.endswith('__pw_calc')
    # ...but its configuration path is the same structural placement.
    assert second_start.metadata_path == ('pw_calc',)
    merged_second = _merge_task_metadata(
        second_start.task, metadata, dict(second_start.inputs), metadata_path=('pw_calc',)
    )
    assert merged_second['metadata'] == {'options': {'max_wallclock_seconds': 777}}
    _, second_node = run_get_node(ArithmeticAddCalculation, **merged_second)
    assert second_node.get_option('max_wallclock_seconds') == 777
    restored.started('loop_1_iteration_0__pw_calc', second_node.pk)
    restored.completed('loop_1_iteration_0__pw_calc', second_node.pk)
    assert [start.instance for start in restored.step().starts] == []


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize(
    'configuration, offending',
    [
        pytest.param({'pw_calc': {'options': {}}}, 'pw_calc', id='flat-nested-fallback'),
        pytest.param({'scf__pw_calc': {'options': {}}}, 'scf__pw_calc', id='provenance-label'),
        pytest.param({'scf__*': {'options': {}}}, 'scf__', id='wildcard'),
        pytest.param({1: {}}, 'string placement', id='non-string-key'),
        pytest.param({'scf': []}, 'scf', id='non-mapping-scope'),
        pytest.param({'scf': {'pw_calc': {'options': []}}}, 'scf.pw_calc.options', id='non-mapping-options'),
        pytest.param(
            {'scf': {'pw_calc': {'options': {'max_wallclock_seconds': 'bad'}}}},
            'scf.pw_calc.options.max_wallclock_seconds',
            id='wrong-option-type',
        ),
    ],
)
def test_invalid_configuration_has_actionable_path(aiida_code_installed, configuration, offending) -> None:
    """Invalid paths and metadata fail before a root or calculation is stored, without fallbacks."""
    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    before = _stored_counts()
    with pytest.raises(GraphValidationError) as error:
        run_get_node(structured_metadata.nested_two, x=2, y=3, code=code, task_metadata=configuration)
    assert offending in str(error.value)
    assert _stored_counts() == before


@pytest.mark.usefixtures('aiida_profile_clean')
def test_metadata_contents_are_preserved(aiida_code_installed) -> None:
    """Validation and dispatch do not replace, merge, or fill the selected metadata leaf."""
    import copy

    code = aiida_code_installed(default_calc_job_plugin='core.arithmetic.add', filepath_executable='/bin/bash')
    metadata = {
        'scf': {'pw_calc': {'label': 'first', 'options': {'max_wallclock_seconds': 111}}},
        'nscf': {'pw_calc': {'label': 'second', 'options': {'max_wallclock_seconds': 222}}},
    }
    before = copy.deepcopy(metadata)
    _, root = run_get_node(structured_metadata.nested_two, x=2, y=3, code=code, task_metadata=metadata)
    assert root.is_finished_ok
    stored = root.base.links.get_incoming().get_node_by_label('task_metadata')
    assert stored.get_dict() == before
    assert metadata == before
    assert {child.label for child in root.called} == {'first', 'second'}


def test_duplicate_metadata_alias_same_scope_rejected() -> None:
    """Repeating a `CalcJob`-bearing graph alias in one scope guides toward distinct aliases."""
    with pytest.raises(GraphLanguageError, match='distinct Python aliases'):
        compile_graph(dup_nested)


def test_duplicate_calcjob_alias_same_scope_rejected() -> None:
    """Repeating a direct `CalcJob` alias in one scope guides toward distinct aliases."""
    with pytest.raises(GraphLanguageError, match='distinct Python aliases'):
        compile_graph(dup_calc)


def test_pure_duplicate_keeps_suffix() -> None:
    """Pure task-function duplicates keep numeric suffixes and need no metadata."""
    template = compile_graph(pure_twice)
    assert [placement.name for placement in template.tasks] == ['pure_add', 'pure_add_2']
