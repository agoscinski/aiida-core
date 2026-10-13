###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
###########################################################################
"""Storage-owned deferred metadata, visibility, isolation and failure handling."""

import asyncio

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from aiida import orm
from aiida.common import exceptions


def test_metadata_waits_for_boundary(commit_backend):
    """Repeated setters remain local until one atomic persistence boundary."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    model_class = type(node.backend_entity.bare_model)
    pk = node.pk
    session = backend.get_session()
    engine = session.get_bind()
    commits = []

    def committed(connection):
        commits.append(connection)

    event.listen(engine, 'commit', committed)
    try:
        with backend._deferred_writes(node.backend_entity):
            node.set_process_status('preparing')
            node.base.attributes.set_many({'process_status': 'waiting', 'exit_message': 'pending'})
            node.base.attributes.delete_many(['exit_message'])
            node.label = 'pending'
            assert node.process_status == 'waiting'
            assert 'exit_message' not in node.base.attributes.all
            assert node.label == 'pending'
            assert not session.in_transaction()
            assert not session.dirty
            assert not commits
            with Session(engine) as external:
                persisted = external.get(model_class, pk)
                assert 'process_status' not in persisted.attributes
                assert persisted.label == ''
            with backend._flush_deferred_writes():
                node.set_exit_status(0)
            assert len(commits) == 1
            assert not session.in_transaction()
    finally:
        event.remove(engine, 'commit', committed)
    with Session(engine) as external:
        persisted = external.get(model_class, pk)
        assert persisted.attributes['process_status'] == 'waiting'
        assert persisted.attributes['exit_status'] == 0
        assert 'exit_message' not in persisted.attributes
        assert persisted.label == 'pending'


def test_unrelated_writes_remain_immediate(commit_backend):
    """Another node's writes do not commit or discard the current node's buffer."""
    backend = commit_backend
    owner = orm.CalculationNode().store()
    other = orm.CalculationNode().store()
    owner_pk, other_pk = owner.pk, other.pk
    model = type(owner.backend_entity.bare_model)
    with backend._deferred_writes(owner.backend_entity):
        owner.set_process_status('pending')
        other.set_process_status('immediate')
        with Session(backend.get_session().get_bind()) as external:
            assert 'process_status' not in external.get(model, owner_pk).attributes
            assert external.get(model, other_pk).attributes['process_status'] == 'immediate'
        assert owner.process_status == 'pending'
        with backend._flush_deferred_writes():
            pass
    assert owner.process_status == 'pending'


def test_extras_remain_immediate_for_live_coordination(commit_backend):
    """Plugins can publish a live marker without flushing pending execution metadata."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    pk = node.pk
    model = type(node.backend_entity.bare_model)
    with backend._deferred_writes(node.backend_entity):
        node.set_process_status('pending')
        node.base.extras.set('monitor_called', True)
        with Session(backend.get_session().get_bind()) as external:
            persisted = external.get(model, pk)
            assert persisted.extras['monitor_called'] is True
            assert 'process_status' not in persisted.attributes
        with backend._flush_deferred_writes():
            pass
    assert node.process_status == 'pending'


@pytest.mark.parametrize('failure', [RuntimeError, asyncio.CancelledError])
def test_scope_failure_discards_pending_writes(commit_backend, failure):
    """Cancellation/failure cannot leave metadata for a later unrelated commit."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    with pytest.raises(failure, match='abort execution'):
        with backend._deferred_writes(node.backend_entity):
            node.set_process_status('discarded')
            msg = 'abort execution'
            raise failure(msg)
    node.base.extras.set('immediate', True)
    assert node.process_status is None
    assert node.base.extras.get('immediate') is True


def test_other_execution_boundary_does_not_flush_owner(commit_backend):
    """Child initialization must not commit or discard its parent's buffer."""
    backend = commit_backend
    owner = orm.CalculationNode().store()
    other = orm.CalculationNode().store()
    owner_pk = owner.pk
    model = type(owner.backend_entity.bare_model)
    with backend._deferred_writes(owner.backend_entity):
        owner.set_process_status('pending')
        with backend._flush_deferred_writes(other.backend_entity):
            other.set_process_status('child')
        with Session(backend.get_session().get_bind()) as external:
            assert 'process_status' not in external.get(model, owner_pk).attributes
        assert owner.process_status == 'pending'
        with backend._flush_deferred_writes(owner.backend_entity):
            pass
    assert owner.process_status == 'pending'


def test_external_changes_are_preserved(commit_backend):
    """Flush merges changed JSON keys instead of replacing a stale whole column."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    pk = node.pk
    model = type(node.backend_entity.bare_model)
    with backend._deferred_writes(node.backend_entity):
        node.set_process_status('waiting')
        with Session(backend.get_session().get_bind()) as external:
            persisted = external.get(model, pk)
            persisted.attributes = {**persisted.attributes, 'paused': True}
            external.commit()
        with backend._flush_deferred_writes():
            pass
    assert node.paused
    assert node.process_status == 'waiting'


def test_overlapping_external_change_rejected(commit_backend):
    """A stale decision on the same field must not overwrite an external writer."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    pk = node.pk
    model = type(node.backend_entity.bare_model)
    with backend._deferred_writes(node.backend_entity):
        node.set_process_status('local')
        with Session(backend.get_session().get_bind()) as external:
            persisted = external.get(model, pk)
            persisted.attributes = {**persisted.attributes, 'process_status': 'external'}
            external.commit()
        with pytest.raises(exceptions.InvalidOperation, match='Concurrent update'):
            with backend._flush_deferred_writes():
                pass
    assert node.process_status == 'external'


def test_concurrent_batches_are_isolated(commit_backend):
    """Tasks sharing one SQL session cannot flush each other's pending metadata."""
    backend = commit_backend
    first = orm.CalculationNode().store()
    second = orm.CalculationNode().store()
    model = type(first.backend_entity.bare_model)
    first_pk, second_pk = first.pk, second.pk

    async def run():
        staged = asyncio.Event()
        flushed = asyncio.Event()

        async def one():
            with backend._deferred_writes(first.backend_entity):
                first.set_process_status('first')
                await staged.wait()
                with backend._flush_deferred_writes():
                    pass
                flushed.set()

        async def two():
            with backend._deferred_writes(second.backend_entity):
                second.set_process_status('second')
                staged.set()
                await flushed.wait()
                with Session(backend.get_session().get_bind()) as external:
                    assert external.get(model, first_pk).attributes['process_status'] == 'first'
                    assert 'process_status' not in external.get(model, second_pk).attributes
                assert second.process_status == 'second'
                with backend._flush_deferred_writes():
                    pass

        await asyncio.gather(one(), two())

    asyncio.run(run())
    assert first.process_status == 'first'
    assert second.process_status == 'second'


def test_control_boundary_flushes_active_execution(commit_backend):
    """A separate control task can flush the targeted node before updating it."""
    backend = commit_backend
    node = orm.CalculationNode().store()

    async def run():
        staged = asyncio.Event()
        controlled = asyncio.Event()

        async def execution():
            with backend._deferred_writes(node.backend_entity):
                node.set_process_status('running')
                staged.set()
                await controlled.wait()
                assert node.process_status == 'controlled'

        async def control():
            await staged.wait()
            with backend._flush_deferred_writes(node.backend_entity):
                node.set_process_status('controlled')
            controlled.set()

        await asyncio.gather(execution(), control())

    asyncio.run(run())
    assert node.process_status == 'controlled'


def test_inherited_context_expires_with_scope(commit_backend):
    """A task that outlives its parent's scope must return to immediate writes."""
    backend = commit_backend
    node = orm.CalculationNode().store()

    async def run():
        ready = asyncio.Event()

        async def later():
            await ready.wait()
            node.set_process_status('later')

        with backend._deferred_writes(node.backend_entity):
            task = asyncio.create_task(later())
        ready.set()
        await task

    asyncio.run(run())
    assert node.process_status == 'later'


def test_explicit_transaction_rejected_without_losing_pending_writes(commit_backend):
    """A caller transaction cannot silently take ownership of a deferred update."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    with backend._deferred_writes(node.backend_entity):
        node.set_process_status('pending')
        with pytest.raises(exceptions.InvalidOperation, match='explicit caller transaction'):
            with backend.transaction():
                node.set_process_status('invalid')
        assert node.process_status == 'pending'
        with backend._flush_deferred_writes():
            pass
    assert node.process_status == 'pending'


def test_failed_flush_discards_batch(commit_backend):
    """A failed commit rolls back the batch without replaying it automatically."""
    backend = commit_backend
    node = orm.CalculationNode().store()
    engine = backend.get_session().get_bind()

    def fail(connection):
        msg = 'commit failed'
        raise RuntimeError(msg)

    with backend._deferred_writes(node.backend_entity):
        node.set_process_status('temporary')
        event.listen(engine, 'commit', fail)
        try:
            with pytest.raises(RuntimeError, match='commit failed'):
                with backend._flush_deferred_writes():
                    pass
        finally:
            event.remove(engine, 'commit', fail)
        assert not backend.get_session().in_transaction()
    assert node.process_status is None
