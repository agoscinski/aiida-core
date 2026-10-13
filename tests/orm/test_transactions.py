###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Transaction contracts shared by PostgreSQL and SQLite DOS storage."""

import pytest
from sqlalchemy import event

from aiida import orm
from aiida.common import exceptions
from aiida.common.links import LinkType


def test_nested_transaction_does_not_commit(backend):
    """An inner successful savepoint must not commit its caller's transaction."""
    node = orm.Int(1).store()
    original = node.base.extras.all
    with pytest.raises(RuntimeError, match='abort outer'):
        with backend.transaction():
            node.base.extras.set('outer', True)
            with backend.transaction():
                node.base.extras.set('inner', True)
            raise RuntimeError('abort outer')
    assert node.base.extras.all == original


def test_stored_link_respects_transaction(backend):
    """Attaching an already-stored output must respect the enclosing transaction."""
    process = orm.WorkflowNode().store()
    output = orm.Int(1).store()
    with pytest.raises(RuntimeError, match='abort links'):
        with backend.transaction():
            output.base.links.add_incoming(process, LinkType.RETURN, 'result')
            raise RuntimeError('abort links')
    assert process.base.links.get_outgoing().all() == []


def test_failed_savepoint_preserves_outer_writes(backend):
    """An integrity failure in a savepoint must not roll back its caller's writes."""
    user = orm.User(email='duplicate@example.org').store()
    node = orm.Int(1).store()
    original = node.base.extras.all
    with backend.transaction():
        node.base.extras.set('before', True)
        with pytest.raises(exceptions.IntegrityError):
            with backend.transaction():
                orm.User(email=user.email).store()
        node.base.extras.set('after', True)
    assert node.base.extras.all == {**original, 'before': True, 'after': True}


def test_one_durable_commit_for_nested_writes(backend):
    """Only the outermost context commits, including links to stored outputs."""
    process = orm.WorkflowNode().store()
    output = orm.Int(1).store()
    original = process.base.extras.all
    commits = []
    engine = backend.get_session().get_bind()

    def committed(connection):
        commits.append(connection)

    event.listen(engine, 'commit', committed)
    try:
        with backend.transaction():
            process.base.extras.set('before', True)
            with backend.transaction():
                output.base.links.add_incoming(process, LinkType.RETURN, 'result')
                process.base.extras.set('after', True)
    finally:
        event.remove(engine, 'commit', committed)
    assert len(commits) == 1
    assert process.base.extras.all == {**original, 'before': True, 'after': True}
    assert process.outputs.result.pk == output.pk


@pytest.mark.parametrize('error', [RuntimeError, KeyboardInterrupt])
def test_unstored_node_scope_restores_local_state(backend, error):
    """Rollback restores identity, adapters, links and the mutable repository."""
    import io

    process = orm.CalculationNode().store()
    output = orm.Int(7)
    output.base.repository.put_object_from_filelike(io.BytesIO(b'original'), 'result.txt')
    output.base.links.add_incoming(process, LinkType.CREATE, 'result')
    attributes = output.base.attributes
    extras = output.base.extras
    original_uuid = output.uuid
    original_attributes = dict(attributes.all)
    original_extras = dict(extras.all)

    with pytest.raises(error):
        with output._store_scope():
            with backend.transaction():
                output.store()
                assert output.is_stored
                assert process.outputs.result.pk == output.pk
                raise error()

    assert not output.is_stored
    assert output.pk is None
    assert output.uuid == original_uuid
    assert attributes.all == original_attributes
    assert extras.all == original_extras
    assert output.base.repository.get_object_content('result.txt', mode='rb') == b'original'
    assert process.base.links.get_outgoing().all() == []
    assert orm.QueryBuilder().append(orm.Int, filters={'uuid': original_uuid}).count() == 0

    attributes.set('after_rollback', True)
    output.base.repository.put_object_from_filelike(io.BytesIO(b'new'), 'new.txt')
    with output._store_scope():
        with backend.transaction():
            output.store()
    reloaded = orm.load_node(output.pk)
    assert reloaded.uuid == original_uuid
    assert reloaded.base.attributes.get('after_rollback') is True
    assert reloaded.base.repository.get_object_content('new.txt', mode='rb') == b'new'
    assert process.outputs.result.pk == output.pk


def test_commit_failure_rolls_back_savepoints(backend):
    """Releasing a savepoint is not an implicit durable commit on SQLite."""
    node = orm.Int(1).store()
    original = node.base.extras.all
    engine = backend.get_session().get_bind()

    def fail(connection):
        raise RuntimeError('commit failed')

    event.listen(engine, 'commit', fail)
    try:
        with pytest.raises(RuntimeError, match='commit failed'):
            with backend.transaction():
                node.base.extras.set('temporary', True)
    finally:
        event.remove(engine, 'commit', fail)
    assert node.base.extras.all == original


def test_stored_node_scope_does_not_reset_identity(backend):
    """Existing nodes rely on SQLAlchemy's expiry, not unstored-node restoration."""
    output = orm.Int(1).store()
    original_pk = output.pk
    with pytest.raises(RuntimeError):
        with output._store_scope():
            with backend.transaction():
                output.base.extras.set('temporary', True)
                raise RuntimeError()
    assert output.pk == original_pk
    assert 'temporary' not in output.base.extras.all
