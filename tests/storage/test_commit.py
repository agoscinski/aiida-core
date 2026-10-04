###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
###########################################################################
"""Runtime SQL commit routing and enclosing transaction ownership."""

import pytest
from sqlalchemy import event, inspect

from aiida import orm


def test_runtime_commits_use_gateway(commit_backend, monkeypatch):
    """ORM writes, queries and explicit transactions share the commit policy."""
    backend = commit_backend
    engine = backend.get_session().get_bind()
    original = backend._commit
    inside_gateway = False
    commits = []

    def gateway():
        nonlocal inside_gateway
        inside_gateway = True
        try:
            original()
        finally:
            inside_gateway = False

    def committed(connection):
        assert inside_gateway
        commits.append(connection)

    monkeypatch.setattr(backend, '_commit', gateway)
    event.listen(engine, 'commit', committed)
    try:
        node = orm.Int(1).store()
        node.base.extras.set('test', True)
        group = orm.Group(label='commit-gateway').store()
        group.add_nodes(node)
        group.remove_nodes(node)
        group.add_nodes(node)
        group.clear()
        orm.QueryBuilder().append(orm.Int).all()
        with backend.transaction():
            node.base.extras.set('transaction', True)
        backend.groups.delete(group.pk)
        backend.nodes.delete(node.pk)
    finally:
        event.remove(engine, 'commit', committed)
    assert commits


def test_commit_gateway_finishes_transaction_and_expires_models(commit_backend):
    """Routing a commit preserves SQLAlchemy transaction release and expiration."""
    backend = commit_backend
    session = backend.get_session()
    user = backend.users.create(email='commit-lifecycle@example.org')
    model = user.bare_model
    session.add(model)
    backend._commit()
    assert not session.in_transaction()
    assert inspect(model).expired
    assert user.email == 'commit-lifecycle@example.org'


def test_commit_failure_cleans_up_transaction(commit_backend):
    """A failed transaction commit releases the transaction and rolls back writes."""
    backend = commit_backend
    node = orm.Int(1).store()
    session = backend.get_session()
    engine = session.get_bind()

    def fail(connection):
        msg = 'commit failed'
        raise RuntimeError(msg)

    event.listen(engine, 'commit', fail)
    try:
        with pytest.raises(RuntimeError, match='commit failed'):
            with backend.transaction():
                node.base.extras.set('temporary', True)
        assert not session.in_transaction()
    finally:
        event.remove(engine, 'commit', fail)
    assert 'temporary' not in node.base.extras.all


@pytest.mark.parametrize('operation', ['attributes', 'clear_group', 'delete_group', 'delete_node'])
def test_commit_gateway_preserves_outer_rollback(commit_backend, operation):
    """A runtime commit request cannot commit the caller's explicit transaction."""
    backend = commit_backend
    node = orm.Int(1).store()
    group = orm.Group(label='commit-rollback').store()
    group.add_nodes(node)
    node_pk = node.pk
    group_pk = group.pk
    commits = []
    engine = backend.get_session().get_bind()

    def committed(connection):
        commits.append(connection)

    event.listen(engine, 'commit', committed)
    try:
        with pytest.raises(RuntimeError, match='abort caller transaction'):
            with backend.transaction():
                if operation == 'attributes':
                    node.base.extras.set('uncommitted', True)
                elif operation == 'clear_group':
                    group.clear()
                elif operation == 'delete_group':
                    backend.groups.delete(group_pk)
                else:
                    backend.nodes.delete(node_pk)
                assert not commits
                msg = 'abort caller transaction'
                raise RuntimeError(msg)
        assert not commits
    finally:
        event.remove(engine, 'commit', committed)

    restored_node = orm.load_node(node_pk)
    restored_group = orm.load_group(group_pk)
    assert 'uncommitted' not in restored_node.base.extras.all
    assert [member.pk for member in restored_group.nodes] == [node_pk]
