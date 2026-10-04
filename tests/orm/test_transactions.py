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
