###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Unit tests for the Comment ORM class."""

import pytest

from aiida import orm
from aiida.common import exceptions
from aiida.manage import get_manager
from aiida.orm.comments import Comment
from aiida.tools.graph.deletions import delete_nodes


@pytest.fixture
def node():
    """Return a stored node."""
    return orm.Data().store()


@pytest.fixture
def create_comment(node):
    """Create a comment with a node."""

    def factory(content=''):
        return Comment(node, content).store()

    return factory


def test_comment_content(create_comment):
    """Test getting and setting content of a Comment."""
    content = 'Be more constructive with your feedback'
    comment = create_comment(content)
    assert comment.content == content


def test_comment_mtime(create_comment):
    """Test getting and setting mtime of a Comment."""
    comment = create_comment()
    mtime = comment.mtime
    comment.set_content('Changing an attribute should automatically change the mtime')
    assert comment.content == 'Changing an attribute should automatically change the mtime'
    assert comment.mtime != mtime


def test_comment_node(node):
    """Test getting the node of a Comment."""
    comment = Comment(node, 'comment').store()
    assert comment.node.uuid == node.uuid


def test_comment_profile_uuid(node):
    """Test that a Comment is stamped with the owning profile UUID."""
    comment = Comment(node, 'comment').store()
    assert comment.profile_uuid == get_manager().get_profile().uuid


def test_comment_collection_get(create_comment):
    """Test retrieving a Comment through the collection."""
    comment = create_comment()
    loaded = Comment.collection.get(id=comment.pk)
    assert loaded.uuid == comment.uuid


@pytest.mark.usefixtures('aiida_profile_clean')
def test_comment_collection_delete(node):
    """Test deleting a Comment through the collection."""
    comment = Comment(node, 'I will perish').store()
    comment_pk = comment.pk

    Comment.collection.delete(comment.pk)

    with pytest.raises(exceptions.NotExistent):
        Comment.collection.delete(comment_pk)

    with pytest.raises(exceptions.NotExistent):
        Comment.collection.get(id=comment_pk)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_comment_collection_delete_all(node):
    """Test deleting all Comments through the collection."""
    comment = Comment(node, 'I will perish').store()
    Comment(node, 'Surely not?').store()
    comment_pk = comment.pk

    # Assert the comments exist
    assert len(Comment.collection.all()) == 2

    # Delete all Comments
    Comment.collection.delete_all()

    with pytest.raises(exceptions.NotExistent):
        Comment.collection.delete(comment_pk)

    with pytest.raises(exceptions.NotExistent):
        Comment.collection.get(id=comment_pk)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_comment_collection_delete_many(node):
    """Test deleting many Comments through the collection."""
    comment_one = Comment(node, 'I will perish').store()
    comment_two = Comment(node, 'Surely not?').store()
    comment_ids = [_.pk for _ in [comment_one, comment_two]]

    # Assert the Comments exist
    assert len(Comment.collection.all()) == 2

    # Delete new Comments using filter
    filters = {'id': {'in': comment_ids}}
    Comment.collection.delete_many(filters=filters)

    builder = orm.QueryBuilder().append(Comment, project='id')
    assert builder.count() == 0

    for comment_pk in comment_ids:
        with pytest.raises(exceptions.NotExistent):
            Comment.collection.delete(comment_pk)

        with pytest.raises(exceptions.NotExistent):
            Comment.collection.get(id=comment_pk)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_comment_querybuilder():
    """Test querying for comments by joining on nodes in the QueryBuilder."""
    node_one = orm.Data().store()
    comment_one = Comment(node_one, 'comment_one').store()

    node_two = orm.Data().store()
    comment_two = Comment(node_two, 'comment_two').store()
    comment_three = Comment(node_two, 'comment_three').store()

    node_three = orm.CalculationNode().store()
    comment_four = Comment(node_three, 'calc_comment').store()

    # Retrieve a node by joining on a specific comment
    builder = orm.QueryBuilder()
    builder.append(Comment, tag='comment', filters={'id': comment_one.pk})
    builder.append(orm.Node, with_comment='comment', project=['uuid'])
    nodes = builder.all()

    assert len(nodes) == 1
    for query_node in nodes:
        assert str(query_node[0]) in [node_one.uuid]

    # Retrieve a comment by joining on a specific node
    builder = orm.QueryBuilder()
    builder.append(orm.Node, tag='node', filters={'id': node_two.pk})
    builder.append(Comment, with_node='node', project=['uuid'])
    comments = builder.all()

    assert len(comments) == 2
    for comment in comments:
        assert str(comment[0]) in [comment_two.uuid, comment_three.uuid]

    # Retrieve comments filtered by profile UUID
    profile_uuid = get_manager().get_profile().uuid
    builder = orm.QueryBuilder()
    builder.append(Comment, filters={'profile_uuid': profile_uuid}, project=['uuid'])
    comments = builder.all()

    assert len(comments) == 4
    for comment in comments:
        assert str(comment[0]) in [comment_one.uuid, comment_two.uuid, comment_three.uuid, comment_four.uuid]


def test_objects_get(node):
    """Test getting a comment from the collection"""
    comment = node.base.comments.add('Check out the comment on _this_ one')
    gotten_comment = Comment.collection.get(id=comment.pk)
    assert isinstance(gotten_comment, Comment)


@pytest.mark.usefixtures('aiida_profile_clean')
def test_delete_node_with_comments(node):
    """Test deleting a node with comments."""
    Comment(node, 'I will perish').store()
    assert len(Comment.collection.all()) == 1
    delete_nodes([node.pk], dry_run=False)
    assert len(Comment.collection.all()) == 0
