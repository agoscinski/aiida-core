###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Comment objects and functions"""

from __future__ import annotations

import typing as t
from datetime import datetime
from uuid import UUID

from aiida.manage import get_manager
from aiida.orm import entities
from aiida.orm.pydantic import OrmMetadataField

if t.TYPE_CHECKING:
    from aiida.orm.implementation import BackendComment, BackendNode, StorageBackend
    from aiida.orm.nodes.node import Node

__all__ = ('Comment',)


class CommentCollection(entities.Collection['Comment']):
    """The collection of Comment entries."""

    collection_type: t.ClassVar[str] = 'comments'

    @staticmethod
    def _entity_base_cls() -> type[Comment]:
        return Comment

    def delete(self, pk: int) -> None:
        """Remove a Comment from the collection with the given id

        :param pk: the id of the comment to delete

        :raises TypeError: if ``comment_id`` is not an `int`
        :raises `~aiida.common.exceptions.NotExistent`: if Comment with ID ``comment_id`` is not found
        """
        self._backend.comments.delete(pk)

    def delete_all(self) -> None:
        """Delete all Comments from the Collection

        :raises `~aiida.common.exceptions.IntegrityError`: if all Comments could not be deleted
        """
        self._backend.comments.delete_all()

    def delete_many(self, filters: dict) -> list[int]:
        """Delete Comments from the Collection based on ``filters``

        :param filters: similar to QueryBuilder filter

        :return: (former) ``PK`` s of deleted Comments

        :raises TypeError: if ``filters`` is not a `dict`
        :raises `~aiida.common.exceptions.ValidationError`: if ``filters`` is empty
        """
        return self._backend.comments.delete_many(filters)


class Comment(entities.Entity['BackendComment', CommentCollection]):
    """Base class to map a DbComment that represents a comment attached to a certain Node."""

    _CLS_COLLECTION = CommentCollection

    identity_field = 'uuid'

    class ReadModel(entities.Entity.ReadModel):
        uuid: UUID = OrmMetadataField(
            description='The UUID of the comment',
            read_only=True,
            examples=['123e4567-e89b-12d3-a456-426614174000'],
        )
        ctime: datetime = OrmMetadataField(
            description='Creation time of the comment',
            read_only=True,
            examples=['2024-01-01T12:00:00+00:00'],
        )
        mtime: datetime = OrmMetadataField(
            description='Modified time of the comment',
            read_only=True,
            examples=['2024-01-02T12:00:00+00:00'],
        )
        node: int = OrmMetadataField(
            description='Node PK that the comment is attached to',
            orm_class='core.node',
            orm_to_model=lambda comment: t.cast(Comment, comment).node.pk,
            examples=[42],
        )
        profile_uuid: str = OrmMetadataField(
            description='UUID of the profile that owns the comment',
            orm_to_model=lambda comment: t.cast(Comment, comment).profile_uuid,
            read_only=True,
        )
        content: str = OrmMetadataField(
            description='Content of the comment',
            examples=['This is a comment.'],
        )

    def __init__(self, node: Node, content: str | None = None, backend: StorageBackend | None = None):
        """Create a Comment for a given node

        :param node: a Node instance
        :param content: the comment content
        :param backend: the backend to use for the instance, or use the default backend if None

        :return: a Comment object associated to the given node
        """
        backend = backend or get_manager().get_profile_storage()
        model = backend.comments.create(node=node.backend_entity, content=content)
        super().__init__(model)

    def __str__(self) -> str:
        arguments = [self.uuid, self.node.pk, self.content]
        return 'Comment<{}> for node<{}>: {}'.format(*arguments)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Comment):
            return False

        return self.uuid == other.uuid

    @property
    def uuid(self) -> str:
        """Return the UUID for this comment.

        This identifier is unique across all entities types and backend instances.

        :return: the entity uuid
        """
        return self._backend_entity.uuid

    @property
    def ctime(self) -> datetime:
        return self._backend_entity.ctime

    @property
    def mtime(self) -> datetime:
        return self._backend_entity.mtime

    def set_mtime(self, value: datetime) -> None:
        return self._backend_entity.set_mtime(value)

    @property
    def node(self) -> BackendNode:
        return self._backend_entity.node

    @property
    def profile_uuid(self) -> str:
        return self._backend_entity.profile_uuid

    @property
    def content(self) -> str:
        return self._backend_entity.content

    def set_content(self, value: str) -> None:
        return self._backend_entity.set_content(value)
