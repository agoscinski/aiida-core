"""Unified Comment model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship

from .base import Base
from .types import GUID, TZDateTime
from aiida.common import timezone
from aiida.common.utils import get_new_uuid


class DbComment(Base):
    """Database model to store Comment entities.

    Comments allow users to attach text annotations to nodes. Each comment:
    - Is attached to a specific node
    - Has an author (user)
    - Records creation and modification times
    - Contains text content

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.comment.DbComment
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dbcomment'

    id = Column(Integer, primary_key=True)
    uuid = Column(GUID, default=get_new_uuid, nullable=False, unique=True)

    # Foreign keys
    dbnode_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True,
    )

    # Timestamps (timezone-aware on both PostgreSQL and SQLite)
    ctime = Column(TZDateTime, default=timezone.now, nullable=False)
    mtime = Column(TZDateTime, default=timezone.now, onupdate=timezone.now, nullable=False)

    # Comment content
    content = Column(Text, default='', nullable=False)

    # Relationships
    dbnode = relationship('DbNode', backref='dbcomments')
    user = relationship('DbUser')

    def __str__(self):
        """String representation."""
        return 'DbComment for [{} {}] on {}'.format(
            self.dbnode.get_simple_name(),
            self.dbnode.id,
            timezone.localtime(self.ctime).strftime('%Y-%m-%d')
        )

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbComment(id={self.id}, uuid={self.uuid}, "
            f"node_id={self.dbnode_id}, user_id={self.user_id})>"
        )
