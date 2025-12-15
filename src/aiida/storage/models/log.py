"""Unified Log model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Index
from sqlalchemy.orm import relationship, backref

from .base import Base
from .types import GUID, TZDateTime, JSONType
from aiida.common import timezone
from aiida.common.utils import get_new_uuid


class DbLog(Base):
    """Database model to store Log entries for ProcessNodes.

    Logs record runtime information about process execution:
    - Timestamps of events
    - Log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - Logger names (which component generated the log)
    - Messages with optional metadata

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.log.DbLog
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dblog'

    id = Column(Integer, primary_key=True)
    uuid = Column(GUID, default=get_new_uuid, nullable=False, unique=True)

    # Timestamp
    time = Column(TZDateTime, default=timezone.now, nullable=False)

    # Log identification
    loggername = Column(
        String(255),
        nullable=False,
        index=True,
        doc='What process recorded the message'
    )
    levelname = Column(
        String(50),
        nullable=False,
        index=True,
        doc='How critical the message is'
    )

    # Foreign key to node
    dbnode_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )

    # Log content
    message = Column(Text(), default='', nullable=False)

    # Additional metadata (JSON)
    # Use _metadata to avoid conflict with SQLAlchemy's reserved 'metadata' attribute
    _metadata = Column('metadata', JSONType, default=dict, nullable=False)

    # Relationship
    dbnode = relationship(
        'DbNode',
        backref=backref('dblogs', passive_deletes='all', cascade='merge')
    )

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations

    def __str__(self):
        """String representation."""
        return f'DbLog: {self.levelname} for node {self.dbnode.id}: {self.message}'

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbLog(id={self.id}, uuid={self.uuid}, "
            f"level='{self.levelname}', node_id={self.dbnode_id})>"
        )
