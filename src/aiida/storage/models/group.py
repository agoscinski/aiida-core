"""Unified Group models for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship, backref

from .base import Base
from .types import GUID, TZDateTime, JSONType
from aiida.common import timezone
from aiida.common.utils import get_new_uuid


class DbGroupNode(Base):
    """Database model to store many-to-many relationships between groups and nodes.

    This is a join table that establishes the many-to-many relationship:
    - One group can contain many nodes
    - One node can belong to many groups

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.group.DbGroupNode
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dbgroup_dbnodes'

    id = Column(Integer, primary_key=True)

    # Foreign keys to node and group
    dbnode_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True
    )
    dbgroup_id = Column(
        Integer,
        ForeignKey('db_dbgroup.id', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True
    )

    # Unique constraint: each (group, node) pair can only exist once
    __table_args__ = (UniqueConstraint('dbgroup_id', 'dbnode_id'),)


# Export table for use in relationships
table_groups_nodes = DbGroupNode.__table__


class DbGroup(Base):
    """Database model to store Group entities.

    Groups are named collections of nodes that users can organize for various purposes:
    - Organizing calculation workflows
    - Marking nodes for special processing
    - Categorizing data for analysis

    Key features:
    - label: Human-readable group name
    - type_string: Identifies the group plugin/purpose
    - extras: JSON field for arbitrary metadata
    - Unique constraint on (label, type_string) combination

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.group.DbGroup
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dbgroup'

    id = Column(Integer, primary_key=True)
    uuid = Column(GUID, default=get_new_uuid, nullable=False, unique=True)

    # Group identification
    label = Column(String(255), nullable=False, index=True)
    type_string = Column(String(255), default='', nullable=False, index=True)

    # Metadata
    time = Column(TZDateTime, default=timezone.now, nullable=False)
    description = Column(Text, default='', nullable=False)
    extras = Column(JSONType, default=dict, nullable=False)

    # Owner
    user_id = Column(
        Integer,
        ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True,
    )

    # Relationships
    user = relationship('DbUser', backref=backref('dbgroups', cascade='merge'))
    dbnodes = relationship(
        'DbNode',
        secondary=table_groups_nodes,
        backref='dbgroups',
        lazy='dynamic'
    )

    # Constraints
    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations
    __table_args__ = (
        UniqueConstraint('label', 'type_string'),
    )

    @property
    def pk(self):
        """Alias for id (primary key)."""
        return self.id

    def __str__(self):
        """String representation."""
        return f'<DbGroup [type: {self.type_string}] "{self.label}">'

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbGroup(id={self.id}, uuid={self.uuid}, "
            f"label='{self.label}', type='{self.type_string}')>"
        )
