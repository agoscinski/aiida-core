"""Unified AuthInfo model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, backref

from .base import Base
from .types import JSONType


class DbAuthInfo(Base):
    """Database model to store authentication information per user and computer.

    This model stores computer-specific authentication data and configuration
    for each user. It includes:
    - Authentication parameters (SSH keys, passwords, etc.)
    - Metadata for job submission configuration
    - An enabled/disabled flag to control computer access

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.models.authinfo.DbAuthInfo
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dbauthinfo'

    id = Column(Integer, primary_key=True)

    # Foreign keys
    aiidauser_id = Column(
        Integer,
        ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True,
    )
    dbcomputer_id = Column(
        Integer,
        ForeignKey('db_dbcomputer.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True,
    )

    # JSON fields
    # Use _metadata to avoid conflict with SQLAlchemy's reserved 'metadata' attribute
    _metadata = Column('metadata', JSONType, default=dict, nullable=False)
    auth_params = Column(JSONType, default=dict, nullable=False)

    # Status flag
    enabled = Column(Boolean, default=True, nullable=False)

    # Relationships
    aiidauser = relationship(
        'DbUser',
        backref=backref('authinfos', passive_deletes=True, cascade='all, delete')
    )
    dbcomputer = relationship(
        'DbComputer',
        backref=backref('authinfos', passive_deletes=True, cascade='all, delete')
    )

    # Unique constraint: one authinfo per (user, computer) pair
    __table_args__ = (UniqueConstraint('aiidauser_id', 'dbcomputer_id'),)

    def __str__(self):
        """String representation."""
        if self.enabled:
            return f'DB authorization info for {self.aiidauser.email} on {self.dbcomputer.label}'
        return f'DB authorization info for {self.aiidauser.email} on {self.dbcomputer.label} [DISABLED]'

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbAuthInfo(id={self.id}, user_id={self.aiidauser_id}, "
            f"computer_id={self.dbcomputer_id}, enabled={self.enabled})>"
        )
