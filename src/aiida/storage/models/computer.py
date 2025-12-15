"""Unified Computer model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, String, Text, Index

from .base import Base
from .types import GUID, JSONType
from aiida.common.utils import get_new_uuid


class DbComputer(Base):
    """Database model to store data for Computer entities.

    A computer represents a computational resource where calculations can be run.
    This includes both local machines and remote HPC clusters.

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.computer.DbComputer
    - aiida.storage.sqlite_zip.models.DbComputer
    """

    __tablename__ = 'db_dbcomputer'

    id = Column(Integer, primary_key=True)
    uuid = Column(GUID, default=get_new_uuid, nullable=False, unique=True)
    label = Column(String(255), nullable=False, unique=True, index=True)
    hostname = Column(String(255), nullable=False)
    description = Column(Text, nullable=False, default='')
    scheduler_type = Column(String(255), nullable=False)
    transport_type = Column(String(255), nullable=False)

    # JSON metadata for computer configuration
    # Use _metadata to avoid conflict with SQLAlchemy's reserved 'metadata' attribute
    _metadata = Column('metadata', JSONType, default=dict, nullable=False)

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations
    # They use varchar_pattern_ops which is PostgreSQL-only and should not be in ORM models

    def __str__(self):
        """String representation."""
        return f'{self.label} ({self.hostname})'

    def __repr__(self):
        """Developer-friendly representation."""
        return f"<DbComputer(id={self.id}, label='{self.label}', hostname='{self.hostname}')>"
