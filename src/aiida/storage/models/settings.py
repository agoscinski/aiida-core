"""Unified Setting model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, String, Text, Index

from .base import Base
from .types import TZDateTime, JSONType
from aiida.common import timezone


class DbSetting(Base):
    """Database model to store global backend settings.

    Settings are key-value pairs stored in the database that configure
    backend behavior. Each setting:
    - Has a unique key (string identifier)
    - Stores a value as JSON (can be any JSON-serializable type)
    - Includes an optional description
    - Tracks last modification time

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.settings.DbSetting
    - Dynamic creation in aiida.storage.sqlite_zip.models
    """

    __tablename__ = 'db_dbsetting'

    id = Column(Integer, primary_key=True)

    # Setting key (unique identifier)
    key = Column(String(1024), nullable=False, unique=True)

    # Setting value (stored as JSON)
    val = Column(JSONType, default={})

    # Metadata
    description = Column(Text, default='', nullable=False)
    time = Column(
        TZDateTime,
        default=timezone.now,
        onupdate=timezone.now,
        nullable=False
    )

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations

    def __str__(self):
        """String representation."""
        return f"'{self.key}'={self.val}"

    def __repr__(self):
        """Developer-friendly representation."""
        return f"<DbSetting(id={self.id}, key='{self.key}')>"
