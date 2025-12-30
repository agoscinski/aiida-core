"""Database-agnostic type system for AiiDA.

This module provides SQLAlchemy TypeDecorator classes that automatically adapt
to different database backends (PostgreSQL vs SQLite) without requiring separate
model definitions.

Key Types:
- GUID: Platform-independent UUID type
- TZDateTime: Timezone-aware datetime for all databases
- JSONType: JSONB (PostgreSQL) or JSON (SQLite)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import String, TypeDecorator, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.dialects.sqlite import JSON


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    PostgreSQL: Uses native UUID type
    SQLite: Uses String(32) to store UUID as hex string (no dashes)

    Usage:
        uuid_col = Column(GUID, default=uuid.uuid4, nullable=False)

    The type automatically handles conversion between Python's uuid.UUID objects
    and the appropriate database representation. Queries accept both dashed and hex
    formats, which are normalized to hex for SQLite comparison.

    Storage format: hex (32 chars) - e.g., 'a0eebc999c0b4ef8bb6d6bb9bd380a11'
    Query format: accepts both dashed and hex - normalized to hex
    Return format: dashed string (36 chars) - e.g., 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'

    Examples:
        # PostgreSQL: stored as native UUID
        # SQLite: stored as 'a0eebc999c0b4ef8bb6d6bb9bd380a11'
        # Python: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    """

    impl = String(32)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """Load the dialect-specific implementation.

        :param dialect: SQLAlchemy dialect (postgresql, sqlite, etc.)
        :return: Dialect-specific type descriptor
        """
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID(as_uuid=True))
        else:
            # SQLite: use VARCHAR(32) for hex string storage
            return dialect.type_descriptor(String(32))

    def process_bind_param(self, value: Optional[uuid.UUID | str], dialect) -> Optional[uuid.UUID | str]:
        """Process value going INTO the database.

        Normalizes incoming UUIDs to appropriate format for each backend.
        Accepts both dashed and hex UUID strings for SQLite queries.

        :param value: Python uuid.UUID object or string (dashed or hex)
        :param dialect: SQLAlchemy dialect
        :return: Database-appropriate representation
        """
        if value is None:
            return value

        # Convert to UUID object first (handles both dashed and hex inputs)
        if isinstance(value, uuid.UUID):
            uuid_obj = value
        elif isinstance(value, str):
            try:
                # Try parsing as standard UUID (with or without dashes)
                uuid_obj = uuid.UUID(value)
            except ValueError:
                # Invalid UUID - pass through (will fail comparison)
                return value
        else:
            return str(value)

        if dialect.name == 'postgresql':
            # PostgreSQL uses native UUID type
            return uuid_obj
        else:
            # SQLite: convert to hex (32 chars, no dashes)
            return uuid_obj.hex

    def process_result_value(self, value: Optional[str | uuid.UUID], dialect) -> Optional[str]:
        """Process value coming FROM the database.

        Converts database format to Python dashed string format.

        :param value: Database value (UUID object for PostgreSQL, hex string for SQLite)
        :param dialect: SQLAlchemy dialect
        :return: UUID string in dashed format
        """
        if value is None:
            return value

        if dialect.name == 'postgresql':
            # PostgreSQL returns UUID object - convert to dashed string
            return str(value)
        else:
            # SQLite: hex string -> UUID object -> dashed string
            if isinstance(value, str) and len(value) == 32:
                # Convert hex to dashed format
                return str(uuid.UUID(hex=value))
            return value


class TZDateTime(TypeDecorator):
    """Timezone-aware DateTime for all databases.

    PostgreSQL: Uses TIMESTAMP WITH TIME ZONE
    SQLite: Uses TEXT in ISO8601 format, always stored as UTC

    Usage:
        created = Column(TZDateTime, default=lambda: datetime.now(timezone.utc))

    The type ensures all datetimes are timezone-aware and consistently handles
    timezone conversions across different database backends.

    Examples:
        # PostgreSQL: '2024-12-14 10:30:00+00:00' (native TZ support)
        # SQLite: '2024-12-14 10:30:00.000000' (UTC, TZ stripped)
        # Python: datetime(2024, 12, 14, 10, 30, 0, tzinfo=timezone.utc)
    """

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """Load the dialect-specific implementation.

        :param dialect: SQLAlchemy dialect
        :return: Dialect-specific type descriptor
        """
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(DateTime(timezone=True))
        else:
            # SQLite doesn't have native timezone support
            return dialect.type_descriptor(DateTime())

    def process_bind_param(self, value: Optional[datetime], dialect) -> Optional[datetime]:
        """Process value going INTO the database.

        Ensures timezone awareness and converts to appropriate format.

        :param value: Python datetime object
        :param dialect: SQLAlchemy dialect
        :return: Database-appropriate datetime
        """
        if value is None:
            return value

        # Ensure timezone-aware (assume UTC if naive)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        if dialect.name == 'postgresql':
            # PostgreSQL handles timezone natively
            return value
        else:
            # SQLite: convert to UTC and strip timezone info
            # (stored as naive UTC datetime in ISO8601 format)
            return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value: Optional[datetime], dialect) -> Optional[datetime]:
        """Process value coming FROM the database.

        Ensures returned datetime is always timezone-aware UTC.

        :param value: Database datetime value
        :param dialect: SQLAlchemy dialect
        :return: Timezone-aware Python datetime
        """
        if value is None:
            return value

        if dialect.name == 'postgresql':
            # PostgreSQL returns timezone-aware datetime
            return value
        else:
            # SQLite: add UTC timezone to naive datetime
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value


class JSONType(TypeDecorator):
    """Platform-independent JSON type.

    PostgreSQL: Uses JSONB for better performance and indexing
    SQLite: Uses JSON (stored as TEXT with JSON validation)

    Usage:
        attributes = Column(JSONType, default=dict, nullable=False)

    The type handles JSON serialization/deserialization automatically and
    uses the optimal JSON type for each database.

    Supports JSON indexing operations like column['key'] for querying nested data.

    Examples:
        # PostgreSQL: Binary JSON with indexing support
        # SQLite: TEXT with JSON functions
        # Python: dict, list, or any JSON-serializable object
    """

    impl = JSONB  # Use JSONB as base to get its comparator
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """Load the dialect-specific implementation.

        :param dialect: SQLAlchemy dialect
        :return: Dialect-specific type descriptor
        """
        if dialect.name == 'postgresql':
            # PostgreSQL: use JSONB for better performance
            return dialect.type_descriptor(JSONB)
        elif dialect.name == 'sqlite':
            # SQLite: use JSON type (requires SQLite 3.9+)
            return dialect.type_descriptor(JSON)
        else:
            # Other databases: use Text as fallback
            # (JSON will be serialized as string)
            return dialect.type_descriptor(String)

    def process_bind_param(self, value: Any, dialect) -> Any:
        """Process value going INTO the database.

        SQLAlchemy handles JSON serialization automatically for JSON/JSONB types.

        :param value: Python object (dict, list, etc.)
        :param dialect: SQLAlchemy dialect
        :return: Database-appropriate value
        """
        # SQLAlchemy's JSON types handle serialization automatically
        return value

    def process_result_value(self, value: Any, dialect) -> Any:
        """Process value coming FROM the database.

        SQLAlchemy handles JSON deserialization automatically for JSON/JSONB types.

        :param value: Database value
        :param dialect: SQLAlchemy dialect
        :return: Python object
        """
        # SQLAlchemy's JSON types handle deserialization automatically
        return value


# Type hints for better IDE support
UUIDType = GUID
DateTimeType = TZDateTime
