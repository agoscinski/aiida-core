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
    SQLite: Uses String to store UUID as text

    Usage:
        uuid_col = Column(GUID, default=uuid.uuid4, nullable=False)

    The type automatically handles conversion between Python's uuid.UUID objects
    and the appropriate database representation. On SQLite, UUIDs are stored as
    strings to allow flexible comparison with different UUID formats (with or without dashes).

    Examples:
        # PostgreSQL: native UUID type
        # SQLite: TEXT (flexible string comparison)
        # Python: str (UUID string with dashes)
    """

    impl = String
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """Load the dialect-specific implementation.

        :param dialect: SQLAlchemy dialect (postgresql, sqlite, etc.)
        :return: Dialect-specific type descriptor
        """
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID(as_uuid=True))
        else:
            # SQLite: use TEXT for flexible UUID string storage
            return dialect.type_descriptor(String)

    def process_bind_param(self, value: Optional[uuid.UUID | str], dialect) -> Optional[uuid.UUID | str]:
        """Process value going INTO the database.

        Converts Python UUID to database format.

        :param value: Python uuid.UUID object or string
        :param dialect: SQLAlchemy dialect
        :return: Database-appropriate representation
        """
        if value is None:
            return value

        # Ensure we have a string representation
        if isinstance(value, uuid.UUID):
            value_str = str(value)
        elif isinstance(value, str):
            # Validate and normalize the UUID string
            try:
                value_str = str(uuid.UUID(value))
            except (ValueError, AttributeError):
                # Invalid UUID string - pass through as-is
                # Query will just not match anything in the database
                return value
        else:
            return str(value)

        if dialect.name == 'postgresql':
            # PostgreSQL: convert back to UUID object
            return uuid.UUID(value_str)
        else:
            # SQLite: store as string
            return value_str

    def process_result_value(self, value: Optional[str | uuid.UUID], dialect) -> Optional[str]:
        """Process value coming FROM the database.

        Converts database format to Python string.

        :param value: Database value
        :param dialect: SQLAlchemy dialect
        :return: UUID string
        """
        if value is None:
            return value

        if dialect.name == 'postgresql':
            # PostgreSQL returns UUID object - convert to string
            return str(value)
        else:
            # SQLite: return string as-is
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
