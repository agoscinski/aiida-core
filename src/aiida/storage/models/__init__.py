"""Unified database models for AiiDA storage backends.

This module contains database-agnostic model definitions that work with both
PostgreSQL and SQLite using SQLAlchemy's TypeDecorator system.

This replaces the duplicated models in:
- aiida.storage.models (PostgreSQL-specific)
- aiida.storage.sqlite_zip.models (SQLite-specific)

The unified models use custom types that automatically adapt to the database dialect,
eliminating the need for manual conversion functions like pg_to_sqlite().
"""

from .base import Base, naming_convention, get_orm_metadata
from .types import GUID, TZDateTime, JSONType
from .user import DbUser
from .computer import DbComputer
from .node import DbNode, DbLink
from .authinfo import DbAuthInfo
from .group import DbGroup, DbGroupNode
from .comment import DbComment
from .log import DbLog
from .settings import DbSetting

__all__ = [
    'Base',
    'naming_convention',
    'get_orm_metadata',
    'GUID',
    'TZDateTime',
    'JSONType',
    'DbUser',
    'DbComputer',
    'DbNode',
    'DbLink',
    'DbAuthInfo',
    'DbGroup',
    'DbGroupNode',
    'DbComment',
    'DbLog',
    'DbSetting',
]
