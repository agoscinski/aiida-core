"""Base classes and utilities for unified AiiDA models.

This module provides the base SQLAlchemy declarative base and common utilities
used by all AiiDA database models.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import MetaData, event, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Column


class Model:
    """Base ORM model class.

    Provides common functionality for all database models, including
    a standardized __repr__ method.
    """

    def __repr__(self) -> str:
        """Return a representation of the row columns."""
        string = f'<{self.__class__.__name__}'
        for col in self.__table__.columns:  # type: ignore[attr-defined]
            col_name = col.name
            if col_name == 'metadata':
                col_name = '_metadata'
            val = f'{getattr(self, col_name)!r}'
            if len(val) > 10:  # truncate long values
                val = val[:10] + '...'
            string += f' {col_name}={val},'
        return string + '>'


# Naming convention for database constraints
# See https://alembic.sqlalchemy.org/en/latest/naming.html
# This ensures consistent naming across PostgreSQL and SQLite
naming_convention = (
    ('pk', '%(table_name)s_pkey'),  # this is identical to the default PSQL convention
    ('ix', 'ix_%(table_name)s_%(column_0_N_label)s'),
    # note, indexes using varchar_pattern_ops should be named: 'ix_pat_%(table_name)s_%(column_0_N_label)s'
    ('uq', 'uq_%(table_name)s_%(column_0_N_name)s'),
    ('ck', 'ck_%(table_name)s_%(constraint_name)s'),
    ('fk', 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s'),
    # note, ideally we may also append with '_%(referred_column_0_N_name)s', but this causes ORM construction errors:
    # https://github.com/sqlalchemy/sqlalchemy/issues/5350
)

# Create metadata with naming convention (convert tuple to dict)
metadata = MetaData(naming_convention=dict(naming_convention))


# Create the declarative base for all models using the Model class
Base = declarative_base(cls=Model, name='Model', metadata=metadata)


def instant_defaults_listener(target: Any, args: Any, kwargs: Any) -> None:
    """Set column defaults immediately on instance creation.

    By default, SQLAlchemy only sets column defaults when inserting to the database.
    This listener sets them immediately when creating Python objects, which provides
    better user experience and matches AiiDA's expected behavior.

    This is especially important for:
    - UUID generation (want UUID available immediately, not just after insert)
    - Timestamps (want creation time set on object creation)
    - Default values (want consistent state before and after save)

    :param target: The model class being instantiated
    :param args: Positional arguments passed to __init__
    :param kwargs: Keyword arguments passed to __init__

    Example:
        user = DbUser(email='test@example.com')
        print(user.uuid)  # Works immediately, not None!
    """
    # using args would break this logic
    assert not args, f'args are not allowed in {target} instantiation'

    # If someone passes metadata in **kwargs we change it to _metadata
    if 'metadata' in kwargs:
        kwargs['_metadata'] = kwargs.pop('metadata')

    # don't allow certain JSON fields to be null
    for col in ('attributes', 'extras', '_metadata'):
        if col in kwargs and kwargs[col] is None:
            kwargs[col] = {}

    columns = inspect(target.__class__).columns

    # The only time that we allow mtime not to be non-null is when we explicitly pass it as a kwarg.
    # This covers the case that a node is constructed based on some very predefined data,
    # like when we create nodes in the AiiDA import functions
    if 'mtime' in columns and 'mtime' not in kwargs:
        kwargs['mtime'] = None

    for key, column in columns.items():
        if key in kwargs:
            continue
        if hasattr(column, 'default') and column.default is not None:
            if callable(column.default.arg):
                kwargs[key] = column.default.arg(target)
            else:
                kwargs[key] = column.default.arg


# Register the listener for all models inheriting from Base
event.listen(Base, 'init', instant_defaults_listener, propagate=True)


def create_indexes_for_dialect(dialect_name: str = None) -> dict:
    """Create index configuration that may be dialect-specific.

    Some index types are only supported by certain databases:
    - varchar_pattern_ops: PostgreSQL-only (for LIKE queries)
    - GIN/GiST indexes: PostgreSQL-only (for JSONB queries)
    - Full-text indexes: Database-specific syntax

    This helper allows conditional index creation based on the dialect.

    :param dialect_name: Name of the dialect ('postgresql', 'sqlite', etc.)
    :return: Dictionary of index options

    Usage:
        __table_args__ = (
            Index(
                'ix_node_label',
                'label',
                **create_indexes_for_dialect('postgresql')
            ),
        )
    """
    if dialect_name == 'postgresql':
        return {
            'postgresql_using': 'btree',
            'postgresql_ops': {'label': 'varchar_pattern_ops'}
        }
    return {}
