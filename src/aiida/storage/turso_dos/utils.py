###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Utilities specific to the Turso storage backend."""

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from sqlalchemy import Engine

__all__ = ('TursoConfig', 'create_sqlalchemy_engine')

PYTURSO_SCHEMES = ('sqlite+turso://', 'sqlite+turso_sync://')
UNSUPPORTED_LIBSQL_SCHEMES = ('libsql://', 'sqlite+libsql://', 'ws://', 'wss://', 'http://', 'https://')


class TursoConfig(TypedDict, total=False):
    """Configuration to connect to a Turso database."""

    database_url: str
    auth_token: str | None
    engine_kwargs: dict[str, Any]


def _enable_wal(dbapi_connection: Any, connection_record: Any) -> None:
    """Enable write-ahead logging for local database files.

    Turso/pyturso and pysqlite both default to the rollback journal (``DELETE``), where every commit takes an
    exclusive lock on the whole database. Under sustained concurrent writers (e.g. multiple daemon workers plus a
    submitter) lock convoys form and waits can exceed the busy timeout, ending in ``database is locked`` errors. WAL
    mode lets readers proceed during writes and keeps write locks short.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA synchronous=NORMAL;')
    # The journal mode is persisted in the database file, so it only needs to be switched once (in practice by the
    # migrator's engine during storage initialisation). Switching requires an exclusive lock, which under concurrent
    # connections would spuriously fail with `database is locked`, so only switch when actually needed.
    if cursor.execute('PRAGMA journal_mode;').fetchone()[0].lower() != 'wal':
        cursor.execute('PRAGMA journal_mode=WAL;')
    cursor.close()


def _set_busy_timeout(dbapi_connection: Any, connection_record: Any) -> None:
    """Make the connection wait on write locks instead of failing immediately."""
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA busy_timeout=60000;')
    cursor.close()


def _register_json_contains(dbapi_connection: Any, connection_record: Any) -> None:
    """Register the ``json_contains`` function if the DBAPI connection supports it.

    Pyturso does not support ``create_function``, in which case queries relying on ``json_contains`` (e.g. the
    ``contains`` operator of the ``QueryBuilder``) are not supported. Plain ``sqlite://`` URLs use pysqlite, which does
    support it.
    """
    from aiida.storage.sqlite_zip.utils import register_json_contains

    if hasattr(dbapi_connection, 'create_function'):
        register_json_contains(dbapi_connection, connection_record)


def _requires_pyturso(database_url: str) -> bool:
    """Return whether the database URL requires the pyturso SQLAlchemy dialect."""
    return database_url.startswith(PYTURSO_SCHEMES)


def _is_local_sqlite_database(database_url: str) -> bool:
    """Return whether the database URL points at a local SQLite-compatible file."""
    return database_url.startswith(('sqlite:///', 'sqlite+turso:///', 'sqlite+turso_sync:///'))


def _validate_database_url(database_url: str) -> None:
    """Validate the database URL for the supported Turso backend drivers."""
    if database_url.startswith(UNSUPPORTED_LIBSQL_SCHEMES):
        msg = (
            '`core.turso_dos` no longer supports `sqlalchemy-libsql` URLs. Use `sqlite+turso:///path.db` for a '
            'local pyturso database or `sqlite+turso_sync:///path.db?remote_url=https://...` for Turso sync.'
        )
        raise ValueError(msg)


def create_sqlalchemy_engine(config: TursoConfig) -> Engine:
    """Create a SQLAlchemy engine for a Turso or SQLite database."""
    from sqlalchemy import create_engine, event

    from aiida.storage.sqlite_zip.utils import sqlite_case_sensitive_like, sqlite_enforce_foreign_keys

    database_url = config['database_url']
    _validate_database_url(database_url)

    if _requires_pyturso(database_url):
        try:
            importlib.import_module('turso.sqlalchemy')
        except ImportError as exception:
            msg = 'The `pyturso` package is required to use pyturso URLs with the `core.turso_dos` storage backend.'
            raise ImportError(msg) from exception

    engine_kwargs = dict(config.get('engine_kwargs', {}))
    connect_args = dict(engine_kwargs.get('connect_args', {}))

    if _requires_pyturso(database_url):
        from sqlalchemy.pool import NullPool

        # Pyturso reports DB-API threadsafety level 1: threads may share the module but not connections. The AiiDA
        # daemon workers are multi-threaded, so avoid pooling connections that may later be checked out by another
        # thread.
        engine_kwargs.setdefault('poolclass', NullPool)

    auth_token = config.get('auth_token')
    if auth_token is not None:
        if not database_url.startswith('sqlite+turso_sync:'):
            msg = '`auth_token` can only be used with `sqlite+turso_sync://` database URLs.'
            raise ValueError(msg)
        connect_args.setdefault('auth_token', auth_token)

    if connect_args:
        engine_kwargs['connect_args'] = connect_args

    engine = create_engine(
        database_url,
        json_serializer=json.dumps,
        json_deserializer=json.loads,
        **engine_kwargs,
    )

    # Attach the same connect listeners as ``aiida.storage.sqlite_zip.utils.create_sqla_engine`` to keep behavior
    # (and performance) on par with the ``core.sqlite_dos`` backend. In particular ``case_sensitive_like`` is required
    # for SQLite to use indexes for the prefix ``LIKE`` filters that the ``QueryBuilder`` emits.
    event.listen(engine, 'connect', _set_busy_timeout)

    if _is_local_sqlite_database(database_url):
        event.listen(engine, 'connect', _enable_wal)

    event.listen(engine, 'connect', sqlite_case_sensitive_like)
    event.listen(engine, 'connect', sqlite_enforce_foreign_keys)
    event.listen(engine, 'connect', _register_json_contains)

    return engine
