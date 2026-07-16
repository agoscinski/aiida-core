###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Utilities specific to the Turso/libSQL storage backend."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from sqlalchemy import Engine

__all__ = ('TursoConfig', 'create_sqlalchemy_engine')


class TursoConfig(TypedDict, total=False):
    """Configuration to connect to a Turso/libSQL database."""

    database_url: str
    auth_token: str | None
    engine_kwargs: dict[str, Any]


def _enable_wal(dbapi_connection: Any, connection_record: Any) -> None:
    """Enable write-ahead logging for local database files.

    The libSQL driver defaults to the rollback journal (``DELETE``), where every commit takes an exclusive lock on
    the whole database. Under sustained concurrent writers (e.g. multiple daemon workers plus a submitter) lock
    convoys form and waits exceed any reasonable busy timeout, ending in ``database is locked`` errors. WAL mode
    lets readers proceed during writes and keeps write locks short.
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
    """Make the connection wait on write locks instead of failing immediately.

    pysqlite defaults to a 5 second busy timeout, but the libSQL driver (``libsql_experimental``) does not set one,
    causing immediate ``database is locked`` errors under concurrent writers such as multiple daemon workers.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA busy_timeout=60000;')
    cursor.close()


def _register_json_contains(dbapi_connection: Any, connection_record: Any) -> None:
    """Register the ``json_contains`` function if the DBAPI connection supports it.

    The libSQL driver (``libsql_experimental``, used for both local files and remote connections) does not support
    ``create_function``, in which case queries relying on ``json_contains`` (e.g. the ``contains`` operator of the
    ``QueryBuilder``) are not supported. Plain ``sqlite://`` URLs use pysqlite, which does support it.
    """
    from aiida.storage.sqlite_zip.utils import register_json_contains

    if hasattr(dbapi_connection, 'create_function'):
        register_json_contains(dbapi_connection, connection_record)


def create_sqlalchemy_engine(config: TursoConfig) -> Engine:
    """Create a SQLAlchemy engine for a Turso/libSQL database."""
    from sqlalchemy import create_engine, event

    from aiida.storage.sqlite_zip.utils import sqlite_case_sensitive_like, sqlite_enforce_foreign_keys

    database_url = config['database_url']

    if database_url.startswith(('libsql://', 'sqlite+libsql://', 'ws://', 'wss://', 'http://', 'https://')):
        try:
            import sqlalchemy_libsql  # noqa: F401
        except ImportError as exception:
            msg = 'The `sqlalchemy-libsql` package is required to use the `core.turso_dos` storage backend.'
            raise ImportError(msg) from exception

    engine_kwargs = dict(config.get('engine_kwargs', {}))
    connect_args = dict(engine_kwargs.get('connect_args', {}))

    if database_url.startswith('sqlite+libsql:'):
        from sqlalchemy.pool import NullPool

        # The libSQL driver (`libsql_experimental`) advertises `check_same_thread=False` for local files, but unlike
        # pysqlite its connections are not safe to reuse across threads: under the multi-threaded daemon workers,
        # pooled connections produce spurious `database is locked` errors that ignore the busy timeout. A fresh
        # connection per checkout avoids cross-thread reuse.
        engine_kwargs.setdefault('poolclass', NullPool)

    auth_token = config.get('auth_token')
    if auth_token is not None and 'auth_token' not in connect_args:
        connect_args['auth_token'] = auth_token

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
    # Triple slash means no host, i.e. a local database file; journal mode is a property of the local file and
    # must not be sent to remote/managed Turso databases.
    # The busy timeout must be set before any other statement: fresh libSQL connections perform locked
    # initialisation work on their first statement, which races with concurrent writers.
    event.listen(engine, 'connect', _set_busy_timeout)

    if database_url.startswith(('sqlite:///', 'sqlite+libsql:///')):
        event.listen(engine, 'connect', _enable_wal)

    event.listen(engine, 'connect', sqlite_case_sensitive_like)
    event.listen(engine, 'connect', sqlite_enforce_foreign_keys)
    event.listen(engine, 'connect', _register_json_contains)

    return engine
