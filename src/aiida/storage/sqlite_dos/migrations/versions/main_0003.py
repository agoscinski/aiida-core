###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Prepare the storage schema for AiiDA v3.0.0.

Migration steps:

1. :func:`_ensure_settings_table`: backfill the settings table.
2. :func:`_migrate_ssh_transports`: migrate SSH computers to the asynchronous ``core.ssh`` transport plugin.
3. :func:`_migrate_legacy_codes`: migrate the deprecated ``Code`` data plugin.

See the ``main_0003`` revision of the ``psql_dos`` backend for the rationale of the transport
migration, which likewise has no inverse: the downgrade only restores the schema revision.

Revision ID: main_0003
Revises: main_0002
Create Date: 2026-09-07
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.engine import Connection

from aiida.storage.log import MIGRATE_LOGGER
from aiida.storage.migrations.legacy_ssh import migrate_ssh_transports

revision = 'main_0003'
down_revision = 'main_0002'
branch_labels = None
depends_on = None

LEGACY_NODE_TYPE = 'data.core.code.Code.'
INSTALLED_NODE_TYPE = 'data.core.code.installed.InstalledCode.'
PORTABLE_NODE_TYPE = 'data.core.code.portable.PortableCode.'

# The node type is rewritten and one attribute key is renamed, so the stored hash no longer describes the node and is
# dropped. ``json_extract`` returns 1/0 for a JSON boolean, and ``json_remove`` ignores keys that are not present.
UPGRADE_STATEMENTS = (
    f"""
    UPDATE db_dbnode
    SET node_type = '{INSTALLED_NODE_TYPE}',
        attributes = json_set(
            json_remove(attributes, '$.is_local', '$.local_executable', '$.remote_exec_path'),
            '$.filepath_executable', COALESCE(json_extract(attributes, '$.remote_exec_path'), '')
        ),
        extras = json_remove(extras, '$._aiida_hash')
    WHERE node_type = '{LEGACY_NODE_TYPE}'
      AND COALESCE(json_extract(attributes, '$.is_local'), 0) = 0;
    """,
    f"""
    UPDATE db_dbnode
    SET node_type = '{PORTABLE_NODE_TYPE}',
        attributes = json_set(
            json_remove(attributes, '$.is_local', '$.local_executable', '$.remote_exec_path'),
            '$.filepath_executable', COALESCE(json_extract(attributes, '$.local_executable'), '')
        ),
        extras = json_remove(extras, '$._aiida_hash')
    WHERE node_type = '{LEGACY_NODE_TYPE}'
      AND COALESCE(json_extract(attributes, '$.is_local'), 0) = 1;
    """,
)


def _migrate_legacy_codes(conn):
    """Migrate the deprecated ``Code`` data plugin to ``InstalledCode``/``PortableCode``.

    The ``Code`` plugin was deprecated in ``aiida-core==2.0`` and removed in ``aiida-core==3.0``. Without this
    migration, stored nodes with the ``data.core.code.Code.`` node type would no longer resolve to a code class
    at all: the entry point is gone, so they would silently load as plain ``Data`` and lose the entire code API.

    A stored legacy code carries everything needed to become a modern one. The ``is_local`` attribute
    distinguishes the two cases:

    * ``is_local=False``: a code installed on a remote computer, i.e. an ``InstalledCode``. The executable is
      recorded under ``remote_exec_path`` and the computer is already set on the node.
    * ``is_local=True``: a code whose files live in the node repository, i.e. a ``PortableCode``. The
      executable is recorded under ``local_executable`` and the repository contents are left untouched.

    Both replacements record the executable under ``filepath_executable``, so the rewrite is a node type change
    plus a rename of one attribute key. All other attributes (``input_plugin``, ``prepend_text``,
    ``append_text``, ...) use the same keys on ``AbstractCode`` and are left as they are.
    """
    count = conn.execute(text(f"SELECT count(*) FROM db_dbnode WHERE node_type = '{LEGACY_NODE_TYPE}';")).scalar()

    if count:
        MIGRATE_LOGGER.report(
            f'Migrating {count} legacy `Code` node(s) to `InstalledCode`/`PortableCode`. Their hashes are '
            'invalidated; run `verdi node rehash` to recompute them.'
        )

    for statement in UPGRADE_STATEMENTS:
        conn.execute(text(statement))


def _ensure_settings_table(conn: Connection) -> None:
    """Backfill the settings table.

    The initial SQLite migration was based on the archive schema, which does not contain the settings table.
    The ``sqlite_dos`` backend, however, requires it for the repository UUID. Fresh profiles created before
    this migration therefore miss the table, so it is created here if absent. No-op on databases that already
    have it.
    """
    if sa.inspect(conn).has_table('db_dbsetting'):
        return

    op.create_table(
        'db_dbsetting',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('key', sa.String(length=1024), nullable=False),
        sa.Column('val', JSON(), nullable=True),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('time', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id', name='db_dbsetting_pkey'),
        sa.UniqueConstraint('key', name='uq_db_dbsetting_key'),
    )


def _migrate_ssh_transports(conn: Connection) -> None:
    """Migrate both kinds of SSH computers to the asynchronous ``core.ssh`` transport plugin.

    Delegates to :func:`~aiida.storage.migrations.legacy_ssh.migrate_ssh_transports`, which converts
    the legacy ``core.ssh`` computers first and renames the ``core.ssh_async`` computers after, since
    the rename is what still tells the two kinds apart.
    """
    migrate_ssh_transports(conn)


def upgrade():
    """Migrations for the upgrade."""
    conn = op.get_bind()
    _ensure_settings_table(conn)
    _migrate_ssh_transports(conn)
    _migrate_legacy_codes(conn)


def downgrade():
    """Migrations for the downgrade."""
    raise NotImplementedError('Downgrade of main_0003.')
