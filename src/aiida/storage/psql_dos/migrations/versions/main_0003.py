###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Prepare the storage schema for AiiDA v3.0.0.

Migration steps:

1. :func:`_migrate_users_to_profile`: replace user ownership with a profile UUID label.
2. :func:`_migrate_legacy_codes`: migrate the deprecated ``Code`` data plugin.

Revision ID: main_0003
Revises: main_0002
Create Date: 2026-09-07
"""

from alembic import op
from sqlalchemy import text

from aiida.storage.log import MIGRATE_LOGGER

revision = 'main_0003'
down_revision = 'main_0002'
branch_labels = None
depends_on = None

LEGACY_NODE_TYPE = 'data.core.code.Code.'
INSTALLED_NODE_TYPE = 'data.core.code.installed.InstalledCode.'
PORTABLE_NODE_TYPE = 'data.core.code.portable.PortableCode.'

# The node type is rewritten and one attribute key is renamed, so the stored hash no longer describes the node. It is
# dropped rather than recomputed, which is what the other data migrations that touch attributes do as well.
UPGRADE_STATEMENTS = (
    f"""
    UPDATE db_dbnode
    SET node_type = '{INSTALLED_NODE_TYPE}',
        attributes = (attributes - 'is_local' - 'local_executable' - 'remote_exec_path')
            || jsonb_build_object('filepath_executable', COALESCE(attributes -> 'remote_exec_path', '""'::jsonb)),
        extras = extras - '_aiida_hash'
    WHERE node_type = '{LEGACY_NODE_TYPE}'
      AND COALESCE((attributes ->> 'is_local')::boolean, false) IS FALSE;
    """,
    f"""
    UPDATE db_dbnode
    SET node_type = '{PORTABLE_NODE_TYPE}',
        attributes = (attributes - 'is_local' - 'local_executable' - 'remote_exec_path')
            || jsonb_build_object('filepath_executable', COALESCE(attributes -> 'local_executable', '""'::jsonb)),
        extras = extras - '_aiida_hash'
    WHERE node_type = '{LEGACY_NODE_TYPE}'
      AND COALESCE((attributes ->> 'is_local')::boolean, false) IS TRUE;
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


def _migrate_users_to_profile():
    """Replace per-row user ownership with a profile UUID label.

    Every row is stamped with the UUID of the profile being migrated (the
    single-owner model); ``db_dbuser`` and all user foreign keys are dropped.
    See :func:`aiida.storage.migrations.migrate_users_to_profile`.
    """
    from aiida.storage.migrations import migrate_users_to_profile

    profile_uuid: str = op.get_context().opts['aiida_profile'].uuid
    migrate_users_to_profile(op, profile_uuid)


def upgrade():
    """Migrations for the upgrade."""
    # Rewrite nodes only after the schema changes: the UPDATE can leave deferred foreign-key trigger events
    # pending, which prevents PostgreSQL from creating indexes on db_dbnode in the same transaction.
    _migrate_users_to_profile()
    _migrate_legacy_codes(op.get_bind())


def downgrade():
    """Migrations for the downgrade."""
    raise NotImplementedError('Downgrade of main_0003.')
