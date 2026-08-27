###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Prepare the storage schema for AiiDA v3.0.0.

Migration steps:

1. :func:`_rename_ssh_async_transport`: rename the ``core.ssh_async`` transport plugin to ``core.ssh``.
2. Migrate the deprecated ``Code`` data plugin to modern code plugins.

Bring archives in line with the profile database ``main_0003``, so that a computer exported from a
profile that used ``core.ssh_async`` can be imported into a v3 profile. Archives carry no client
configuration of their own, so unlike the profile databases, only the rename is needed here and not
the legacy ``core.ssh`` conversion. See ``aiida.storage.psql_dos.migrations.versions.main_0003``
for the rationale.

The rename has no inverse: once both kinds of computers share the ``core.ssh`` transport type, they
can no longer be told apart. The downgrade therefore only restores the schema revision and leaves
the transport types as they are.

Without the code migration, an archive containing nodes with the ``data.core.code.Code.`` node type
would import them as plain ``Data``, since the entry point no longer exists. See
``aiida.storage.psql_dos.migrations.versions.main_0003`` for the rationale.

Revision ID: main_0003
Revises: main_0002
Create Date: 2026-09-07
"""

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from aiida.storage.migrations.legacy_ssh import rename_ssh_async_transport

revision = 'main_0003'
down_revision = 'main_0002'
branch_labels = None
depends_on = None

LEGACY_NODE_TYPE = 'data.core.code.Code.'

# The node type is rewritten and one attribute key is renamed, so the stored hash no longer describes the node and is
# dropped. ``json_extract`` returns 1/0 for a JSON boolean, and ``json_remove`` ignores keys that are not present.
UPGRADE_STATEMENTS = (
    f"""
    UPDATE db_dbnode
    SET node_type = 'data.core.code.installed.InstalledCode.',
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
    SET node_type = 'data.core.code.portable.PortableCode.',
        attributes = json_set(
            json_remove(attributes, '$.is_local', '$.local_executable', '$.remote_exec_path'),
            '$.filepath_executable', COALESCE(json_extract(attributes, '$.local_executable'), '')
        ),
        extras = json_remove(extras, '$._aiida_hash')
    WHERE node_type = '{LEGACY_NODE_TYPE}'
      AND COALESCE(json_extract(attributes, '$.is_local'), 0) = 1;
    """,
)


def _rename_ssh_async_transport(conn: Connection) -> None:
    """Rewrite the ``transport_type`` of all ``core.ssh_async`` computers to ``core.ssh``."""
    rename_ssh_async_transport(conn)


def upgrade():
    """Migrations for the upgrade."""
    conn = op.get_bind()
    _rename_ssh_async_transport(conn)

    for statement in UPGRADE_STATEMENTS:
        conn.execute(text(statement))


def downgrade():
    """Migrations for the downgrade."""
    raise NotImplementedError('Downgrade of main_0003.')
