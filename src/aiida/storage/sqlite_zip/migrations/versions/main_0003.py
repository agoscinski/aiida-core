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

Bring archives in line with the profile database ``main_0003``, so that a computer exported from a
profile that used ``core.ssh_async`` can be imported into a v3 profile. Archives carry no client
configuration of their own, so unlike the profile databases, only the rename is needed here and not
the legacy ``core.ssh`` conversion. See ``aiida.storage.psql_dos.migrations.versions.main_0003``
for the rationale.

The rename has no inverse: once both kinds of computers share the ``core.ssh`` transport type, they
can no longer be told apart. The downgrade therefore only restores the schema revision and leaves
the transport types as they are.

Revision ID: main_0003
Revises: main_0002
Create Date: 2026-09-07
"""

from alembic import op
from sqlalchemy.engine import Connection

from aiida.storage.migrations.legacy_ssh import rename_ssh_async_transport

revision = 'main_0003'
down_revision = 'main_0002'
branch_labels = None
depends_on = None


def _rename_ssh_async_transport(conn: Connection) -> None:
    """Rewrite the ``transport_type`` of all ``core.ssh_async`` computers to ``core.ssh``."""
    rename_ssh_async_transport(conn)


def upgrade():
    """Migrations for the upgrade."""
    _rename_ssh_async_transport(op.get_bind())


def downgrade():
    """Migrations for the downgrade."""
    raise NotImplementedError('Downgrade of main_0003.')
