###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Replace shared process activity settings with per-node transition timestamps."""

import sqlalchemy as sa
from alembic import op

COLUMN = 'process_state_change_timestamp'
INDEX = f'ix_db_dbnode_db_dbnode_{COLUMN}'


def add_process_state_change_timestamp() -> None:
    """Add an indexed, nullable transition timestamp and remove obsolete settings.

    Exact transition times cannot be recovered from node modification times or the
    shared settings, so historical timestamps remain unknown.
    """
    op.add_column('db_dbnode', sa.Column(COLUMN, sa.DateTime(timezone=True), nullable=True))
    op.create_index(INDEX, 'db_dbnode', [COLUMN])
    connection = op.get_bind()
    if sa.inspect(connection).has_table('db_dbsetting'):
        connection.execute(
            sa.text(
                'DELETE FROM db_dbsetting WHERE key IN '
                "('process|state_change|calculation', 'process|state_change|work')"
            )
        )


def drop_process_state_change_timestamp() -> None:
    """Remove the transition timestamp; deleted activity settings are not reconstructed."""
    op.drop_index(INDEX, table_name='db_dbnode')
    op.drop_column('db_dbnode', COLUMN)
