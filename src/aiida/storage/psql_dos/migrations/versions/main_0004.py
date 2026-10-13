###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Repair profiles stamped main_0003 before the transition timestamp was added.

Revision ID: main_0004
Revises: main_0003
"""

from aiida.storage.migrations.process_state import add_process_state_change_timestamp

revision = 'main_0004'
down_revision = 'main_0003'
branch_labels = None
depends_on = None


def upgrade():
    """Ensure both the column and index exist without changing existing timestamps."""
    add_process_state_change_timestamp()


def downgrade():
    """Keep the timestamp, which also belongs to the corrected main_0003 schema."""
