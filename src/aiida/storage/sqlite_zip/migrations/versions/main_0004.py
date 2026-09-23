###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Replace user ownership with a profile UUID label.

Migration steps:

1. :func:`_migrate_users_to_profile`: stamp ``profile_uuid`` and drop users.

This is the archive counterpart of the profile-database ``main_0003``
user-to-profile transformation. Archives are profile-agnostic, so rows of
unknown origin are stamped with an empty string; the importing profile keeps
stored values as-is, preserving origin across merged databases.

Revision ID: main_0004
Revises: main_0003
Create Date: 2026-09-22
"""

from alembic import op

revision = 'main_0004'
down_revision = 'main_0003'
branch_labels = None
depends_on = None


def _migrate_users_to_profile():
    """Replace per-row user ownership with a profile UUID label.

    Unknown origin (archives carry users, not profiles) is marked with an
    empty string. See :func:`aiida.storage.migrations.migrate_users_to_profile`.
    """
    from aiida.storage.migrations import migrate_users_to_profile

    migrate_users_to_profile(op, '')


def upgrade():
    """Migrations for the upgrade."""
    _migrate_users_to_profile()


def downgrade():
    """Migrations for the downgrade."""
    raise NotImplementedError('Downgrade of main_0004.')
