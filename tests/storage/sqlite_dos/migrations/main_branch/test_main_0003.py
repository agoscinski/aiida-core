###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Test ``main_0003.py``."""

from sqlalchemy import inspect

from aiida.storage.sqlite_dos.backend import SqliteDosMigrator
from aiida.storage.sqlite_zip.models import SqliteBase


def test_with_db_setting(uninitialised_profile):
    """Test upgrading a historically initialized database with the settings table.

    Before ``main_0003``, fresh ``sqlite_dos`` initialization created
    ``db_dbsetting`` through ORM metadata, while the migration schema omitted
    it. This models that database state before upgrading it to ``main_0003``.
    """
    with SqliteDosMigrator(uninitialised_profile) as migrator:
        SqliteBase.metadata.tables['db_dbsetting'].create(migrator.connection)
        migrator.connection.commit()
        migrator.migrate_up('main@main_0003')
        assert inspect(migrator.connection).has_table('db_dbsetting')
