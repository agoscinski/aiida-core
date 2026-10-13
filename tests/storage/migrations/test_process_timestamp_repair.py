###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Upgrade databases and archives already stamped with either main_0003 schema."""

import pytest
import sqlalchemy as sa
from alembic.command import downgrade, upgrade
from alembic.migration import MigrationContext
from alembic.operations import Operations

from aiida.storage.migrations.process_state import COLUMN, INDEX
from aiida.storage.sqlite_zip.migrator import _alembic_connect
from aiida.storage.sqlite_zip.utils import create_sqla_engine


def remove_timestamp(connection, missing):
    """Reproduce an old stamp or a missing index without changing the revision."""
    if missing == 'none':
        return
    operations = Operations(MigrationContext.configure(connection))
    operations.drop_index(INDEX, table_name='db_dbnode')
    if missing == 'column':
        operations.drop_column('db_dbnode', COLUMN)


def assert_timestamp_schema(connection):
    inspector = sa.inspect(connection)
    column = next(column for column in inspector.get_columns('db_dbnode') if column['name'] == COLUMN)
    assert column['nullable'] is True
    assert isinstance(column['type'], sa.DateTime)
    indexes = [index for index in inspector.get_indexes('db_dbnode') if index['name'] == INDEX]
    assert len(indexes) == 1
    assert indexes[0]['column_names'] == [COLUMN]


@pytest.mark.parametrize('missing', ['none', 'column', 'index'])
def test_profile_repair(migration_profile, missing):
    """The next revision repairs old stamps and is harmless on corrected ones."""
    migrator_class = migration_profile.storage_cls.migrator
    with migrator_class(migration_profile) as migrator:
        migrator.migrate_up('main@main_0003')
        remove_timestamp(migrator.connection, missing)
        migrator.connection.commit()
        assert migrator.get_schema_version_profile() == 'main_0003'
        migrator.migrate_up('main@main_0004')
        assert_timestamp_schema(migrator.connection)
        assert migrator.get_schema_version_profile() == 'main_0004'
        migrator.migrate_down('main@main_0003')
        assert_timestamp_schema(migrator.connection)
        migrator.migrate_up('main@main_0004')
        assert_timestamp_schema(migrator.connection)


@pytest.mark.parametrize('missing', ['none', 'column', 'index'])
def test_archive_repair(tmp_path, missing):
    """Archive upgrades also repair stamps created before the column was added."""
    database_path = tmp_path / 'database.sqlite'
    with _alembic_connect(database_path) as config:
        upgrade(config, 'main_0003')
    engine = create_sqla_engine(database_path)
    with engine.begin() as connection:
        remove_timestamp(connection, missing)
    with _alembic_connect(database_path) as config:
        upgrade(config, 'main_0004')
        downgrade(config, 'main_0003')
        upgrade(config, 'main_0004')
    with engine.connect() as connection:
        assert_timestamp_schema(connection)
    engine.dispose()
