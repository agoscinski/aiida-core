###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Test ``main_0004_ownership.py``."""

from sqlalchemy import inspect

from aiida.common import timezone
from aiida.common.utils import get_new_uuid
from aiida.storage.psql_dos.migrator import PsqlDosMigrator


def test_migration(perform_migrations: PsqlDosMigrator):
    """Test the migration adds ownership storage, keeping existing nodes standalone."""
    perform_migrations.migrate_up('main@main_0003')

    user_model = perform_migrations.get_current_table('db_dbuser')
    node_model = perform_migrations.get_current_table('db_dbnode')

    with perform_migrations.session() as session:
        user = user_model(email='test', first_name='test', last_name='test', institution='test')
        session.add(user)
        session.commit()

        node = node_model(
            uuid=get_new_uuid(),
            user_id=user.id,
            ctime=timezone.now(),
            mtime=timezone.now(),
            label='test',
            description='',
            node_type='data.core.float.Float.',
            attributes={'value': 1.0},
            repository_metadata={},
            extras={},
        )
        session.add(node)
        session.commit()
        node_id = node.id

    # Perform the migration that is being tested.
    perform_migrations.migrate_up('main@main_0004')

    node_model = perform_migrations.get_current_table('db_dbnode')
    membership_model = perform_migrations.get_current_table('db_dbmembership')

    with perform_migrations.session() as session:
        # Existing standalone nodes remain standalone through migration: no owner, unset element type.
        node = session.query(node_model).filter(node_model.id == node_id).one()
        assert node.owner_id is None
        assert node.container_element_type is None
        assert session.query(membership_model).count() == 0

    # The migrated schema carries the ownership tables, columns, and constraints.
    inspector = inspect(perform_migrations.connection.engine)
    assert {column['name'] for column in inspector.get_columns('db_dbnode')} >= {
        'owner_id',
        'container_element_type',
    }
    assert {column['name'] for column in inspector.get_columns('db_dbmembership')} == {
        'id',
        'owner_id',
        'child_id',
        'position',
        'key',
    }
    unique = {
        tuple(sorted(constraint['column_names'])) for constraint in inspector.get_unique_constraints('db_dbmembership')
    }
    assert unique == {('child_id',), ('key', 'owner_id'), ('owner_id', 'position')}
