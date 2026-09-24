###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests shared by profile storage backends for ``main_0003``."""

import pytest
import sqlalchemy as sa

from aiida.common import timezone
from aiida.common.utils import get_new_uuid


def test_migrate_users_to_profile(migration_profile):
    """Test that user ownership is replaced with a profile UUID label."""
    migrator_class = migration_profile.storage_cls.migrator

    with migrator_class(migration_profile) as migrator:
        migrator.migrate_up('main@main_0002')

        user_model = migrator.get_current_table('db_dbuser')
        computer_model = migrator.get_current_table('db_dbcomputer')
        authinfo_model = migrator.get_current_table('db_dbauthinfo')
        group_model = migrator.get_current_table('db_dbgroup')
        node_model = migrator.get_current_table('db_dbnode')
        comment_model = migrator.get_current_table('db_dbcomment')

        with migrator.session() as session:
            user = user_model(email='test', first_name='test', last_name='test', institution='test')
            session.add(user)
            session.commit()

            computer = computer_model(
                uuid=get_new_uuid(),
                label='test',
                hostname='localhost',
                description='',
                scheduler_type='core.direct',
                transport_type='core.local',
                metadata={},
            )
            session.add(computer)
            session.commit()

            authinfo = authinfo_model(
                aiidauser_id=user.id, dbcomputer_id=computer.id, metadata={}, auth_params={}, enabled=True
            )
            group = group_model(
                uuid=get_new_uuid(),
                label='test',
                type_string='',
                time=timezone.now(),
                description='',
                extras={},
                user_id=user.id,
            )
            node = node_model(
                uuid=get_new_uuid(),
                user_id=user.id,
                ctime=timezone.now(),
                mtime=timezone.now(),
                label='test',
                description='',
                node_type='data.core.dict.Dict.',
                repository_metadata={},
            )
            session.add_all((authinfo, group, node))
            session.commit()
            comment = comment_model(
                uuid=get_new_uuid(),
                dbnode_id=node.id,
                ctime=timezone.now(),
                mtime=timezone.now(),
                user_id=user.id,
                content='test',
            )
            session.add(comment)
            session.commit()

            user_id = user.id
            node_id = node.id
            group_id = group.id
            comment_id = comment.id
            authinfo_id = authinfo.id
            computer_id = computer.id

        migrator.migrate_up('main@main_0003')

        assert migrator.get_schema_version_profile() == 'main_0003'
        assert not sa.inspect(migrator.connection).has_table('db_dbuser')
        profile_model = migrator.get_current_table('db_dbprofile')
        with migrator.session() as session:
            identity = session.query(profile_model).one()
            assert identity.id == user_id
            assert identity.uuid == migration_profile.uuid
            assert identity.email == 'test'
            assert identity.first_name == 'test'
            assert identity.last_name == 'test'
            assert identity.institution == 'test'

        node_model = migrator.get_current_table('db_dbnode')
        group_model = migrator.get_current_table('db_dbgroup')
        comment_model = migrator.get_current_table('db_dbcomment')
        authinfo_model = migrator.get_current_table('db_dbauthinfo')

        with migrator.session() as session:
            node = session.query(node_model).filter(node_model.id == node_id).one()
            assert node.profile_uuid == migration_profile.uuid
            assert not hasattr(node, 'user_id')

            group = session.query(group_model).filter(group_model.id == group_id).one()
            assert group.profile_uuid == migration_profile.uuid

            comment = session.query(comment_model).filter(comment_model.id == comment_id).one()
            assert comment.profile_uuid == migration_profile.uuid

            authinfo = session.query(authinfo_model).filter(authinfo_model.id == authinfo_id).one()
            assert not hasattr(authinfo, 'aiidauser_id')
            assert authinfo.dbcomputer_id == computer_id


def test_migrate_no_users(migration_profile):
    """A fresh legacy storage still needs a profile identity after migration."""
    migrator_class = migration_profile.storage_cls.migrator
    with migrator_class(migration_profile) as migrator:
        migrator.migrate_up('main@main_0002')
        migrator.migrate_up('main@main_0003')
        model = migrator.get_current_table('db_dbprofile')
        with migrator.session() as session:
            profile = session.query(model).one()
            assert profile.uuid == migration_profile.uuid
            assert profile.email == ''


def test_reject_multiple_users(migration_profile):
    """Never drop user identities when a storage needs to be split into multiple profiles."""
    migrator_class = migration_profile.storage_cls.migrator
    with migrator_class(migration_profile) as migrator:
        migrator.migrate_up('main@main_0002')
        user_model = migrator.get_current_table('db_dbuser')
        with migrator.session() as session:
            session.add_all(
                [
                    user_model(email='first@example.org', first_name='First', last_name='', institution=''),
                    user_model(email='second@example.org', first_name='Second', last_name='', institution=''),
                ]
            )
            session.commit()

        with pytest.raises(RuntimeError, match='multiple users'):
            migrator.migrate_up('main@main_0003')

        assert migrator.get_schema_version_profile() == 'main_0002'
        assert sa.inspect(migrator.connection).has_table('db_dbuser')
        with migrator.session() as session:
            assert session.query(user_model).count() == 2


def test_migrate_legacy_code(migration_profile):
    """Test that legacy ``Code`` nodes are rewritten to modern code plugins."""
    migrator_class = migration_profile.storage_cls.migrator

    with migrator_class(migration_profile) as migrator:
        migrator.migrate_up('main@main_0002')

        user_model = migrator.get_current_table('db_dbuser')
        node_model = migrator.get_current_table('db_dbnode')

        with migrator.session() as session:
            user = user_model(email='test', first_name='test', last_name='test', institution='test')
            session.add(user)
            session.commit()

            remote = node_model(
                uuid=get_new_uuid(),
                user_id=user.id,
                ctime=timezone.now(),
                mtime=timezone.now(),
                label='test',
                description='',
                node_type='data.core.code.Code.',
                repository_metadata={},
                attributes={
                    'is_local': False,
                    'remote_exec_path': '/usr/bin/add.sh',
                    'input_plugin': 'core.arithmetic.add',
                    'prepend_text': 'module load add',
                    'append_text': '',
                },
                extras={'_aiida_hash': 'hash', 'hidden': True},
            )
            local = node_model(
                uuid=get_new_uuid(),
                user_id=user.id,
                ctime=timezone.now(),
                mtime=timezone.now(),
                label='test',
                description='',
                node_type='data.core.code.Code.',
                repository_metadata={},
                attributes={'is_local': True, 'local_executable': 'add.sh', 'input_plugin': 'core.arithmetic.add'},
                extras={'_aiida_hash': 'hash'},
            )
            installed = node_model(
                uuid=get_new_uuid(),
                user_id=user.id,
                ctime=timezone.now(),
                mtime=timezone.now(),
                label='test',
                description='',
                node_type='data.core.code.installed.InstalledCode.',
                repository_metadata={},
                attributes={'filepath_executable': '/usr/bin/bash'},
                extras={'_aiida_hash': 'hash'},
            )
            session.add_all((remote, local, installed))
            session.commit()

            remote_id = remote.id
            local_id = local.id
            installed_id = installed.id

        migrator.migrate_up('main@main_0003')

        assert migrator.get_schema_version_profile() == 'main_0003'

        node_model = migrator.get_current_table('db_dbnode')

        with migrator.session() as session:
            remote = session.query(node_model).filter(node_model.id == remote_id).one()
            assert remote.node_type == 'data.core.code.installed.InstalledCode.'
            assert remote.attributes == {
                'filepath_executable': '/usr/bin/add.sh',
                'input_plugin': 'core.arithmetic.add',
                'prepend_text': 'module load add',
                'append_text': '',
            }
            assert remote.extras == {'hidden': True}

            local = session.query(node_model).filter(node_model.id == local_id).one()
            assert local.node_type == 'data.core.code.portable.PortableCode.'
            assert local.attributes == {'filepath_executable': 'add.sh', 'input_plugin': 'core.arithmetic.add'}
            assert local.extras == {}

            installed = session.query(node_model).filter(node_model.id == installed_id).one()
            assert installed.node_type == 'data.core.code.installed.InstalledCode.'
            assert installed.attributes == {'filepath_executable': '/usr/bin/bash'}
            assert installed.extras == {'_aiida_hash': 'hash'}

        with pytest.raises(NotImplementedError, match=r'Downgrade of main_0003\.'):
            migrator.migrate_down('main@main_0002')
