###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Unit tests for the ORM Backend class."""

from __future__ import annotations

import gc
import json
import logging
import pathlib
import subprocess
import sys
import textwrap
import uuid
import weakref
from unittest.mock import MagicMock

import pytest

from aiida import orm
from aiida.common import exceptions
from aiida.common.links import LinkType
from aiida.orm.entities import EntityTypes
from aiida.orm.implementation import storage_backend as storage_backend_module


class TestBackend:
    """Test backend."""

    @pytest.fixture(autouse=True)
    def init_test(self, backend):
        """Set up the backend."""
        self.backend = backend

    def test_transaction_nesting(self):
        """Test that transaction nesting works."""
        group = orm.Group(label='initial').store()
        with self.backend.transaction():
            group.label = 'pre-failure'
            try:
                with self.backend.transaction():
                    group.label = 'failure'
                    assert group.label == 'failure'
                    raise RuntimeError
            except RuntimeError:
                pass
            assert group.label == 'pre-failure'
        assert group.label == 'pre-failure'

    def test_transaction(self):
        """Test that transaction nesting works."""
        group1 = orm.Group(label='group1').store()
        group2 = orm.Group(label='group2').store()

        try:
            with self.backend.transaction():
                assert self.backend.in_transaction
                group1.label = 'broken1'
                group2.label = 'broken2'
                raise RuntimeError
        except RuntimeError:
            pass
        assert group1.label == 'group1'
        assert group2.label == 'group2'

    def test_store_in_transaction(self):
        """Test that storing inside a transaction is correctly dealt with."""
        group1 = orm.Group(label='group_store')
        with self.backend.transaction():
            group1.store()
        # the following shouldn't raise
        orm.Group.collection.get(label='group_store')

        group2 = orm.Group(label='group_store_fail')
        try:
            with self.backend.transaction():
                group2.store()
                raise RuntimeError
        except RuntimeError:
            pass

        with pytest.raises(exceptions.NotExistent):
            orm.Group.collection.get(label='group_store_fail')

    def test_bulk_insert(self):
        """Test that bulk insert works."""
        profile_uuid = self.backend.profile.uuid
        rows = [
            {'label': uuid.uuid4().hex, 'profile_uuid': profile_uuid},
            {'label': uuid.uuid4().hex, 'profile_uuid': profile_uuid},
        ]
        # should fail if all fields are not given and allow_defaults=False
        with pytest.raises(exceptions.IntegrityError, match='Incorrect fields'):
            self.backend.bulk_insert(EntityTypes.GROUP, rows)
        pks = self.backend.bulk_insert(EntityTypes.GROUP, rows, allow_defaults=True)
        assert len(pks) == len(rows)
        for pk, row in zip(pks, rows):
            assert isinstance(pk, int)
            group = orm.Group.collection.get(id=pk)
            assert group.label == row['label']

    def test_bulk_insert_in_transaction(self):
        """Test that bulk insert in a cancelled transaction is not committed."""
        profile_uuid = self.backend.profile.uuid
        rows = [
            {'label': uuid.uuid4().hex, 'profile_uuid': profile_uuid},
            {'label': uuid.uuid4().hex, 'profile_uuid': profile_uuid},
        ]
        try:
            with self.backend.transaction():
                self.backend.bulk_insert(EntityTypes.GROUP, rows, allow_defaults=True)
                raise RuntimeError
        except RuntimeError:
            pass
        for row in rows:
            with pytest.raises(exceptions.NotExistent):
                orm.Group.collection.get(label=row['label'])

    def test_bulk_update(self):
        """Test that bulk update works."""
        prefix = uuid.uuid4().hex
        groups = [orm.Group(label=f'{prefix}-{i}').store() for i in range(3)]
        # should raise if the 'id' field is not present
        with pytest.raises(exceptions.IntegrityError, match="'id' field not given"):
            self.backend.bulk_update(EntityTypes.GROUP, [{'label': 'other'}])
        # should raise if a non-existent field is present
        with pytest.raises(exceptions.IntegrityError, match='Incorrect fields'):
            self.backend.bulk_update(EntityTypes.GROUP, [{'id': groups[0].pk, 'x': 'other'}])
        self.backend.bulk_update(
            EntityTypes.GROUP, [{'id': groups[0].pk, 'label': 'other0'}, {'id': groups[1].pk, 'label': 'other1'}]
        )
        assert groups[0].label == 'other0'
        assert groups[1].label == 'other1'
        assert groups[2].label == f'{prefix}-2'

    def test_bulk_update_in_transaction(self):
        """Test that bulk update in a cancelled transaction is not committed."""
        prefix = uuid.uuid4().hex
        groups = [orm.Group(label=f'{prefix}-{i}').store() for i in range(3)]
        try:
            with self.backend.transaction():
                self.backend.bulk_update(
                    EntityTypes.GROUP,
                    [{'id': groups[0].pk, 'label': 'random0'}, {'id': groups[1].pk, 'label': 'random1'}],
                )
                raise RuntimeError
        except RuntimeError:
            pass
        for i, group in enumerate(groups):
            assert group.label == f'{prefix}-{i}'

    def test_delete_nodes_and_connections(self):
        """Delete all nodes and connections."""
        # create node, link and add to group
        node = orm.Data()
        calc_node = orm.CalcFunctionNode().store()
        node.base.links.add_incoming(calc_node, link_type=LinkType.CREATE, link_label='link')
        node.store()
        node_pk = node.pk
        group = orm.Group('name').store()
        group.add_nodes([node])

        # checks before deletion
        orm.Node.collection.get(id=node_pk)
        assert len(calc_node.base.links.get_outgoing().all()) == 1
        assert len(group.nodes) == 1

        # cannot call outside a transaction
        with pytest.raises(AssertionError):
            self.backend.delete_nodes_and_connections([node_pk])

        with self.backend.transaction():
            self.backend.delete_nodes_and_connections([node_pk])

        # checks after deletion
        with pytest.raises(exceptions.NotExistent):
            orm.Node.collection.get(id=node_pk)
        assert len(calc_node.base.links.get_outgoing().all()) == 0
        assert len(group.nodes) == 0


def test_finalize_ignores_closed_backend(aiida_profile, caplog):
    """Test the finalizer does nothing when the backend was closed explicitly."""
    backend = aiida_profile.storage_cls(aiida_profile)
    release = MagicMock(wraps=backend._resources.release)
    backend._resources.release = release
    backend.close()
    release.assert_called_once_with()
    backend_repr = repr(backend)
    backend_reference = weakref.ref(backend)

    with caplog.at_level(logging.INFO, logger=storage_backend_module.LOGGER.name):
        del backend
        gc.collect()

    assert backend_reference() is None
    release.assert_called_once_with()
    assert f'StorageBackend {backend_repr} was not closed explicitly.' not in caplog.messages


def test_finalize_runs_at_interpreter_shutdown(tmp_path):
    """Test the backend finalizer runs when the interpreter exits."""
    marker = tmp_path / 'backend-finalized'
    storage_path = tmp_path / 'storage'
    config_dir = tmp_path / 'config'
    code = textwrap.dedent(
        f"""
        import os
        from pathlib import Path

        os.environ['AIIDA_PATH'] = {str(config_dir)!r}

        from aiida.storage.sqlite_temp.backend import SqliteTempBackend

        backend = SqliteTempBackend(SqliteTempBackend.create_profile(filepath={str(storage_path)!r}))

        class DummyRepo:
            def erase(self):
                Path({str(marker)!r}).write_text('closed')

        backend._resources.repo = DummyRepo()
        """
    )

    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
    assert marker.read_text() == 'closed'


def test_backup_not_implemented(aiida_config, backend, monkeypatch, tmp_path):
    """Test the backup functionality if the plugin does not implement it."""

    def _backup(*args, **kwargs):
        raise NotImplementedError

    monkeypatch.setattr(backend, '_backup', _backup)

    filepath_backup = tmp_path / 'backup_dir'

    with pytest.raises(NotImplementedError):
        backend.backup(str(filepath_backup))

    # The backup directory should have been initialized but then cleaned up when the plugin raised the exception
    assert not filepath_backup.is_dir()

    # Now create the backup directory with the config file and some other content to it.
    filepath_backup.mkdir()
    (filepath_backup / 'config.json').write_text(json.dumps(aiida_config.dictionary))
    (filepath_backup / 'backup-deadbeef').mkdir()

    with pytest.raises(NotImplementedError):
        backend.backup(str(filepath_backup))

    # The backup directory should not have been delete
    assert filepath_backup.is_dir()
    assert (filepath_backup / 'config.json').is_file()


def test_backup_implemented(backend, monkeypatch, tmp_path):
    """Test the backup functionality if the plugin does implement it."""

    def _backup(dest: str, keep: int | None = None):
        (pathlib.Path(dest) / 'backup.file').touch()

    monkeypatch.setattr(backend, '_backup', _backup)

    filepath_backup = tmp_path / 'backup_dir'
    backend.backup(str(filepath_backup))
    assert (filepath_backup / 'config.json').is_file()
    assert (filepath_backup / 'backup.file').is_file()
