###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Test transition timestamp schema migration and archive preservation."""

from aiida import orm
from aiida.common.processes import ProcessState
from aiida.storage.sqlite_zip.backend import SqliteZipBackend
from aiida.storage.sqlite_zip.migrator import migrate
from aiida.tools.archive import create_archive
from aiida.tools.archive.implementations.sqlite_zip.main import ArchiveFormatSqlZip


def test_migrate_unknown_transition_times(archive_main_0002, tmp_path):
    """Old archives gain a queryable column without substituting modification times."""
    filename = tmp_path / 'migrated.aiida'
    migrate(archive_main_0002, filename, SqliteZipBackend.version_head())
    with ArchiveFormatSqlZip().open(filename, 'r') as archive:
        timestamps = archive.querybuilder().append(orm.Node, project='process_state_change_timestamp').all(flat=True)
        assert timestamps
        assert timestamps == [None] * len(timestamps)


def test_archive_preserves_transition_time(tmp_path):
    """Exported process metadata retains the timestamp independently of later modifications."""
    node = orm.CalculationNode().store()
    node.set_process_state(ProcessState.FINISHED)
    timestamp = node.process_state_change_timestamp
    node.seal()
    node.base.extras.set('unrelated', True)
    filename = tmp_path / 'process.aiida'
    create_archive([node], filename=filename)
    with ArchiveFormatSqlZip().open(filename, 'r') as archive:
        archived = archive.get(orm.Node, uuid=node.uuid)
        assert archived.process_state == ProcessState.FINISHED
        assert archived.process_state_change_timestamp == timestamp
        assert archive.querybuilder().append(orm.CalculationNode, project='process_state_change_timestamp').all(
            flat=True
        ) == [timestamp]
