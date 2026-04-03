###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for Phase 4: Log level audit — verbose INFO calls demoted to DEBUG.

These tests verify that specific files have had their routine ``logger.info`` calls
changed to ``logger.debug``. We use AST parsing to count remaining ``logger.info`` calls
in files that should have been fully or mostly converted.
"""

import ast
from pathlib import Path

import pytest

# Files that should have ALL logger.info calls converted to logger.debug
# (i.e., zero remaining logger.info calls after the audit)
FILES_ALL_INFO_TO_DEBUG = [
    'src/aiida/engine/processes/calcjobs/monitors.py',
    'src/aiida/engine/processes/workchains/restart.py',
    'src/aiida/brokers/rabbitmq/defaults.py',
    'src/aiida/parsers/plugins/diff_tutorial/parsers.py',
    'src/aiida/orm/nodes/data/code/portable.py',
]

# Files where some logger.info calls should be converted and some should remain.
# (key, value) = (file_path, maximum number of logger.info calls that should remain)
FILES_PARTIAL_INFO_TO_DEBUG = {
    'src/aiida/engine/processes/calcjobs/tasks.py': 0,
    'src/aiida/engine/processes/calcjobs/manager.py': 0,
    'src/aiida/engine/runners.py': 1,  # keep: process confirmed terminated by polling
    'src/aiida/engine/processes/process.py': 2,  # keep: kill request received, kill signal unable to reach child
}


def _count_logger_info_calls(filepath: str) -> int:
    """Count the number of ``logger.info(...)`` calls in a Python source file.

    Matches both ``logger.info(...)`` and ``self.logger.info(...)`` patterns.

    :param filepath: Path to the Python source file.
    :return: Number of logger.info calls found.
    """
    source = Path(filepath).read_text()
    tree = ast.parse(source)
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == 'info':
                value = node.func.value
                # Match logger.info(...)
                if isinstance(value, ast.Name) and value.id == 'logger':
                    count += 1
                # Match self.logger.info(...)
                elif (
                    isinstance(value, ast.Attribute)
                    and value.attr == 'logger'
                    and isinstance(value.value, ast.Name)
                    and value.value.id == 'self'
                ):
                    count += 1
    return count


@pytest.mark.presto
@pytest.mark.parametrize('filepath', FILES_ALL_INFO_TO_DEBUG)
def test_all_info_converted_to_debug(filepath):
    """Files that should have all logger.info calls converted should have zero remaining."""
    count = _count_logger_info_calls(filepath)
    assert count == 0, f'{filepath} still has {count} logger.info() calls that should be logger.debug()'


@pytest.mark.presto
@pytest.mark.parametrize('filepath,max_info', list(FILES_PARTIAL_INFO_TO_DEBUG.items()))
def test_partial_info_to_debug(filepath, max_info):
    """Files with partial conversion should have at most the expected number of logger.info calls."""
    count = _count_logger_info_calls(filepath)
    assert count <= max_info, f'{filepath} has {count} logger.info() calls, expected at most {max_info}'


@pytest.mark.presto
class TestSchedulerPluginLogLevels:
    """Test that scheduler plugins had their info calls demoted to debug."""

    SCHEDULER_FILES = [
        'src/aiida/schedulers/plugins/slurm.py',
        'src/aiida/schedulers/plugins/sge.py',
        'src/aiida/schedulers/plugins/pbsbaseclasses.py',
        'src/aiida/schedulers/plugins/lsf.py',
    ]

    @pytest.mark.parametrize('filepath', SCHEDULER_FILES)
    def test_scheduler_no_info_submission_poll(self, filepath):
        """Scheduler plugin files should have submission and poll messages at DEBUG, not INFO."""
        source = Path(filepath).read_text()
        tree = ast.parse(source)

        info_messages = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == 'info':
                    value = node.func.value
                    if isinstance(value, ast.Name) and value.id == 'logger':
                        info_messages.append(node.lineno)
                    elif isinstance(value, ast.Attribute) and value.attr == 'logger':
                        info_messages.append(node.lineno)

        # Most scheduler files should have zero logger.info calls after the audit.
        # direct.py is an exception (has a configuration notice that stays at INFO).
        assert len(info_messages) == 0, f'{filepath} still has logger.info() calls at lines {info_messages}'

    def test_direct_scheduler_keeps_config_notice(self):
        """The direct scheduler should keep its configuration notice at INFO."""
        filepath = 'src/aiida/schedulers/plugins/direct.py'
        source = Path(filepath).read_text()
        tree = ast.parse(source)

        info_count = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == 'info':
                    value = node.func.value
                    if isinstance(value, ast.Name) and value.id == 'logger':
                        info_count += 1
                    elif isinstance(value, ast.Attribute) and value.attr == 'logger':
                        info_count += 1

        # Direct scheduler should keep at most 1 INFO call (configuration notice)
        assert info_count <= 1, f'{filepath} has {info_count} logger.info() calls, expected at most 1'
