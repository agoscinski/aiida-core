###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for ``verdi broker``."""

import pytest

from aiida.cmdline.commands import cmd_verdi


def _get_queue_config():
    """Helper to get the current queue config."""
    from aiida import get_profile

    return get_profile().get_queue_config()


@pytest.mark.usefixtures('aiida_profile_clean', 'stopped_daemon_client')
class TestBrokerQueueCommands:
    """Tests for ``verdi broker queue`` commands."""

    def test_queue_list_default_exists(self, run_cli_command):
        """Test `verdi broker queue list` shows auto-initialized default queue."""
        # Note: get_queue_config() auto-initializes a default queue if none exists
        result = run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'list'], use_subprocess=False)
        assert 'default' in result.output

    def test_queue_create_default_already_exists(self, run_cli_command):
        """Test `verdi broker queue create` when default already auto-initialized."""
        # Default queue is auto-initialized by get_queue_config()
        result = run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'create', 'default'], use_subprocess=False)
        assert 'already exists' in result.output

    def test_queue_create_named(self, run_cli_command):
        """Test `verdi broker queue create` with a custom name."""
        result = run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'create', 'priority'], use_subprocess=False)
        assert 'Queue "priority" created successfully' in result.output

        queue_config = _get_queue_config()
        assert 'priority' in queue_config

    def test_queue_create_with_limits(self, run_cli_command):
        """Test `verdi broker queue create` with custom limits."""
        result = run_cli_command(
            cmd_verdi.verdi,
            ['broker', 'queue', 'create', 'custom', '50', '100'],
            use_subprocess=False,
        )
        assert 'Queue "custom" created successfully' in result.output

        queue_config = _get_queue_config()
        assert queue_config['custom']['root_workchain_prefetch'] == 50
        assert queue_config['custom']['calcjob_prefetch'] == 100

    def test_queue_list_multiple_queues(self, run_cli_command):
        """Test `verdi broker queue list` shows multiple queues."""
        # First create an additional queue
        run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'create', 'priority'], use_subprocess=False)

        result = run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'list'], use_subprocess=False)
        assert 'default' in result.output
        assert 'priority' in result.output

    def test_queue_set_root_workchain(self, run_cli_command):
        """Test `verdi broker queue set` for root-workchain limit."""
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'set', 'default', 'root-workchain', '100'], use_subprocess=False
        )
        assert 'Root WorkChains limit' in result.output
        assert '100' in result.output

        queue_config = _get_queue_config()
        assert queue_config['default']['root_workchain_prefetch'] == 100

    def test_queue_set_calcjob(self, run_cli_command):
        """Test `verdi broker queue set` for calcjob limit."""
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'set', 'default', 'calcjob', '50'], use_subprocess=False
        )
        assert 'CalcJobs limit' in result.output
        assert '50' in result.output

        queue_config = _get_queue_config()
        assert queue_config['default']['calcjob_prefetch'] == 50

    def test_queue_set_unlimited(self, run_cli_command):
        """Test `verdi broker queue set` with 0 (unlimited)."""
        # First set a non-zero limit
        run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'set', 'default', 'calcjob', '100'], use_subprocess=False
        )

        # Now set to unlimited
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'set', 'default', 'calcjob', '0'], use_subprocess=False
        )
        assert 'unlimited' in result.output

    def test_queue_set_nonexistent_queue(self, run_cli_command):
        """Test `verdi broker queue set` for non-existent queue."""
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'set', 'nonexistent', 'calcjob', '50'], raises=True, use_subprocess=False
        )
        assert 'does not exist' in result.output

    def test_queue_delete(self, run_cli_command):
        """Test `verdi broker queue delete`."""
        # First create an additional queue
        run_cli_command(cmd_verdi.verdi, ['broker', 'queue', 'create', 'priority'], use_subprocess=False)

        # Now delete it
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'delete', 'priority', '--force'], use_subprocess=False
        )
        assert 'deleted' in result.output

        queue_config = _get_queue_config()
        assert 'priority' not in queue_config
        assert 'default' in queue_config

    def test_queue_delete_default_fails(self, run_cli_command):
        """Test that deleting the 'default' queue fails."""
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'delete', 'default', '--force'], raises=True, use_subprocess=False
        )
        assert 'cannot be deleted' in result.output

    def test_queue_delete_nonexistent(self, run_cli_command):
        """Test deleting a non-existent queue."""
        result = run_cli_command(
            cmd_verdi.verdi, ['broker', 'queue', 'delete', 'nonexistent', '--force'], use_subprocess=False
        )
        assert 'does not exist' in result.output
