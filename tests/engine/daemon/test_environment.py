###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for the submission-time daemon drift check."""

import sys
from unittest.mock import Mock

import pytest

from aiida.common import callables
from aiida.engine.daemon import client, environment


@pytest.fixture
def daemon_environment(monkeypatch):
    """Provide a running daemon with the same Python and installed packages as this interpreter."""
    daemon = Mock(is_daemon_running=True)
    daemon._get_daemon_env_info.return_value = {'python_binary': sys.executable, 'packages': {}}
    monkeypatch.setattr(client, 'get_daemon_client', lambda: daemon)
    monkeypatch.setattr(client, 'get_daemon_import_paths', lambda: ('/worker',))
    monkeypatch.setattr(client.DaemonClient, '_get_package_version_snapshot', lambda: {})
    monkeypatch.setattr(callables, 'module_resolves_in', lambda module_name, search_paths: True)
    return daemon


def test_submission_accepts_matching_environment(daemon_environment):
    """A matching daemon allows submission."""
    assert environment.validate_submission_environment() is None


def test_submission_refuses_python_drift(daemon_environment):
    """The same Python binary check used by `verdi status` applies to submissions."""
    daemon_environment._get_daemon_env_info.return_value['python_binary'] = '/other/python'
    assert 'different Python binary' in environment.validate_submission_environment()


def test_submission_refuses_package_drift(daemon_environment):
    """The same package snapshot check used by `verdi status` applies to submissions."""
    daemon_environment._get_daemon_env_info.return_value['packages'] = {'aiida-core': {'version': 'old'}}
    assert 'Removed packages: aiida-core' in environment.validate_submission_environment()


def test_submission_refuses_other_installation(daemon_environment, monkeypatch):
    """Different AiiDA sources are unsafe even when the version metadata matches."""
    monkeypatch.setattr(callables, 'module_resolves_in', lambda module_name, search_paths: False)
    assert 'different AiiDA installation' in environment.validate_submission_environment()


def test_submission_without_running_daemon_is_not_refused(daemon_environment):
    """A stale environment file does not describe the next daemon to start."""
    daemon_environment.is_daemon_running = False
    assert environment.validate_submission_environment() is None
    daemon_environment._get_daemon_env_info.assert_not_called()
