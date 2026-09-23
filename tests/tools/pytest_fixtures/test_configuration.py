"""Test the pytest fixtures."""

from pathlib import Path

import pytest

from aiida import orm
from aiida.manage.configuration import get_config, load_config
from aiida.manage.configuration.settings import DEFAULT_CONFIG_FILE_NAME

# This is needed when we run this file in isolation using
# the `--noconftest` pytest option in the 'test-pytest-fixtures' CI job.
pytest_plugins = ['aiida.tools.pytest_fixtures']


def test_aiida_config(tmp_path_factory):
    """Test that ``aiida_config`` fixture is loaded by default and creates a config instance in temp directory."""
    from aiida.manage.configuration import CONFIG

    config = get_config(create=False)
    assert config is CONFIG
    assert config.dirpath.startswith(str(tmp_path_factory.getbasetemp()))
    assert Path(config.dirpath, DEFAULT_CONFIG_FILE_NAME).is_file()
    assert config._default_profile


def test_aiida_config_file(tmp_path_factory):
    """Test that ``aiida_config`` fixture stores the configuration in a config file in a temp directory."""
    # Unlike get_config, load_config always loads the configuration from a file
    config = load_config(create=False)
    assert config.dirpath.startswith(str(tmp_path_factory.getbasetemp()))
    assert Path(config.dirpath, DEFAULT_CONFIG_FILE_NAME).is_file()
    assert config._default_profile


def test_aiida_config_tmp(aiida_config_tmp, tmp_path_factory):
    """Test that ``aiida_config_tmp`` returns a config instance in temp directory."""
    from aiida.manage.configuration.config import Config

    assert isinstance(aiida_config_tmp, Config)
    assert aiida_config_tmp.dirpath.startswith(str(tmp_path_factory.getbasetemp()))


def test_aiida_profile():
    """Test that ``aiida_profile`` fixture is loaded by default and loads a temporary test profile."""
    from aiida.manage.configuration import get_profile
    from aiida.manage.configuration.profile import Profile

    profile = get_profile()
    assert isinstance(profile, Profile)
    assert profile.is_test_profile


def test_aiida_profile_tmp(aiida_profile, aiida_profile_tmp):
    """Test that ``aiida_profile_tmp`` returns a new profile instance in temporary config directory."""
    from aiida.manage.configuration.profile import Profile

    assert isinstance(aiida_profile_tmp, Profile)
    assert aiida_profile_tmp.is_test_profile
    assert aiida_profile_tmp.uuid != aiida_profile.uuid


def test_profile_reset_storage_clears_data(aiida_profile_tmp):
    """Test that resetting a profile clears its storage but keeps its identity."""
    profile_uuid = aiida_profile_tmp.uuid
    node = orm.Data().store()
    node_pk = node.pk
    assert orm.QueryBuilder().append(orm.Data, filters={'id': node_pk}).count() == 1

    aiida_profile_tmp.reset_storage()

    assert aiida_profile_tmp.uuid == profile_uuid
    assert orm.QueryBuilder().append(orm.Data, filters={'id': node_pk}).count() == 0


def test_profile_reset_storage_isolates_inactive_profile(aiida_config, aiida_profile_factory):
    """Test that resetting an inactive profile leaves the active storage unchanged."""
    with aiida_profile_factory(aiida_config) as active_profile:
        active_node_pk = orm.Data().store().pk
        with aiida_profile_factory(aiida_config) as inactive_profile:
            orm.Data().store()

        inactive_profile.reset_storage()

        assert orm.QueryBuilder().append(orm.Data, filters={'id': active_node_pk}).count() == 1
        assert active_profile.uuid != inactive_profile.uuid


def test_profile_reset_storage_waits_for_daemon_to_stop(aiida_config, aiida_profile_factory, monkeypatch):
    """Test that storage reset waits for a daemon shutdown to complete."""

    class DaemonClient:
        def __init__(self, profile):
            self.running_states = iter((True, True, False))

        @property
        def is_daemon_running(self):
            return next(self.running_states)

        def stop_daemon(self, *, wait):
            assert wait is True

    sleep_calls = []
    monkeypatch.setattr('aiida.engine.daemon.client.DaemonClient', DaemonClient)
    monkeypatch.setattr('aiida.tools.pytest_fixtures.configuration.time.monotonic', lambda: 0)
    monkeypatch.setattr('aiida.tools.pytest_fixtures.configuration.time.sleep', sleep_calls.append)

    with aiida_profile_factory(aiida_config, broker_backend='core.zeromq') as profile:
        profile.reset_storage()

    assert sleep_calls == [0.1]


def test_profile_reset_storage_raises_if_daemon_does_not_stop(aiida_config, aiida_profile_factory, monkeypatch):
    """Test that storage is not cleared while the daemon remains running."""
    from aiida.engine.daemon.client import DaemonTimeoutException

    class DaemonClient:
        def __init__(self, profile):
            pass

        @property
        def is_daemon_running(self):
            return True

        def stop_daemon(self, *, wait):
            assert wait is True

    monotonic_times = iter((0, 5.1))
    monkeypatch.setattr('aiida.engine.daemon.client.DaemonClient', DaemonClient)
    monkeypatch.setattr('aiida.tools.pytest_fixtures.configuration.time.monotonic', lambda: next(monotonic_times))

    with aiida_profile_factory(aiida_config, broker_backend='core.zeromq') as profile:
        with pytest.raises(DaemonTimeoutException, match='failed to stop before resetting storage'):
            profile.reset_storage()


@pytest.mark.requires_psql
def test_aiida_profile_factory_psql_dos(aiida_config, aiida_profile_factory, config_psql_dos):
    """Test that the factory creates and resets a ``core.psql_dos`` profile."""
    with aiida_profile_factory(
        aiida_config,
        storage_backend='core.psql_dos',
        storage_config=config_psql_dos(),
    ) as profile:
        assert profile.storage_backend == 'core.psql_dos'
        assert profile.storage_cls.version_profile(profile) == profile.storage_cls.version_head()


def test_aiida_profile_factory_unsupported_broker(aiida_config_tmp, aiida_profile_factory):
    """Test that ``aiida_profile_factory`` raises for a broker backend without a default configuration."""
    with pytest.raises(ValueError, match='Unsupported broker backend: core\\.unsupported'):
        with aiida_profile_factory(aiida_config_tmp, broker_backend='core.unsupported'):
            pass
