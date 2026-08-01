"""Tests for :mod:`aiida.storage.turso_dos.backend`."""

from unittest.mock import MagicMock

import pytest

from aiida.storage.turso_dos.backend import TursoDosStorage
from aiida.storage.turso_dos.utils import create_sqlalchemy_engine


@pytest.mark.usefixtures('chdir_tmp_path')
def test_model():
    """Test :class:`aiida.storage.turso_dos.backend.TursoDosStorage.CliModel`."""
    model = TursoDosStorage.CliModel(database_url='sqlite+turso:///example.db')
    assert model.repository_uri.startswith('file://')


def test_create_sqlalchemy_engine_sqlite(monkeypatch):
    """Test :func:`aiida.storage.turso_dos.utils.create_sqlalchemy_engine` with pysqlite."""
    mock_create_engine = MagicMock()
    mock_listen = MagicMock()
    monkeypatch.setattr('sqlalchemy.create_engine', mock_create_engine)
    monkeypatch.setattr('sqlalchemy.event.listen', mock_listen)

    create_sqlalchemy_engine({'database_url': 'sqlite:///test.db'})

    mock_create_engine.assert_called_once()
    assert mock_listen.call_count == 5
    _, kwargs = mock_create_engine.call_args
    assert 'connect_args' not in kwargs


def test_create_sqlalchemy_engine_pyturso(monkeypatch):
    """Test :func:`aiida.storage.turso_dos.utils.create_sqlalchemy_engine` with pyturso."""
    mock_create_engine = MagicMock()
    mock_import_module = MagicMock()
    mock_listen = MagicMock()
    monkeypatch.setattr('sqlalchemy.create_engine', mock_create_engine)
    monkeypatch.setattr('sqlalchemy.event.listen', mock_listen)
    monkeypatch.setattr('importlib.import_module', mock_import_module)

    create_sqlalchemy_engine({'database_url': 'sqlite+turso:///test.db'})

    mock_import_module.assert_called_once_with('turso.sqlalchemy')
    mock_create_engine.assert_called_once()
    _, kwargs = mock_create_engine.call_args
    assert kwargs['poolclass'].__name__ == 'NullPool'


def test_create_sqlalchemy_engine_auth_token(monkeypatch):
    """Test that auth tokens are only passed for pyturso sync URLs."""
    mock_create_engine = MagicMock()
    monkeypatch.setattr('sqlalchemy.create_engine', mock_create_engine)
    monkeypatch.setattr('sqlalchemy.event.listen', MagicMock())
    monkeypatch.setattr('importlib.import_module', MagicMock())

    create_sqlalchemy_engine(
        {
            'database_url': 'sqlite+turso_sync:///test.db?remote_url=https://example.turso.io',
            'auth_token': 'token',
        }
    )

    _, kwargs = mock_create_engine.call_args
    assert kwargs['connect_args'] == {'auth_token': 'token'}

    with pytest.raises(ValueError, match='auth_token'):
        create_sqlalchemy_engine({'database_url': 'sqlite+turso:///test.db', 'auth_token': 'token'})


def test_create_sqlalchemy_engine_unsupported_libsql():
    """Test that the old sqlalchemy-libsql URL schemes are rejected."""
    with pytest.raises(ValueError, match='sqlalchemy-libsql'):
        create_sqlalchemy_engine({'database_url': 'sqlite+libsql:///test.db'})


def test_initialise_version_check(monkeypatch):
    """Test :meth:`aiida.storage.turso_dos.backend.TursoDosStorage.initialise`."""
    mock_validate = MagicMock()
    monkeypatch.setattr('aiida.storage.turso_dos.backend.validate_sqlite_version', mock_validate)

    with pytest.raises(AttributeError):
        TursoDosStorage.initialise('')

    mock_validate.assert_called_once()
