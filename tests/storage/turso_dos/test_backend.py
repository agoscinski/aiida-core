"""Tests for :mod:`aiida.storage.turso_dos.backend`."""

from unittest.mock import MagicMock

import pytest

from aiida.storage.turso_dos.backend import TursoDosStorage
from aiida.storage.turso_dos.utils import create_sqlalchemy_engine


@pytest.mark.usefixtures('chdir_tmp_path')
def test_model():
    """Test :class:`aiida.storage.turso_dos.backend.TursoDosStorage.CliModel`."""
    model = TursoDosStorage.CliModel(database_url='libsql://example.turso.io')
    assert model.repository_uri.startswith('file://')


def test_create_sqlalchemy_engine(monkeypatch):
    """Test :func:`aiida.storage.turso_dos.utils.create_sqlalchemy_engine`."""
    mock_create_engine = MagicMock()
    monkeypatch.setattr('sqlalchemy.create_engine', mock_create_engine)

    create_sqlalchemy_engine({'database_url': 'sqlite:///test.db', 'auth_token': 'token'})

    mock_create_engine.assert_called_once()
    _, kwargs = mock_create_engine.call_args
    assert kwargs['connect_args'] == {'auth_token': 'token'}


def test_initialise_version_check(monkeypatch):
    """Test :meth:`aiida.storage.turso_dos.backend.TursoDosStorage.initialise`."""
    mock_validate = MagicMock()
    monkeypatch.setattr('aiida.storage.turso_dos.backend.validate_sqlite_version', mock_validate)

    with pytest.raises(AttributeError):
        TursoDosStorage.initialise('')

    mock_validate.assert_called_once()
