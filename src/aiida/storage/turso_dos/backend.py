###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Storage implementation using a Turso/libSQL database and disk-objectstore container."""

from __future__ import annotations

from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from disk_objectstore import Container
from aiida.common.log import AIIDA_LOGGER
from aiida.common.pydantic import AiiDABaseModel, MetadataField
from aiida.manage.configuration.profile import Profile
from aiida.manage.configuration.settings import AiiDAConfigDir
from aiida.storage.psql_dos.backend import get_filepath_container
from aiida.storage.sqlite_dos.backend import SqliteDosMigrator, SqliteDosStorage
from aiida.storage.sqlite_zip.backend import validate_sqlite_version

from .utils import create_sqlalchemy_engine

if TYPE_CHECKING:
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend

__all__ = ('TursoDosStorage',)

LOGGER = AIIDA_LOGGER.getChild(__file__)


class TursoDosMigrator(SqliteDosMigrator):
    """Class for validating and migrating `turso_dos` storage instances."""

    def __init__(self, profile: Profile) -> None:
        self.profile = profile
        self._engine = create_sqlalchemy_engine(self.profile.storage_config)  # type: ignore[arg-type]
        self._connection = None

    def get_container(self) -> Container:
        """Return the disk-object store container."""
        return Container(str(get_filepath_container(self.profile)))


class TursoDosStorage(SqliteDosStorage):
    """Storage using a Turso/libSQL database with a local disk-objectstore repository."""

    migrator = TursoDosMigrator

    class CliModel(AiiDABaseModel):
        """Model describing required information to configure an instance of the storage."""

        database_url: str = MetadataField(
            title='Turso/libSQL database URL',
            description='SQLAlchemy database URL for the Turso/libSQL database.',
        )
        auth_token: str | None = MetadataField(
            title='Turso auth token',
            description='Authentication token for the Turso/libSQL database.',
            default=None,
        )
        repository_uri: str = MetadataField(
            title='File repository URI',
            description='URI to the local file repository.',
            default_factory=lambda: (AiiDAConfigDir.get() / 'repository' / f'turso_dos_{uuid4().hex}').as_uri(),
        )

    @property
    def filepath_root(self) -> Path:
        return get_filepath_container(self.profile).parent

    @property
    def filepath_container(self) -> Path:
        return get_filepath_container(self.profile)

    @classmethod
    def initialise(cls, profile: Profile, reset: bool = False) -> bool:
        validate_sqlite_version()
        filepath = get_filepath_container(profile).parent
        filepath.mkdir(parents=True, exist_ok=True)

        with cls.migrator(profile) as migrator:
            return migrator.initialise(reset=reset)

    def __str__(self) -> str:
        state = 'closed' if self.is_closed else 'open'
        database_url = self.profile.storage_config.get('database_url')
        return f'TursoDosStorage[{database_url!r}, {self.filepath_root}]: {state},'

    def _initialise_session(self) -> None:
        """Initialise the SQLAlchemy session factory."""
        from sqlalchemy.orm import scoped_session, sessionmaker

        engine = create_sqlalchemy_engine(self._profile.storage_config)  # type: ignore[arg-type]
        self._session_factory = scoped_session(sessionmaker(bind=engine, future=True, expire_on_commit=True))

    def delete(self) -> None:  # type: ignore[override]
        """Delete the repository and clear the remote database."""
        if not self.is_closed:
            self.close()

        with self.migrator(self.profile) as migrator:
            migrator.reset_database()
            migrator.reset_repository()

        if self.filepath_root.exists():
            rmtree(self.filepath_root)
            LOGGER.report(f'Deleted storage directory at `{self.filepath_root}`.')

    def get_container(self) -> Container:
        return Container(str(self.filepath_container))

    def get_repository(self) -> 'DiskObjectStoreRepositoryBackend':
        from aiida.repository.backend import DiskObjectStoreRepositoryBackend

        return DiskObjectStoreRepositoryBackend(container=self.get_container())

    def _backup_storage(self, *args: Any, **kwargs: Any) -> None:
        msg = 'Backups are not yet implemented for the `core.turso_dos` storage backend.'
        raise NotImplementedError(msg)
