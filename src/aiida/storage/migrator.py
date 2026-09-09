###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
###########################################################################
"""Shared Alembic migration drivers."""

from __future__ import annotations

import abc
import contextlib
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from alembic.command import downgrade, stamp, upgrade
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.runtime.migration import MigrationContext, MigrationInfo
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, Engine, MetaData, insert, inspect, select
from sqlalchemy.exc import OperationalError

from aiida.common import exceptions
from aiida.storage.log import MIGRATE_LOGGER
from aiida.storage.migrations import TEMPLATE_INVALID_SCHEMA_VERSION

if TYPE_CHECKING:
    from types import TracebackType

    from disk_objectstore import Container
    from typing_extensions import Self

    from aiida.manage.configuration.profile import Profile


class AlembicMigrator:
    """Alembic driver for one migration script directory.

    The driver owns no database resources. Callers supply an open connection,
    allowing profile storage backends and archive migrations to use it alike.
    """

    def __init__(self, script_location: Path, target_metadata: Callable[[], MetaData]) -> None:
        self._script_location = script_location
        self._target_metadata = target_metadata

    def _alembic_config(self) -> Config:
        """Return an Alembic configuration for the migration directory."""
        config = Config()
        config.set_main_option('script_location', str(self._script_location))
        return config

    def _alembic_script(self) -> ScriptDirectory:
        """Return the Alembic script directory."""
        return ScriptDirectory.from_config(self._alembic_config())

    def get_schema_versions(self) -> dict[str, str]:
        """Return all Alembic schema versions, from oldest to latest."""
        return {entry.revision: entry.doc for entry in reversed(list(self._alembic_script().walk_revisions()))}

    def get_schema_version_head(self) -> str:
        """Return the head of the ``main`` migration branch."""
        return self._alembic_script().revision_map.get_current_head('main') or ''

    @contextlib.contextmanager
    def _alembic_connect(self, connection: Connection, *, profile: Profile | None = None) -> Iterator[Config]:
        """Configure Alembic to use an existing connection."""
        config = self._alembic_config()
        config.attributes['connection'] = connection
        config.attributes['aiida_profile'] = profile
        config.attributes['target_metadata'] = self._target_metadata()

        def callback(step: MigrationInfo, **kwargs: Any) -> None:
            from_revision = step.down_revision_ids[0] if step.down_revision_ids else '<base>'
            MIGRATE_LOGGER.report(f'- {from_revision} -> {step.up_revision_id}')

        config.attributes['on_version_apply'] = callback
        yield config

    @contextlib.contextmanager
    def migration_context(
        self, connection: Connection, *, profile: Profile | None = None
    ) -> Iterator[MigrationContext]:
        """Return a migration context configured for an existing connection."""
        with self._alembic_connect(connection, profile=profile) as config:
            script = ScriptDirectory.from_config(config)
            with EnvironmentContext(config, script) as environment:
                environment.configure(connection)
                yield environment.get_context()

    def migrate_up(self, connection: Connection, version: str, *, profile: Profile | None = None) -> None:
        """Upgrade an existing connection to ``version``."""
        with self._alembic_connect(connection, profile=profile) as config:
            upgrade(config, version)

    def migrate_down(self, connection: Connection, version: str, *, profile: Profile | None = None) -> None:
        """Downgrade an existing connection to ``version``."""
        with self._alembic_connect(connection, profile=profile) as config:
            downgrade(config, version)

    def stamp(self, connection: Connection, version: str, *, profile: Profile | None = None) -> None:
        """Stamp an existing connection with ``version`` without running migrations."""
        with self._alembic_connect(connection, profile=profile) as config:
            stamp(config, version)


class BaseDosMigrator(abc.ABC):
    """Shared lifecycle for profile-bound disk-objectstore migrators.

    This composes an :class:`AlembicMigrator` driver with the engine/connection lifecycle and the
    initialise/validate/migrate policy shared by the ``psql_dos`` and ``sqlite_dos`` storage backends.
    The archive (``sqlite_zip``) migrator stays out: it is file-bound, not profile-bound.

    Subclasses provide the engine, the Alembic driver instance, the repository container and the ORM
    metadata; everything else is shared. Legacy (non-``main`` branch) handling is a hook that only
    ``psql_dos`` overrides.

    .. important:: This class should only be accessed via the storage backend class, not directly!
    """

    alembic_version_tbl_name = 'alembic_version'
    repository_uuid_key = 'repository|uuid'

    alembic_migrator: ClassVar[AlembicMigrator]
    """The Alembic driver for the backend migration graph, set by subclasses."""

    def __init__(self, profile: Profile) -> None:
        self.profile = profile
        self._engine: Engine | None = self._create_engine()
        self._connection: Connection | None = None

    @abc.abstractmethod
    def _create_engine(self) -> Engine:
        """Create a new SQLAlchemy engine for the profile database."""

    @property
    @abc.abstractmethod
    def orm_metadata(self) -> MetaData:
        """Return the ORM metadata used to create a fresh database schema."""

    @abc.abstractmethod
    def get_container(self) -> Container:
        """Return the disk-objectstore container configured for the current profile."""

    def close(self) -> None:
        """Close the connection if it was opened and dispose of the engine."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    @property
    def connection(self) -> Connection:
        """Return an open connection to the profile database."""
        if self._connection is None:
            if self._engine is None:
                self._engine = self._create_engine()
            try:
                self._connection = self._engine.connect()
            except OperationalError as exception:
                raise exceptions.UnreachableStorage(f'Could not connect to database: {exception}') from exception
        return self._connection

    @classmethod
    def _alembic_config(cls) -> Config:
        """Return the Alembic configuration for the backend migration graph."""
        return cls.alembic_migrator._alembic_config()

    @classmethod
    def _alembic_script(cls) -> ScriptDirectory:
        """Return the Alembic script directory for the backend migration graph."""
        return cls.alembic_migrator._alembic_script()

    @classmethod
    def get_schema_versions(cls) -> dict[str, str]:
        """Return all available schema versions, from oldest to latest."""
        return cls.alembic_migrator.get_schema_versions()

    @classmethod
    def get_schema_version_head(cls) -> str:
        """Return the latest schema version available for this storage."""
        return cls.alembic_migrator.get_schema_version_head()

    @contextlib.contextmanager
    def _migration_context(self) -> Iterator[MigrationContext]:
        with self.alembic_migrator.migration_context(self.connection, profile=self.profile) as context:
            yield context

    def migrate_up(self, version: str) -> None:
        self.alembic_migrator.migrate_up(self.connection, version, profile=self.profile)

    def migrate_down(self, version: str) -> None:
        self.alembic_migrator.migrate_down(self.connection, version, profile=self.profile)

    def get_repository_uuid(self) -> str:
        """Return the UUID of the configured disk-objectstore container."""
        try:
            return self.get_container().container_id
        except Exception as exception:
            raise exceptions.UnreachableStorage(
                f'Could not access disk-objectstore {self.get_container()}: {exception}'
            ) from exception

    def get_schema_version_profile(self, check_legacy: bool = False) -> str | None:
        """Return the schema version of the backend instance for this profile.

        Note, the version will be None if the database is empty. The ``check_legacy`` flag is only
        honored by backends with legacy (non-``main`` branch) version tables, such as ``psql_dos``.
        """
        with self._migration_context() as context:
            return context.get_current_revision()

    def initialise(self, reset: bool = False) -> bool:
        """Initialise the repository and database, then migrate to the head."""
        if reset:
            self.reset_repository()
            self.reset_database()

        initialised = False
        if not self.is_initialised:
            self.initialise_repository()
            self.initialise_database()
            initialised = True

        # Call migrate in the case the storage was already initialised but not yet at the latest schema version. If it
        # was, then the following is a no-op anyway.
        self.migrate()
        return initialised

    @property
    def is_initialised(self) -> bool:
        """Return whether both the repository and database are initialised."""
        return self.is_repository_initialised and self.is_database_initialised

    @property
    def is_repository_initialised(self) -> bool:
        """Return whether the disk-objectstore container is initialised."""
        return self.get_container().is_initialised

    @property
    def is_database_initialised(self) -> bool:
        """Return whether the database is initialised.

        This is the case if it contains the table that holds the schema version for alembic.
        """
        return inspect(self.connection).has_table(self.alembic_version_tbl_name)

    def reset_repository(self) -> None:
        """Delete the disk-objectstore container contents."""
        try:
            shutil.rmtree(self.get_container().get_folder())
        except FileNotFoundError:
            pass

    def reset_database(self) -> None:
        """Delete all database contents except the Alembic version table."""
        self.delete_all_tables(exclude_tables=[self.alembic_version_tbl_name])

    def initialise_repository(self) -> None:
        """Initialise the disk-objectstore container."""
        self.get_container().init_container(
            clear=True,
            pack_size_target=4 * 1024 * 1024 * 1024,
            loose_prefix_len=2,
            hash_type='sha256',
            compression_algorithm='zlib+1',
        )

    def delete_all_tables(self, *, exclude_tables: list[str] | None = None) -> None:
        """Delete all reflected schema tables except the requested exclusions."""
        if not inspect(self.connection).has_table(self.alembic_version_tbl_name):
            return

        metadata = MetaData()
        metadata.reflect(bind=self.connection)
        for schema_table in reversed(metadata.sorted_tables):
            if schema_table.name not in (exclude_tables or []):
                self.connection.execute(schema_table.delete())
        self.connection.commit()

    def initialise_database(self) -> None:
        """Initialise the database.

        This assumes that the database has no schema whatsoever and so the initial schema is created directly from the
        models at the current head version without migrating through all of them one by one.
        """
        assert self._engine is not None
        self.orm_metadata.create_all(self._engine)

        from aiida.storage.psql_dos.models.settings import DbSetting

        repository_uuid = self.get_repository_uuid()

        # Create a "sync" between the database and repository, by saving its UUID in the settings table
        # this allows us to validate inconsistencies between the two
        self.connection.execute(
            insert(DbSetting).values(key=self.repository_uuid_key, val=repository_uuid, description='Repository UUID')
        )

        # finally, generate the version table, "stamping" it with the most recent revision
        with self._migration_context() as context:
            assert context.script is not None
            context.stamp(context.script, 'main@head')
            self.connection.commit()

    def _check_version_table(self) -> None:
        """Raise if the database has no recognisable schema version table."""
        if not inspect(self.connection).has_table(self.alembic_version_tbl_name):
            raise exceptions.IncompatibleStorageSchema('The database has no known version.')

    def validate_storage(self) -> None:
        """Validate that the storage for this profile

        1. That the database schema is at the head version, i.e. is compatible with the code API.
        2. That the repository ID is equal to the UUID set in the database

        :raises: :class:`aiida.common.exceptions.UnreachableStorage` if the storage cannot be connected to
        :raises: :class:`aiida.common.exceptions.IncompatibleStorageSchema`
            if the storage is not compatible with the code API.
        :raises: :class:`aiida.common.exceptions.CorruptStorage`
            if the repository ID is not equal to the UUID set in thedatabase.
        """
        self._check_version_table()

        # now we can check that the alembic version is the latest
        schema_version_code = self.get_schema_version_head()
        schema_version_database = self.get_schema_version_profile()
        if schema_version_database != schema_version_code:
            raise exceptions.IncompatibleStorageSchema(
                TEMPLATE_INVALID_SCHEMA_VERSION.format(
                    schema_version_database=schema_version_database,
                    schema_version_code=schema_version_code,
                    profile_name=self.profile.name,
                )
            )

        # finally, we check that the ID set within the disk-objectstore is equal to the one saved in the database,
        # i.e. this container is indeed the one associated with the db
        from aiida.storage.psql_dos.models.settings import DbSetting

        repository_uuid = self.get_repository_uuid()
        stmt = select(DbSetting.val).where(DbSetting.key == self.repository_uuid_key)
        database_repository_uuid = self.connection.execute(stmt).scalar_one_or_none()
        if database_repository_uuid is None:
            raise exceptions.CorruptStorage('The database has no repository UUID set.')
        if database_repository_uuid != repository_uuid:
            raise exceptions.CorruptStorage(
                f'The database has a repository UUID configured to {database_repository_uuid} '
                f"but the disk-objectstore's is {repository_uuid}."
            )

    def migrate(self) -> None:
        """Migrate the storage for this profile to the head version.

        :raises: :class:`~aiida.common.exceptions.UnreachableStorage` if the storage cannot be accessed.
        :raises: :class:`~aiida.common.exceptions.StorageMigrationError` if the storage is not initialised.
        """
        if not self.is_database_initialised:
            raise exceptions.StorageMigrationError('storage is uninitialised, cannot migrate.')
        self._migrate_legacy_branches()
        MIGRATE_LOGGER.report('Migrating to the head of the main branch')
        self.migrate_up('main@head')
        self.connection.commit()

    def _migrate_legacy_branches(self) -> None:
        """Migrate any legacy branches onto the main branch; a no-op unless overridden."""
