###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Schema validation and migration utilities.

This code interacts directly with the database, outside of the ORM,
taking a `Profile` as input for the connection configuration.

.. important:: This code should only be accessed via the storage backend class, not directly!
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alembic.config import Config
from sqlalchemy import Engine, MetaData, String, column, desc, inspect, select, table
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from aiida.common import exceptions
from aiida.storage.log import MIGRATE_LOGGER
from aiida.storage.migrator import AlembicMigrator, BaseDosMigrator
from aiida.storage.psql_dos.utils import create_sqlalchemy_engine

if TYPE_CHECKING:
    from disk_objectstore import Container

TEMPLATE_LEGACY_DJANGO_SCHEMA = """
Database schema is using the legacy Django schema.
To migrate the database schema version to the current one, run the following command:

    verdi -p {profile_name} storage migrate
"""


REPOSITORY_UUID_KEY = BaseDosMigrator.repository_uuid_key


def _get_orm_metadata() -> MetaData:
    """Return the PostgreSQL ORM metadata without importing models at module load."""
    from aiida.storage.psql_dos.models.base import get_orm_metadata

    return get_orm_metadata()


_ALEMBIC_MIGRATOR = AlembicMigrator(Path(__file__).resolve().parent / 'migrations', _get_orm_metadata)


class PsqlDosMigrator(BaseDosMigrator):
    """Class for validating and migrating `psql_dos` storage instances.

    .. important:: This class should only be accessed via the storage backend class (apart from for test purposes)
    """

    alembic_migrator = _ALEMBIC_MIGRATOR
    django_version_table = table(
        'django_migrations', column('id'), column('app', String(255)), column('name', String(255)), column('applied')
    )

    def _create_engine(self) -> Engine:
        return create_sqlalchemy_engine(self.profile.storage_config)  # type: ignore[arg-type]

    @property
    def orm_metadata(self) -> MetaData:
        from aiida.storage.psql_dos.models.base import get_orm_metadata

        return get_orm_metadata()

    @contextlib.contextmanager
    def _alembic_connect(self) -> Iterator[Config]:
        with self.alembic_migrator._alembic_connect(self.connection, profile=self.profile) as config:
            yield config

    def get_schema_version_profile(self, check_legacy: bool = False) -> str | None:
        """Return the schema version of the backend instance for this profile.

        Note, the version will be None if the database is empty or is a legacy django database.
        """
        with self._migration_context() as context:
            version = context.get_current_revision()
        if version is None and check_legacy:
            stmt = select(self.django_version_table.c.name).where(self.django_version_table.c.app == 'db')
            stmt = stmt.order_by(desc(self.django_version_table.c.id)).limit(1)
            try:
                return self.connection.execute(stmt).scalar()
            except (OperationalError, ProgrammingError):
                self.connection.rollback()
        return version

    def _check_version_table(self) -> None:
        # A missing alembic version table might mean this is a legacy django database.
        if not inspect(self.connection).has_table(self.alembic_version_tbl_name):
            if inspect(self.connection).has_table(self.django_version_table.name):
                raise exceptions.IncompatibleStorageSchema(
                    TEMPLATE_LEGACY_DJANGO_SCHEMA.format(profile_name=self.profile.name)
                )
            raise exceptions.IncompatibleStorageSchema('The database has no known version.')

    def get_container(self) -> Container:
        """Return the disk-object store container.

        :returns: The disk-object store container configured for the repository path of the current profile.
        """
        from disk_objectstore import Container

        from aiida.storage.psql_dos.backend import get_filepath_container

        return Container(get_filepath_container(self.profile))

    @property
    def is_database_initialised(self) -> bool:
        """Return whether the database is initialised.

        This is the case if it contains the table that holds the schema version for alembic or Django.
        """
        return inspect(self.connection).has_table(self.alembic_version_tbl_name) or inspect(self.connection).has_table(
            self.django_version_table.name
        )

    def initialise_repository(self) -> None:
        """Initialise the repository."""
        from aiida.storage.psql_dos.backend import CONTAINER_DEFAULTS

        container = self.get_container()
        container.init_container(clear=True, **CONTAINER_DEFAULTS)

    def initialise_database(self) -> None:
        # setup the database
        # see: https://alembic.sqlalchemy.org/en/latest/cookbook.html#building-an-up-to-date-database-from-scratch
        MIGRATE_LOGGER.report('initialising empty storage schema')
        super().initialise_database()

    def _migrate_legacy_branches(self) -> None:
        # The database can be in one of a few states:
        # 1. Legacy django database -> we transfer the version to alembic, migrate to the head of the django branch,
        #    reset the revision as one on the main branch, and then migrate to the head of the main branch
        # 2. Legacy sqlalchemy database -> we migrate to the head of the sqlalchemy branch,
        #    reset the revision as one on the main branch, and then migrate to the head of the main branch
        # 3. Already on the main branch -> nothing to do here, the caller migrates to the head of the main branch

        if inspect(self.connection).has_table(self.alembic_version_tbl_name):
            version = self.get_schema_version_profile()
        else:
            # the database is a legacy django one,
            # so we need to copy the version from the 'django_migrations' table to the 'alembic_version' one
            legacy_version = self.get_schema_version_profile(check_legacy=True)
            if legacy_version is None:
                raise exceptions.StorageMigrationError(
                    'No schema version could be read from the database. '
                    "Check that either the 'alembic_version' or 'django_migrations' tables "
                    'are present and accessible, using e.g. `verdi devel run-sql "SELECT * FROM alembic_version"`'
                )
            # the version should be of the format '00XX_description'
            version = f'django_{legacy_version[:4]}'
            with self._migration_context() as context:
                assert context.script is not None
                context.stamp(context.script, version)
                self.connection.commit()
            # now we can continue with the migration as normal

        # find what branch the current version is on
        revisions = self._alembic_script().revision_map.get_revision(version)
        branches = revisions.branch_labels if revisions else set()

        if 'django' in branches or 'sqlalchemy' in branches:
            # migrate up to the top of the respective legacy branches
            if 'django' in branches:
                MIGRATE_LOGGER.report('Migrating to the head of the legacy django branch')
                self.migrate_up('django@head')
            elif 'sqlalchemy' in branches:
                MIGRATE_LOGGER.report('Migrating to the head of the legacy sqlalchemy branch')
                self.migrate_up('sqlalchemy@head')
            # now re-stamp with the comparable revision on the main branch
            with self._migration_context() as context:
                context._ensure_version_table(purge=True)
                assert context.script is not None
                context.stamp(context.script, 'main_0001')
                self.connection.commit()

    # the following are used for migration tests

    @contextlib.contextmanager
    def session(self) -> Iterator[Session]:
        """Context manager to return a session for the database."""
        session = Session(self._engine, future=True)
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_current_table(self, table_name: str) -> Any:
        """Return a table instantiated at the correct migration.

        Note that this is obtained by inspecting the database and not by looking into the models file.
        So, special methods possibly defined in the models files/classes are not present.
        """
        base = automap_base()
        base.prepare(autoload_with=self.connection.engine)
        return getattr(base.classes, table_name)
