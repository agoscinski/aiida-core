###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Persistent contact identity associated with a configured profile."""

from __future__ import annotations

import typing as t

from aiida.common import exceptions
from aiida.manage import get_manager

if t.TYPE_CHECKING:
    from aiida.storage.psql_dos.backend import PsqlDosBackend

__all__ = ('Profile',)


class Profile:
    """A persistent profile identity, independent of its contact email.

    This is distinct from :class:`aiida.manage.configuration.Profile`, which
    holds connection settings. Both profiles share the same UUID.
    """

    def __init__(self, *, backend: PsqlDosBackend | None = None) -> None:
        from aiida.storage.psql_dos.backend import PsqlDosBackend
        from aiida.storage.psql_dos.models.profile import DbProfile
        from aiida.storage.sqlite_zip.models import DbProfile as SqliteDbProfile

        selected_backend = backend or get_manager().get_profile_storage()
        if not isinstance(selected_backend, PsqlDosBackend):
            msg = 'ORM profiles are only available for persistent DOS storage.'
            raise TypeError(msg)
        self._backend = selected_backend
        model = (
            t.cast(type[DbProfile], SqliteDbProfile)
            if self._backend.profile.storage_backend == 'core.sqlite_dos'
            else DbProfile
        )
        row = self._backend.get_session().query(model).filter_by(uuid=self._backend.profile.uuid).one_or_none()
        if row is None:
            msg = f'No ORM profile with UUID {self._backend.profile.uuid} in storage.'
            raise exceptions.NotExistent(msg)
        self._row = row

    @property
    def pk(self) -> int:
        """Return the database primary key."""
        return t.cast(int, self._row.id)

    @property
    def uuid(self) -> str:
        """Return the configured profile's UUID."""
        return t.cast(str, self._row.uuid)

    @property
    def email(self) -> str:
        """Return the contact email (not an identifier)."""
        return t.cast(str, self._row.email)

    @email.setter
    def email(self, value: str) -> None:
        self._row.email = value

    @property
    def first_name(self) -> str:
        """Return the contact's first name."""
        return t.cast(str, self._row.first_name)

    @first_name.setter
    def first_name(self, value: str) -> None:
        self._row.first_name = value

    @property
    def last_name(self) -> str:
        """Return the contact's last name."""
        return t.cast(str, self._row.last_name)

    @last_name.setter
    def last_name(self, value: str) -> None:
        self._row.last_name = value

    @property
    def institution(self) -> str:
        """Return the contact's institution."""
        return t.cast(str, self._row.institution)

    @institution.setter
    def institution(self, value: str) -> None:
        self._row.institution = value

    def store(self) -> Profile:
        """Persist changes to contact information."""
        self._backend.get_session().commit()
        return self
