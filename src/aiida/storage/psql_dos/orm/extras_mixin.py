###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Mixin class for SQL implementations of ``extras``."""

import typing as t
from collections.abc import Iterable

from aiida.orm.implementation.utils import clean_value, validate_attribute_extra_key


class ExtrasMixin:
    """Mixin class for SQL implementations of ``extras``."""

    model: t.Any
    bare_model: t.Any
    is_stored: bool

    @property
    def extras(self) -> dict[str, t.Any]:
        return self.model.extras

    def get_extra(self, key: str) -> t.Any:
        try:
            return self.model.extras[key]
        except KeyError as exception:
            msg = f'extra `{exception}` does not exist'
            raise AttributeError(msg) from exception

    def set_extra(self, key: str, value: t.Any) -> None:
        validate_attribute_extra_key(key)

        if self.is_stored:
            value = clean_value(value)

        extras = self.model.extras
        extras[key] = value
        self.model.extras = extras

    def set_extra_many(self, extras: dict[str, t.Any]) -> None:
        for key in extras:
            validate_attribute_extra_key(key)

        if self.is_stored:
            extras = {key: clean_value(value) for key, value in extras.items()}

        values = self.model.extras
        values.update(extras)
        self.model.extras = values

    def reset_extras(self, extras: dict[str, t.Any]) -> None:
        for key in extras:
            validate_attribute_extra_key(key)

        if self.is_stored:
            extras = clean_value(extras)

        self.model.extras = extras

    def delete_extra(self, key: str) -> None:
        extras = self.model.extras
        try:
            extras.pop(key)
        except KeyError as exception:
            msg = f'extra `{exception}` does not exist'
            raise AttributeError(msg) from exception
        self.model.extras = extras

    def delete_extra_many(self, keys: Iterable[str]) -> None:
        non_existing_keys = [key for key in keys if key not in self.model.extras]

        if non_existing_keys:
            msg = f'extras `{", ".join(non_existing_keys)}` do not exist'
            raise AttributeError(msg)

        values = self.model.extras
        for key in keys:
            values.pop(key)
        self.model.extras = values

    def clear_extras(self) -> None:
        self.model.extras = {}

    def extras_items(self) -> Iterable[tuple[str, t.Any]]:
        yield from self.model.extras.items()

    def extras_keys(self) -> Iterable[str]:
        yield from self.model.extras.keys()
