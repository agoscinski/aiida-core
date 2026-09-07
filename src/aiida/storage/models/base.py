###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Shared SQLAlchemy model utilities."""

from sqlalchemy import inspect


class Model:
    """Base ORM model."""

    def __repr__(self) -> str:
        """Return a representation of the row columns."""
        string = f'<{self.__class__.__name__}'
        for col in self.__table__.columns:  # type: ignore[attr-defined]
            col_name = '_metadata' if col.name == 'metadata' else col.name
            val = f'{getattr(self, col_name)!r}'
            if len(val) > 10:
                val = val[:10] + '...'
            string += f' {col_name}={val},'
        return string + '>'


def instant_defaults_listener(target, args, kwargs):
    """Populate Python-side column defaults when an ORM instance is constructed."""
    assert not args, f'args are not allowed in {target} instantiation'
    if 'metadata' in kwargs:
        kwargs['_metadata'] = kwargs.pop('metadata')
    for column_name in ('attributes', 'extras', '_metadata'):
        if column_name in kwargs and kwargs[column_name] is None:
            kwargs[column_name] = {}
    columns = inspect(target.__class__).columns
    if 'mtime' in columns and 'mtime' not in kwargs:
        kwargs['mtime'] = None
    for key, column in columns.items():
        if key not in kwargs and hasattr(column, 'default') and column.default is not None:
            kwargs[key] = column.default.arg(target) if callable(column.default.arg) else column.default.arg


naming_convention = {
    'pk': '%(table_name)s_pkey',
    'ix': 'ix_%(table_name)s_%(column_0_N_label)s',
    'uq': 'uq_%(table_name)s_%(column_0_N_name)s',
    'ck': 'ck_%(table_name)s_%(constraint_name)s',
    'fk': 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s',
}
