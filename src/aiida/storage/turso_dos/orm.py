###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""ORM implementation for the Turso storage backend."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import select

from aiida.common.links import LinkType
from aiida.orm.implementation import BackendNode
from aiida.storage.psql_dos.orm.querybuilder.joiner import SqlaJoiner
from aiida.storage.sqlite_zip import orm as sqlite_orm

__all__ = ('TursoNodeCollection', 'TursoQueryBuilder')


RECURSIVE_QUERY_ERROR = (
    'Recursive provenance queries are not supported for pyturso database URLs because pyturso does not yet support '
    'recursive CTEs. Use `with_incoming` or `with_outgoing` for direct-link queries.'
)
JSON_CONTAINS_ERROR = (
    'The `contains` operator for JSON fields is not supported for pyturso database URLs because pyturso does not '
    'support registering Python SQL functions.'
)


def _uses_pyturso_backend(backend: Any) -> bool:
    """Return whether the backend is configured to use a pyturso SQLAlchemy dialect."""
    return backend.profile.storage_config['database_url'].startswith('sqlite+turso')


class TursoJoiner(SqlaJoiner):
    """Joiner for pyturso connections that rejects recursive provenance joins."""

    def _join_node_descendants_recursive(self, *args: Any, **kwargs: Any):
        """Raise because pyturso does not support recursive CTEs."""
        raise NotImplementedError(RECURSIVE_QUERY_ERROR)

    def _join_node_ancestors_recursive(self, *args: Any, **kwargs: Any):
        """Raise because pyturso does not support recursive CTEs."""
        raise NotImplementedError(RECURSIVE_QUERY_ERROR)


class TursoQueryBuilder(sqlite_orm.SqliteQueryBuilder):
    """QueryBuilder adapted for Turso/pyturso database URLs."""

    def __init__(self, backend: Any) -> None:
        super().__init__(backend)
        if _uses_pyturso_backend(backend):
            self._joiner = TursoJoiner(self)

    def get_filter_expr_from_jsonb(
        self, operator: str, value: Any, attr_key: list[str], column=None, column_name=None, alias=None
    ):
        """Return a filter expression, rejecting operators unsupported by pyturso."""
        if operator == 'contains' and _uses_pyturso_backend(self._backend):
            raise NotImplementedError(JSON_CONTAINS_ERROR)
        return super().get_filter_expr_from_jsonb(operator, value, attr_key, column, column_name, alias)


class TursoNodeCollection(sqlite_orm.SqliteNodeCollection):
    """Node collection for Turso storage."""

    def has_link_path(
        self, source: BackendNode, target: BackendNode, link_types: Sequence[LinkType] | None = None
    ) -> bool:
        """Return whether ``target`` is reachable from ``source`` without using recursive CTEs."""
        if source.pk is None or target.pk is None:
            return False

        link_class: Any = self.ENTITY_CLASS.LINK_CLASS
        link_table = link_class.__table__
        link_type_values = None if link_types is None else tuple(link_type.value for link_type in link_types)
        if link_type_values is not None and not link_type_values:
            return False

        backend: Any = self.backend
        session = backend.get_session()
        visited = {source.pk}
        frontier = {source.pk}

        while frontier:
            query = select(link_table.c.output_id).where(link_table.c.input_id.in_(frontier))
            if link_type_values is not None:
                query = query.where(link_table.c.type.in_(link_type_values))

            descendants = set(session.execute(query).scalars())
            if target.pk in descendants:
                return True

            frontier = descendants - visited
            visited.update(frontier)

        return False
