###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""SQLAlchemy models for SQLite storage backends."""

import functools
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import JSON

from aiida.orm.entities import EntityTypes
from aiida.storage.models.base import Model, instant_defaults_listener, naming_convention
from aiida.storage.models.schema import create_models


class TZDateTime(sa.TypeDecorator):
    """A timezone-naive UTC ``DateTime`` implementation for SQLite."""

    impl = sa.DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect):
        """Convert an aware timestamp to SQLite's UTC representation."""
        if value is None:
            return value
        if value.tzinfo is None:
            value = value.astimezone(timezone.utc)
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect):
        """Convert SQLite's UTC representation to an aware timestamp."""
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


SqliteBase = sa.orm.declarative_base(
    cls=Model, name='SqliteModel', metadata=sa.MetaData(naming_convention=naming_convention)
)
sa.event.listen(SqliteBase, 'init', instant_defaults_listener, propagate=True)

MODELS = create_models(
    SqliteBase,
    datetime_type=TZDateTime(),
    json_type=JSON(),
    uuid_type=sa.String(32),
    create_pattern_index=lambda name, column, column_name: None,
)

DbUser = MODELS.user
DbComputer = MODELS.computer
DbAuthInfo = MODELS.authinfo
DbGroup = MODELS.group
DbNode = MODELS.node
DbGroupNodes = MODELS.group_node
DbComment = MODELS.comment
DbLog = MODELS.log
DbLink = MODELS.link
DbSetting = MODELS.setting
DB_SETTING_TABLE = SqliteBase.metadata.tables['db_dbsetting']

MAP_ENTITY_TYPE_TO_MODEL: dict[EntityTypes, type[SqliteBase]] = {
    EntityTypes.USER: DbUser,
    EntityTypes.AUTHINFO: DbAuthInfo,
    EntityTypes.GROUP: DbGroup,
    EntityTypes.NODE: DbNode,
    EntityTypes.COMMENT: DbComment,
    EntityTypes.COMPUTER: DbComputer,
    EntityTypes.LOG: DbLog,
    EntityTypes.LINK: DbLink,
    EntityTypes.GROUP_NODE: DbGroupNodes,
}


@functools.lru_cache(maxsize=10)
def get_model_from_entity(entity_type: EntityTypes) -> tuple[Any, set[str]]:
    """Return the SQLAlchemy model and mapped column names for an entity type."""
    model = MAP_ENTITY_TYPE_TO_MODEL[entity_type]
    mapper = sa.inspect(model).mapper
    return model, {col.name for col in mapper.c.values()}
