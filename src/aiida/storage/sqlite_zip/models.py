###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""SQLite backend models - now uses unified models.

This module previously contained complex logic to transform PostgreSQL models
to SQLite-compatible ones. With the unified models in aiida.storage.models,
this transformation is no longer needed as the models use TypeDecorators that
automatically adapt to the database backend.

The unified models handle:
- UUID -> GUID (PostgreSQL UUID vs SQLite CHAR(32))
- DateTime -> TZDateTime (timezone-aware handling)
- JSONB -> JSONType (PostgreSQL JSONB vs SQLite JSON)
- Automatic removal of PostgreSQL-specific indexes (varchar_pattern_ops)
"""

from functools import lru_cache
from typing import Any, Set, Tuple

import sqlalchemy as sa

from aiida.orm.entities import EntityTypes

# Import all unified models
from aiida.storage.models.base import Base as SqliteBase
from aiida.storage.models.user import DbUser
from aiida.storage.models.computer import DbComputer
from aiida.storage.models.node import DbNode, DbLink
from aiida.storage.models.authinfo import DbAuthInfo
from aiida.storage.models.group import DbGroup, DbGroupNode as DbGroupNodes
from aiida.storage.models.comment import DbComment
from aiida.storage.models.log import DbLog

__all__ = [
    'SqliteBase',
    'DbUser',
    'DbComputer',
    'DbNode',
    'DbLink',
    'DbAuthInfo',
    'DbGroup',
    'DbGroupNodes',
    'DbComment',
    'DbLog',
    'MAP_ENTITY_TYPE_TO_MODEL',
    'get_model_from_entity',
]

# Map entity types to their corresponding model classes
MAP_ENTITY_TYPE_TO_MODEL = {
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


@lru_cache(maxsize=10)
def get_model_from_entity(entity_type: EntityTypes) -> Tuple[Any, Set[str]]:
    """Return the SQLAlchemy model and column names corresponding to the given entity.

    :param entity_type: The entity type to get the model for
    :return: Tuple of (model class, set of column names)
    """
    model = MAP_ENTITY_TYPE_TO_MODEL[entity_type]
    mapper = sa.inspect(model).mapper
    column_names = {col.name for col in mapper.c.values()}
    return model, column_names
