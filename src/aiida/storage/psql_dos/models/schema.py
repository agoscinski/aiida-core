###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""PostgreSQL ORM models instantiated from the shared SQL schema."""

from sqlalchemy import DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID

from aiida.storage.models.schema import create_models
from aiida.storage.psql_dos.models.base import Base


def create_pattern_index(name, column, column_name):
    """Create a PostgreSQL index optimized for ``LIKE 'prefix%'`` queries."""
    return Index(name, column, postgresql_using='btree', postgresql_ops={column_name: 'varchar_pattern_ops'})


MODELS = create_models(
    Base,
    datetime_type=DateTime(timezone=True),
    json_type=JSONB(),
    uuid_type=UUID(as_uuid=True),
    create_pattern_index=create_pattern_index,
)
