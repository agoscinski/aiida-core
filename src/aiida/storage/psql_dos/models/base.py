###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Base SQLAlchemy models - now imports from unified models.

This module provides backwards compatibility by re-exporting the Base class
from the unified models and providing the get_orm_metadata() function needed
by the migration system.
"""

from sqlalchemy import MetaData

# Import unified Base class
from aiida.storage.models.base import Base, Model, instant_defaults_listener, naming_convention

__all__ = ['Base', 'Model', 'instant_defaults_listener', 'naming_convention', 'get_orm_metadata']


def get_orm_metadata() -> MetaData:
    """Return the populated metadata object.

    This function is used by Alembic migrations to access the SQLAlchemy
    metadata containing all table definitions.
    """
    # Import all models to ensure they're registered in Base.metadata
    from . import (
        DbUser, DbComputer, DbNode, DbLink,
        DbAuthInfo, DbGroup, DbGroupNode,
        DbComment, DbLog, DbSetting
    )

    return Base.metadata
