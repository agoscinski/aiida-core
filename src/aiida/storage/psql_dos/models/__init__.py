###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""PSQL DOS models - now imports from unified models.

This module re-exports the unified models from aiida.storage.models.
All model definitions have been consolidated into a single location that works
with both PostgreSQL and SQLite backends.
"""

# Re-export unified models
from aiida.storage.models.user import DbUser
from aiida.storage.models.computer import DbComputer
from aiida.storage.models.node import DbNode, DbLink
from aiida.storage.models.authinfo import DbAuthInfo
from aiida.storage.models.group import DbGroup, DbGroupNode
from aiida.storage.models.comment import DbComment
from aiida.storage.models.log import DbLog
from aiida.storage.models.settings import DbSetting
from aiida.storage.models.base import Base

__all__ = [
    'DbUser',
    'DbComputer',
    'DbNode',
    'DbLink',
    'DbAuthInfo',
    'DbGroup',
    'DbGroupNode',
    'DbComment',
    'DbLog',
    'DbSetting',
    'Base',
]
