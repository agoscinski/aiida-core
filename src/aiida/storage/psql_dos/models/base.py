###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Base SQLAlchemy models - now imports from unified models.

This module provides backwards compatibility by re-exporting everything
from the unified models.
"""

# Re-export everything from unified base
from aiida.storage.models.base import Base, Model, instant_defaults_listener, naming_convention, get_orm_metadata

__all__ = ['Base', 'Model', 'instant_defaults_listener', 'naming_convention', 'get_orm_metadata']
