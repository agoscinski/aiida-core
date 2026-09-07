###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""PostgreSQL group models."""

from aiida.storage.psql_dos.models.schema import MODELS

DbGroup = MODELS.group
DbGroupNode = MODELS.group_node
table_groups_nodes = DbGroupNode.__table__
