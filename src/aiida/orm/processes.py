###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Backend-independent queries for process metadata."""

from datetime import datetime

from aiida.orm.nodes.process.calculation.calculation import CalculationNode
from aiida.orm.nodes.process.process import ProcessNode
from aiida.orm.nodes.process.workflow.workflow import WorkflowNode
from aiida.orm.querybuilder import QueryBuilder

__all__ = ('get_process_state_change_timestamp',)


def get_process_state_change_timestamp(process_type: str | None = None) -> datetime | None:
    """Query the most recent recorded process state transition.

    Historical processes without a recorded transition timestamp are excluded.

    :param process_type: optional process type for which to get the latest state change timestamp.
        Valid process types are 'calculation' and 'work'. If not specified, query all processes.
    :return: the latest recorded timestamp, or None if no timestamp is known
    :raises ValueError: if the process type is invalid
    """
    valid_process_types = ['calculation', 'work']

    if process_type is not None and process_type not in valid_process_types:
        msg = f'invalid value for process_type, valid values are {", ".join(valid_process_types)}'
        raise ValueError(msg)

    node_class = (
        ProcessNode if process_type is None else {'calculation': CalculationNode, 'work': WorkflowNode}[process_type]
    )
    field = 'process_state_change_timestamp'
    result = (
        QueryBuilder()
        .append(node_class, tag='process', filters={field: {'!==': None}}, project=field)
        .order_by({'process': {field: 'desc'}})
        .limit(1)
        .first(flat=True)
    )
    return result
