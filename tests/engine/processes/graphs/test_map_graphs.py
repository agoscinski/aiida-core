###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Build rejections for `par_map` over `@graph` callables.

`par_map` maps unary tasks only: every declaration below maps a `@graph` (or a
graph that itself maps) and fails `build()` naming the mapped callable.
Task-only map execution lives in `test_par_map.py`.
"""

import pytest

from aiida.common.exceptions import GraphLanguageError
from aiida.engine.processes.graphs.lowering import GraphHandle
from tests.engine.processes.graphs.programs import map_graphs


@pytest.mark.parametrize(
    'declaration, match',
    [
        pytest.param(map_graphs.map_two_step, 'two_step', id='multi-step-graph'),
        pytest.param(map_graphs.map_add_one, 'add_one', id='single-task-graph'),
        pytest.param(map_graphs.map_nested, 'inner_map', id='nested-map-graph'),
        pytest.param(map_graphs.map_aliased, 'two_step', id='aliased-graph'),
        pytest.param(map_graphs.map_passthrough, 'passthrough_identity', id='task-free-graph'),
    ],
)
def test_map_graph_callable_rejected(declaration: GraphHandle, match: str) -> None:
    """Mapping a graph fails the build while naming the offending callable."""
    with pytest.raises(GraphLanguageError, match=match):
        declaration.build()
