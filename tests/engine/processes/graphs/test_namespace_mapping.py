###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Nested static declarations close enclosing dynamic process namespaces."""

import pytest

from tests.engine.processes.graphs.programs import namespace_mapping

pytestmark = pytest.mark.presto


def test_nested_static_graph_builds() -> None:
    """Explicit nested member names are sufficient to build a graph."""
    assert namespace_mapping.static_nested.build() is None


def test_static_declarations_still_close() -> None:
    """Closing a nested mapping also closes its dynamic parent."""
    closed = namespace_mapping.static_nested_gather._task_spec.inputs
    assert not closed['sub'].dynamic
    assert not closed['sub']['members'].dynamic
