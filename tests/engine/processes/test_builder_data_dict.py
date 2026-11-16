###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Builder updates serialize mapping-valued DataDict leaves instead of treating them as namespaces."""

import pytest

from aiida.engine import Process
from aiida.orm import DataDict, Int

pytestmark = pytest.mark.presto


class ContainerInputProcess(Process):
    """Static container inputs at the root and inside a namespace."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('bundle', valid_type=DataDict, serializer=DataDict, required=False)
        spec.input('nested.bundle', valid_type=DataDict, serializer=DataDict, required=False)


@pytest.mark.usefixtures('aiida_profile_clean')
@pytest.mark.parametrize('as_keywords', [False, True])
@pytest.mark.parametrize('existing', [False, True])
@pytest.mark.parametrize('nested', [False, True])
def test_update_serializes_mapping_leaf(as_keywords, existing, nested):
    """Updating a mapping leaf assigns a new container without mutating the caller's data."""
    builder = ContainerInputProcess.get_builder()
    target = builder.nested if nested else builder
    previous = DataDict({'old': Int(1)}).store()
    if existing:
        target.bundle = previous
    mapping = {'new': Int(2)}
    update = {'nested': {'bundle': mapping}} if nested else {'bundle': mapping}
    if as_keywords:
        builder._update(**update)
    else:
        builder._update(update)
    assert isinstance(target.bundle, DataDict)
    assert list(target.bundle) == ['new']
    assert target.bundle['new'].value == 2
    assert list(previous) == ['old']
    assert list(mapping) == ['new']
