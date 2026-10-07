###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Contracts for converting ORM data nodes to Python values."""

from enum import IntEnum

import pytest

from aiida import orm
from aiida.common.links import LinkType
from aiida.manage.configuration import Profile

pytestmark = pytest.mark.presto


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


@pytest.mark.parametrize(
    ('node_type', 'value'),
    [
        pytest.param(orm.Bool, True, id='bool'),
        pytest.param(orm.Int, 3, id='int'),
        pytest.param(orm.Float, 1.5, id='float'),
        pytest.param(orm.Str, 'hello', id='str'),
        pytest.param(orm.List, [1, {'name': 'entry'}], id='list'),
        pytest.param(orm.Dict, {'values': [1, 2]}, id='dict'),
        pytest.param(orm.EnumData, LinkType.CREATE, id='enum'),
        pytest.param(orm.EnumData, Priority.HIGH, id='int-enum'),
    ],
)
@pytest.mark.parametrize('stored', [pytest.param(False, id='unstored'), pytest.param(True, id='stored')])
def test_registered_data_returns_exact_python_type(
    aiida_profile_clean: Profile, node_type: type[orm.Data], value: object, stored: bool
) -> None:
    """Registered nodes retain their exact Python value and type after conversion."""
    node: orm.Data = node_type(value)
    if stored:
        node.store()
    converted: object = orm.from_aiida_type(node)
    assert type(converted) is type(value)
    assert converted == value
    if isinstance(value, IntEnum | LinkType):
        assert converted is value


@pytest.mark.parametrize('stored', [pytest.param(False, id='unstored'), pytest.param(True, id='stored')])
def test_unregistered_data_preserves_node_identity(aiida_profile_clean: Profile, stored: bool) -> None:
    """Unregistered data is returned as the original node."""
    node: orm.Data = orm.Data()
    if stored:
        node.store()
    assert orm.from_aiida_type(node) is node


@pytest.mark.parametrize(
    ('node_type', 'value'),
    [
        pytest.param(orm.Dict, {'counts': [1, 2]}, id='dict'),
        pytest.param(orm.List, [{'counts': [1, 2]}], id='list'),
    ],
)
def test_mutating_stored_converted_container_preserves_stored_values(
    aiida_profile_clean: Profile, node_type: type[orm.Data], value: object
) -> None:
    """Mutating the returned container leaves stored node attributes unchanged."""
    node: orm.Data = node_type(value).store()
    converted: object = orm.from_aiida_type(node)
    if isinstance(converted, dict):
        converted['counts'].append(3)
    else:
        assert isinstance(converted, list)
        converted[0]['counts'].append(3)
    assert orm.from_aiida_type(node) == value


def test_plugin_registration_overrides_unregistered_fallback(aiida_profile_clean: Profile) -> None:
    """A plugin registration controls conversion of its own data subclass."""

    class PluginData(orm.Data):
        pass

    node: PluginData = PluginData()
    assert orm.from_aiida_type(node) is node

    @orm.from_aiida_type.register(PluginData)
    def convert(value: PluginData) -> str:
        return f'plugin:{value.uuid}'

    assert orm.from_aiida_type(node) == f'plugin:{node.uuid}'


def test_python_round_trip_preserves_registered_values(aiida_profile_clean: Profile) -> None:
    """Forward and reverse conversion preserve the regular enum member."""
    assert orm.from_aiida_type(orm.to_aiida_type(LinkType.RETURN)) is LinkType.RETURN
