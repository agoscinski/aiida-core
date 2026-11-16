###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Static owned-container staging for ``_find_data_node``.

``CalcJob`` static ``DataDict`` inputs such as ``pseudos`` wire one container
node; ``CalcInfo.local_copy_list`` references the owned member UUIDs inside it.
The lookup must descend into ``DataDict``/``DataList`` owned subtrees without
cloning or detaching members: members keep their identity and repository content.
"""

import io
import logging

import pytest

from aiida.engine.daemon import execmanager
from aiida.orm import CalcJobNode, DataDict, DataList, Int, SinglefileData, load_node
from aiida.transports.plugins.local import LocalTransport

pytestmark = pytest.mark.presto


def test_find_descends_data_dict(aiida_profile_clean):
    """An owned member is found through its owning ``DataDict``."""
    container = DataDict({'Si': SinglefileData(__file__), 'O': SinglefileData(__file__)})
    container.store()
    member = container['Si']
    assert load_node(member.uuid) is not None
    found = execmanager._find_data_node({'pseudos': container}, member.uuid)
    assert found is not None
    assert found.uuid == member.uuid
    # No copies: the found handle is the owned member itself with readable repository content.
    assert found.base.repository.list_object_names()


def test_find_descends_data_list(aiida_profile_clean):
    """An owned member is found through its owning ``DataList``."""
    container = DataList([Int(1), Int(2)])
    container.store()
    member = container[0]
    found = execmanager._find_data_node({'inputs': {'values': container}}, member.uuid)
    assert found is not None
    assert found.uuid == member.uuid


def test_find_descends_nested_mapping_and_container(aiida_profile_clean):
    """Nested mappings and nested containers are searched depth-first."""
    inner = DataDict({'a': Int(1)})
    inner.store()
    outer = DataDict({'first': inner})
    # ``outer`` owns a copy of ``inner``; search must reach the leaf through both levels.
    outer.store()
    nested = outer['first']
    leaf = next(iter(nested.values()))
    found = execmanager._find_data_node({'pw': {'pseudos': outer}}, leaf.uuid)
    assert found is not None
    assert found.uuid == leaf.uuid


@pytest.mark.asyncio
async def test_copy_unstored_owned_file(aiida_profile_clean, tmp_path):
    """The unstored-input staging path copies an owned file without detaching or storing it."""
    content = b'pseudopotential file content\n'
    container = DataDict({'Si': SinglefileData(io.BytesIO(content), filename='Si.upf')})
    member = container['Si']
    identity = member.uuid
    node = CalcJobNode()
    async with LocalTransport() as transport:
        await execmanager._copy_local_files(
            logging.getLogger(__name__),
            node,
            transport,
            {'pseudos': container},
            [(member.uuid, member.filename, 'pseudo/Si.upf')],
            workdir=tmp_path,
        )
    assert (tmp_path / 'pseudo' / 'Si.upf').read_bytes() == content
    assert container['Si'].uuid == identity
    assert not container.is_stored
    assert not member.is_stored


def test_find_returns_none_for_unknown(aiida_profile_clean):
    """Unknown UUIDs still miss without touching membership."""
    container = DataDict({'a': Int(1)})
    container.store()
    assert execmanager._find_data_node({'pseudos': container}, '00000000-0000-4000-8000-000000000000') is None
