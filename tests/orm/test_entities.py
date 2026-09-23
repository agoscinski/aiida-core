###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Test for general backend entities"""

import pickle

import pytest

from aiida import orm
from aiida.common.exceptions import InvalidOperation


class TestBackendEntitiesAndCollections:
    """Test backend entities and their collections"""

    def test_get_collection(self, backend):
        """Test :meth:`aiida.orm.entities.Entity.get_collection`."""
        group_collection = orm.Group.get_collection(backend)
        assert group_collection is orm.Group.collection
        assert group_collection.backend is backend

    def test_collections_cache(self):
        """Make sure that we're not recreating collections each time .collection is called"""
        # Check directly
        group_collection = orm.Group.collection
        assert group_collection is orm.Group.collection

        # Now check passing an explicit backend
        backend = group_collection.backend
        assert group_collection is group_collection(backend)

    def test_collections_count(self):
        """Make sure count() works for collections"""
        orm.Group(label='test-collection-count').store()
        group_collection_count = orm.Group.collection.count()
        number_of_groups = orm.QueryBuilder().append(orm.Group).count()
        assert number_of_groups > 0, 'There should be more than 0 Groups in the DB'
        assert group_collection_count == number_of_groups, (
            f"{group_collection_count} Group(s) was/were found using Collections' count() method, "
            f'but {number_of_groups} Group(s) was/were found using QueryBuilder directly'
        )

    def test_pickle(self):
        """Pickling is not supported and should raise."""
        with pytest.raises(InvalidOperation):
            pickle.dumps(orm.Entity({}))
