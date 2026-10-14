###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
###########################################################################
"""Fixtures shared by SQL storage policy tests."""

import pytest


@pytest.fixture(params=['profile', 'temporary'])
def commit_backend(
    request, aiida_profile_clean, backend, aiida_config_factory, aiida_profile_factory, manager, tmp_path
):
    """Exercise both persistent SQL storage and the temporary SQLite backend."""
    if request.param == 'profile':
        yield backend
    else:
        with aiida_config_factory(tmp_path) as config:
            with aiida_profile_factory(config, storage_backend='core.sqlite_temp'):
                yield manager.get_profile_storage()
