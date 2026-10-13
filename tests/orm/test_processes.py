###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for process metadata queries."""

from datetime import datetime, timedelta, timezone

import pytest

from aiida import orm
from aiida.common import timezone as aiida_timezone
from aiida.common.processes import ProcessState
from aiida.orm import get_process_state_change_timestamp


@pytest.mark.usefixtures('aiida_profile_clean')
def test_get_process_state_change_timestamp(monkeypatch):
    """The latest transition is queried per process type, excluding unknown timestamps."""
    assert get_process_state_change_timestamp() is None
    orm.CalculationNode().store()
    orm.WorkflowNode().store()
    assert get_process_state_change_timestamp() is None

    calculation = orm.CalculationNode().store()
    workflow = orm.WorkflowNode().store()
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    second = first + timedelta(seconds=1)
    monkeypatch.setattr(aiida_timezone, 'now', lambda: first)
    calculation.set_process_state(ProcessState.RUNNING)
    monkeypatch.setattr(aiida_timezone, 'now', lambda: second)
    workflow.set_process_state(ProcessState.RUNNING)

    assert get_process_state_change_timestamp('calculation') == first
    assert get_process_state_change_timestamp('work') == second
    assert get_process_state_change_timestamp() == second

    # Unrelated modifications must not masquerade as process activity.
    calculation.base.extras.set('unrelated', True)
    assert get_process_state_change_timestamp('calculation') == first
    assert get_process_state_change_timestamp() == second

    third = second + timedelta(seconds=1)
    monkeypatch.setattr(aiida_timezone, 'now', lambda: third)
    calculation.set_process_state(ProcessState.FINISHED)
    assert get_process_state_change_timestamp() == third


def test_get_process_state_change_timestamp_invalid_type():
    """Invalid process categories are rejected before querying storage."""
    with pytest.raises(ValueError, match='invalid value for process_type'):
        get_process_state_change_timestamp('invalid')
