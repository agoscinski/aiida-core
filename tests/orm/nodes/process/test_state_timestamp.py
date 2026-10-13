###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for per-process transition timestamps."""

from datetime import datetime, timedelta, timezone

import pytest

from aiida import orm
from aiida.common import timezone as aiida_timezone
from aiida.common.exceptions import ModificationNotAllowed
from aiida.common.processes import ProcessState


@pytest.mark.parametrize('stored', (False, True))
def test_transition_timestamp(stored, monkeypatch):
    """Only state transitions change the timestamp, including resetting state to None."""
    node = orm.CalculationNode()
    if stored:
        node.store()
    assert node.process_state_change_timestamp is None

    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    second = first + timedelta(seconds=1)
    monkeypatch.setattr(aiida_timezone, 'now', lambda: first)
    node.set_process_state(ProcessState.RUNNING)
    assert node.process_state_change_timestamp == first
    assert node.process_state == ProcessState.RUNNING
    if stored:
        assert node.to_model().process_state_change_timestamp == first

    monkeypatch.setattr(aiida_timezone, 'now', lambda: second)
    node.set_process_state('running')
    node.set_process_status('Working')
    node.base.extras.set('unrelated', True)
    assert node.process_state_change_timestamp == first

    node.set_process_state(None)
    assert node.process_state is None
    assert node.process_state_change_timestamp == second
    node.store()
    assert orm.load_node(node.pk).process_state_change_timestamp == second


def test_transition_rollback():
    """Rolling back an explicit transaction restores both state and timestamp."""
    node = orm.CalculationNode().store()
    node.set_process_state(ProcessState.RUNNING)
    timestamp = node.process_state_change_timestamp

    with pytest.raises(RuntimeError, match='abort'):
        with node.backend.transaction():
            node.set_process_state(ProcessState.FINISHED)
            raise RuntimeError('abort')

    reloaded = orm.load_node(node.pk)
    assert reloaded.process_state == ProcessState.RUNNING
    assert reloaded.process_state_change_timestamp == timestamp


def test_transition_timestamp_failure_is_atomic(monkeypatch):
    """A timestamp persistence failure cannot leave only the state update committed."""
    node = orm.CalculationNode().store()
    node.set_process_state(ProcessState.RUNNING)
    timestamp = node.process_state_change_timestamp
    entity_type = type(node.backend_entity)
    original = entity_type.process_state_change_timestamp

    def reject_timestamp(entity, value):
        raise RuntimeError('timestamp write failed')

    monkeypatch.setattr(entity_type, 'process_state_change_timestamp', property(original.fget, reject_timestamp))
    with pytest.raises(RuntimeError, match='timestamp write failed'):
        node.set_process_state(ProcessState.FINISHED)

    reloaded = orm.load_node(node.pk)
    assert reloaded.process_state == ProcessState.RUNNING
    assert reloaded.process_state_change_timestamp == timestamp


def test_sealed_transition_rejected():
    """Sealing prevents state updates without changing the recorded timestamp."""
    node = orm.CalculationNode().store()
    node.set_process_state(ProcessState.FINISHED)
    timestamp = node.process_state_change_timestamp
    node.seal()
    with pytest.raises(ModificationNotAllowed):
        node.set_process_state(ProcessState.RUNNING)
    assert node.process_state == ProcessState.FINISHED
    assert node.process_state_change_timestamp == timestamp
