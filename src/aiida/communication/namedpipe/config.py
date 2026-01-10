"""Configuration utilities for named pipe broker."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from aiida.manage.configuration.profile import Profile

__all__ = ('configure_profile_for_namedpipe', 'is_namedpipe_broker', 'ensure_broker_running')


def configure_profile_for_namedpipe(profile: 'Profile') -> None:
    """Configure a profile to use the named pipe broker.

    This will update the profile's process control backend to use 'core.namedpipe'.

    :param profile: The profile to configure.
    """
    profile.set_process_controller('core.namedpipe', {})


def is_namedpipe_broker(profile: 'Profile') -> bool:
    """Check if a profile is configured to use the named pipe broker.

    :param profile: The profile to check.
    :return: True if the profile uses named pipe broker, False otherwise.
    """
    backend = profile.process_control_backend
    return backend == 'core.namedpipe' or backend == 'namedpipe'


def ensure_broker_running(profile: 'Profile') -> None:
    """Ensure that the message broker is running for the profile.

    :param profile: The profile to check.
    :raises ConnectionError: If broker is not running.
    """
    from . import discovery

    if not is_namedpipe_broker(profile):
        raise ValueError(f'Profile {profile.name} is not configured to use named pipe broker')

    broker_info = discovery.discover_broker(profile.name)
    if broker_info is None:
        raise ConnectionError(
            f'Named pipe message broker for profile {profile.name} is not running. '
            'Please start it with: verdi scheduler start'
        )
