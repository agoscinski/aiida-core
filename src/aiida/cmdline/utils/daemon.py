###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""CLI wrappers for daemon environment checks."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from aiida.engine.daemon.client import DaemonClient, DaemonEnvInfo, PackageVersionInfo, PackageVersionSnapshot


def format_package_version_info(version_info: PackageVersionInfo) -> str:
    """Return a human-readable string for package version information."""
    from aiida.engine.daemon.environment import format_package_version_info as format_info

    return format_info(version_info)


def package_versions_match(package_version: PackageVersionInfo, current_package_version: PackageVersionInfo) -> bool:
    """Return whether daemon and current package version information match."""
    from aiida.engine.daemon.environment import package_versions_match as match

    return match(package_version, current_package_version)


def format_package_state_change_lines(
    daemon_packages: PackageVersionSnapshot, current_packages: PackageVersionSnapshot
) -> list[str]:
    """Return formatted package-state mismatch lines."""
    from aiida.engine.daemon.environment import format_package_state_change_lines as format_lines

    return format_lines(daemon_packages, current_packages)


def validate_python_binary(env_info: DaemonEnvInfo) -> str | None:
    """Return an error message if the daemon's Python binary differs, or None if it matches."""
    from aiida.engine.daemon.environment import validate_python_binary as validate

    return validate(env_info)


def validate_package_versions(env_info: DaemonEnvInfo) -> str | None:
    """Return an error message if the daemon and current package versions differ, or None if they match."""
    from aiida.engine.daemon.environment import validate_package_versions as validate

    return validate(env_info)


def validate_daemon_env(client: DaemonClient) -> str | None:
    """Return an error message if the daemon environment differs, or None if it matches."""
    from aiida.engine.daemon.environment import validate_daemon_env as validate

    return validate(client)
