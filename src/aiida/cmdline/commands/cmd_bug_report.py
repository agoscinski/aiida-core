###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`verdi bug-report` command."""

from __future__ import annotations

import json
import pathlib
import platform
import sys
import zipfile
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import click

from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.cmdline.utils import echo

if TYPE_CHECKING:
    from aiida.manage.configuration.profile import Profile
    from aiida.manage.manager import Manager

SENSITIVE_KEY_FRAGMENTS = ('password', 'passphrase', 'secret', 'token')
SENSITIVE_CONFIG_KEYS = ('AIIDADB_PASS',)
REDACTED_VALUE = '***'

MAX_LOG_BYTES = 1024 * 1024
"""Maximum number of bytes included per log file; longer logs are truncated to their tail."""


def _format_exception(exception: Exception) -> str:
    """Return a stable string representation for an exception."""
    return f'{type(exception).__name__}: {exception}'


def _is_sensitive_key(key: str) -> bool:
    """Return whether the key likely contains a sensitive value."""
    lowercase = key.lower()
    return key in SENSITIVE_CONFIG_KEYS or any(fragment in lowercase for fragment in SENSITIVE_KEY_FRAGMENTS)


def _redact_sensitive_values(value: Any) -> Any:
    """Redact values that are likely to contain secrets."""
    if isinstance(value, dict):
        redacted = {}

        for key, subvalue in value.items():
            if _is_sensitive_key(key) and subvalue is not None:
                redacted[key] = REDACTED_VALUE
            else:
                redacted[key] = _redact_sensitive_values(subvalue)

        return redacted

    if isinstance(value, list):
        return [_redact_sensitive_values(subvalue) for subvalue in value]

    return value


def _check_storage(profile: 'Profile') -> dict[str, Any]:
    """Return storage connection information for the current profile."""
    try:
        storage = profile.storage_cls(profile)
    except Exception as exception:
        return {'connected': False, 'message': _format_exception(exception)}

    message = None
    storage_exception = None

    try:
        message = str(storage)
    except Exception as exception:
        storage_exception = exception

    try:
        storage.close()
    except Exception as exception:
        if storage_exception is None:
            storage_exception = exception

    if storage_exception is not None:
        return {'connected': False, 'message': _format_exception(storage_exception)}

    return {'connected': True, 'message': message}


def _check_broker(manager: 'Manager') -> dict[str, Any]:
    """Return broker connection information for the current profile."""
    from aiida.brokers.zeromq.broker import ZeromqBroker

    try:
        broker = manager.get_broker()
    except Exception as exception:
        return {'connected': False, 'message': _format_exception(exception)}

    if broker is None:
        return {'connected': False, 'message': 'No broker configured for this profile.'}

    if isinstance(broker, ZeromqBroker):
        # The broker service is managed by the daemon, so its liveness can be read from its status files. Do not use
        # ``get_communicator``, which blocks polling for the service to come up precisely when it is down.
        result: dict[str, Any] = {'connected': broker.is_service_running(), 'message': str(broker)}
        status = broker.get_service_status()
        if status is not None:
            result['status'] = status
        return result

    try:
        broker.get_communicator()
    except Exception as exception:
        return {'connected': False, 'message': _format_exception(exception)}
    finally:
        try:
            broker.close()
        except Exception:
            pass

    # Do not use ``str(broker)`` here: the RabbitMQ representation includes the connection URL with credentials.
    return {'connected': True, 'message': broker.__class__.__name__}


def _check_daemon(manager: 'Manager') -> dict[str, Any]:
    """Return daemon connection information for the current profile."""
    try:
        status = manager.get_daemon_client().get_status()
    except Exception as exception:
        return {'connected': False, 'message': _format_exception(exception)}

    pid = status.get('pid')
    message = 'Daemon status retrieved successfully.'

    if pid is not None:
        message = f'Daemon is running with PID {pid}'

    return {'connected': True, 'message': message, 'status': status}


def _collect_python_info() -> dict[str, Any]:
    """Return structured information on the Python interpreter."""
    return {
        'version': platform.python_version(),
        'major': sys.version_info.major,
        'minor': sys.version_info.minor,
        'micro': sys.version_info.micro,
        'implementation': platform.python_implementation(),
        'compiler': platform.python_compiler(),
        'build': list(platform.python_build()),
        'executable': sys.executable,
    }


def _get_config_data() -> dict[str, Any] | None:
    """Return the contents of the AiiDA configuration file with secrets redacted."""
    from aiida.manage.configuration import get_config
    from aiida.manage.configuration.settings import DEFAULT_CONFIG_FILE_NAME

    filepath = pathlib.Path(get_config().dirpath) / DEFAULT_CONFIG_FILE_NAME

    if not filepath.exists():
        return None

    return _redact_sensitive_values(json.loads(filepath.read_text(encoding='utf-8')))


def _collect_diagnostics() -> dict[str, Any]:
    """Collect structured diagnostic information for the bug report."""
    import aiida
    from aiida.manage import get_manager

    manager = get_manager()
    profile = manager.get_profile()

    profile_data = None

    if profile is not None:
        profile_data = {'name': profile.name}

    services = {'storage': {'connected': False, 'message': 'No profile loaded.'}}

    if profile is not None:
        services['storage'] = _check_storage(profile)

    services['broker'] = _check_broker(manager)
    services['daemon'] = _check_daemon(manager)

    config = None
    config_error = None

    try:
        config = _get_config_data()
    except Exception as exception:
        config_error = _format_exception(exception)

    diagnostics = {
        'generated_at': datetime.now().astimezone().isoformat(),
        'aiida_version': aiida.__version__,
        'python': _collect_python_info(),
        'platform': {
            'platform': platform.platform(),
            'system': platform.system(),
            'release': platform.release(),
            'machine': platform.machine(),
        },
        'profile': profile_data,
        'config': config,
        'services': services,
    }

    if config_error is not None:
        diagnostics['config_error'] = config_error

    return diagnostics


def _get_log_files() -> list[tuple[str, pathlib.Path]]:
    """Return the log files to include."""
    from aiida.manage import get_manager
    from aiida.manage.configuration import get_config

    profile = get_manager().get_profile()

    if profile is None:
        return []

    filepaths = get_config().filepaths(profile)
    files = []

    for service, service_filepaths in filepaths.items():
        log_filepath = cast(dict[str, str], service_filepaths).get('log')

        if log_filepath is None:
            continue

        archive_name = 'broker.log' if service == 'zmq_broker_service' else f'{service}.log'
        path = pathlib.Path(log_filepath)
        if path.exists():
            files.append((archive_name, path))

    return files


def _read_log_tail(filepath: pathlib.Path, max_bytes: int = MAX_LOG_BYTES) -> bytes:
    """Return the contents of the log file, truncated to the last ``max_bytes`` bytes."""
    size = filepath.stat().st_size

    with filepath.open('rb') as handle:
        if size <= max_bytes:
            return handle.read()

        header = f'... (truncated from {size} bytes)\n'.encode('utf-8')

        if len(header) >= max_bytes:
            return header[:max_bytes]

        payload_bytes = max_bytes - len(header)
        handle.seek(size - payload_bytes)
        data = handle.read(payload_bytes)
        return header + data.decode('utf-8', errors='ignore').encode('utf-8')


@verdi.command('bug-report')
@click.option(
    '-o',
    '--output',
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    default=None,
    help=(
        'Output zip file path or directory where to put the zip. Defaults to '
        'aiida-bug-report-<timestamp>.zip in current directory.'
    ),
)
def verdi_bug_report(output: str | None) -> None:
    """Create a zip file with diagnostic information for bug reports.

    Bundles profile configuration, service status, and log files into a
    zip archive that can be attached to a GitHub issue.
    """
    if output is None:
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        output = 'aiida-bug-report-{timestamp}.zip'

    output_path = pathlib.Path(output)

    if output_path.exists() and output_path.is_dir():
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        output_path = output_path / f'aiida-bug-report-{timestamp}.zip'

    if output_path.exists():
        echo.echo_critical(f'Output file `{output_path}` already exists.')

    echo.echo('Collecting diagnostic information...')

    diagnostics = _collect_diagnostics()
    log_files = _get_log_files()

    contents: list[tuple[str, int]] = []

    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('diagnostics.json', json.dumps(diagnostics, indent=2, sort_keys=True, default=str))
            for archive_name, filepath in log_files:
                data = _read_log_tail(filepath)
                zf.writestr(archive_name, data)
                contents.append((archive_name, len(data)))
    except OSError as exception:
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        echo.echo_critical(f'Failed to write bug report to `{output_path}`: {exception}')

    echo.echo_success(f'Bug report written to: {output_path}')
    echo.echo_info('Contents:')
    echo.echo('  diagnostics.json')
    for archive_name, num_bytes in contents:
        echo.echo(f'  {archive_name} ({num_bytes} bytes)')
    echo.echo('')
    echo.echo('Attach this file to your post at Discourse:')
    echo.echo('https://aiida.discourse.group')
