###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Generate a terminal-friendly checklist from the public API inventory."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEADER = """# Public API review
#
# Edit only the first character of each non-comment line:
#   _ = unreviewed    y = accept    m = maybe    n = deny
#
# The tab-separated columns are: decision, resource, kind, signature.
# Regenerating this file preserves decisions for resources that still exist.
# Useful summary command:
#   awk -F '\\t' '!/^#/ {count[$1]++} END {for (key in count) print key, count[key]}' public_api_review.txt
"""


def parse_arguments() -> argparse.Namespace:
    """Return the parsed command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'input',
        nargs='?',
        type=Path,
        default=PROJECT_ROOT / 'public_api_v3.json',
        help='Public API JSON inventory (default: %(default)s).',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=PROJECT_ROOT / 'public_api_review.txt',
        help='Checklist to write (default: %(default)s).',
    )
    return parser.parse_args()


def single_line(value: object) -> str:
    """Return ``value`` with whitespace normalized for a tab-separated record."""
    return ' '.join(str(value).split())


def read_decisions(path: Path) -> dict[str, str]:
    """Return decisions previously recorded in a checklist."""
    decisions = {}
    for line_number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), start=1):
        if not line or line.startswith('#'):
            continue
        fields = line.split('\t', maxsplit=2)
        if len(fields) < 2 or fields[0] not in {'_', 'y', 'm', 'n'}:
            message = f'Invalid checklist record at {path}:{line_number}.'
            raise ValueError(message)
        decisions[fields[1]] = fields[0]
    return decisions


def generate_checklist(input_path: Path, decisions: Mapping[str, str] | None = None) -> str:
    """Return a checklist generated from ``input_path``."""
    data = json.loads(input_path.read_text(encoding='utf-8'))
    resources = data.get('resources')
    if not isinstance(resources, dict):
        message = f'Expected {input_path} to contain a `resources` object.'
        raise ValueError(message)

    decisions = decisions or {}
    lines = [HEADER.rstrip()]
    for name, details in resources.items():
        if not isinstance(details, dict):
            message = f'Expected details for resource `{name}` to be an object.'
            raise ValueError(message)
        lines.append(
            '\t'.join(
                (
                    decisions.get(single_line(name), '_'),
                    single_line(name),
                    single_line(details.get('kind', '')),
                    single_line(details.get('signature') or ''),
                )
            )
        )
    return '\n'.join(lines) + '\n'


def main() -> None:
    """Generate the checklist."""
    arguments = parse_arguments()
    decisions = read_decisions(arguments.output) if arguments.output.exists() else {}
    arguments.output.write_text(generate_checklist(arguments.input, decisions), encoding='utf-8')
    print(f'Wrote {arguments.output}')


if __name__ == '__main__':
    main()
