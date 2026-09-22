###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Run the three isolated import-layout examples."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
SCENARIOS = (
    (
        'Existing layout',
        'existing_layout',
        'from existing_aiida.orm import AuthInfo; print(f"Imported user API {AuthInfo.__name__}")',
        True,
    ),
    (
        'Partial Option B, importing the user API first',
        'partial_option_b',
        'from partial_aiida.orm import AuthInfo; print(f"Imported user API {AuthInfo.__name__}")',
        True,
    ),
    (
        'Partial Option B, importing the core API first',
        'partial_option_b',
        'from partial_aiida._core.orm.entities import Entity; print(Entity)',
        False,
    ),
    (
        'Complete Option B',
        'complete_option_b',
        'from complete_aiida.orm import AuthInfo; from complete_aiida._core.orm.entities import Entity; '
        'print(f"Imported user API {AuthInfo.__name__} and core {Entity.__name__}")',
        True,
    ),
)


def main() -> int:
    """Run each import in a fresh interpreter and check its expected result."""
    unexpected_results = 0

    for title, directory, statement, should_succeed in SCENARIOS:
        print(f'\n=== {title} ===')
        result = subprocess.run(
            [sys.executable, '-c', statement],
            cwd=ROOT / directory,
            capture_output=True,
            check=False,
            text=True,
        )
        output = result.stdout if result.returncode == 0 else result.stderr
        print(output.rstrip())

        succeeded = result.returncode == 0
        if succeeded != should_succeed:
            unexpected_results += 1
            print(f'UNEXPECTED: import was expected to {"succeed" if should_succeed else "fail"}')

    return unexpected_results


if __name__ == '__main__':
    raise SystemExit(main())
