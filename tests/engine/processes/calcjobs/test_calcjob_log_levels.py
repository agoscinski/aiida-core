###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for Phase 3: CalcJob log level promotions (INFO → WARNING for scheduler parse failures)."""

import ast

import pytest


@pytest.mark.presto
class TestCalcJobLogLevelPromotions:
    """Verify that specific log calls in calcjob.py use the correct log level.

    We parse the AST of the source file to check the log method names used for specific messages,
    rather than trying to run full CalcJob processes.
    """

    @staticmethod
    def _find_log_calls_with_message(source: str, substring: str) -> list[str]:
        """Find all logger call method names whose first argument contains ``substring``.

        :param source: Python source code to parse.
        :param substring: Substring to search for in the first argument of log calls.
        :return: List of method names (e.g., 'info', 'warning', 'debug').
        """
        tree = ast.parse(source)
        results = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                # Match self.logger.<method>(...) pattern
                if (
                    isinstance(func, ast.Attribute)
                    and isinstance(func.value, ast.Attribute)
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == 'self'
                    and func.value.attr == 'logger'
                ):
                    method_name = func.attr
                    if node.args:
                        # Check if the first argument is a string or f-string containing the substring
                        first_arg = node.args[0]
                        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                            if substring in first_arg.value:
                                results.append(method_name)
                        elif isinstance(first_arg, ast.JoinedStr):
                            # f-string: check all Constant parts
                            parts = [
                                v.value
                                for v in first_arg.values
                                if isinstance(v, ast.Constant) and isinstance(v.value, str)
                            ]
                            if any(substring in part for part in parts):
                                results.append(method_name)
        return results

    def test_missing_detailed_job_info_is_warning(self):
        """The 'detailed_job_info attribute is missing' message should be logged at WARNING."""
        from pathlib import Path

        source = Path('src/aiida/engine/processes/calcjobs/calcjob.py').read_text()
        methods = self._find_log_calls_with_message(source, 'the `detailed_job_info` attribute is missing')
        assert len(methods) == 1, 'Expected exactly one log call with this message'
        assert methods[0] == 'warning', f'Expected WARNING but found {methods[0]}'

    def test_nonzero_retval_is_warning(self):
        """The 'return value of detailed_job_info is non-zero' message should be logged at WARNING."""
        from pathlib import Path

        source = Path('src/aiida/engine/processes/calcjobs/calcjob.py').read_text()
        methods = self._find_log_calls_with_message(source, 'return value of `detailed_job_info` is non-zero')
        assert len(methods) == 1, 'Expected exactly one log call with this message'
        assert methods[0] == 'warning', f'Expected WARNING but found {methods[0]}'

    def test_scheduler_not_implemented_stays_info(self):
        """The 'does not implement scheduler output parsing' message should remain at INFO."""
        from pathlib import Path

        source = Path('src/aiida/engine/processes/calcjobs/calcjob.py').read_text()
        methods = self._find_log_calls_with_message(source, 'does not implement scheduler output parsing')
        assert len(methods) == 1, 'Expected exactly one log call with this message'
        assert methods[0] == 'info', f'Expected INFO but found {methods[0]}'
