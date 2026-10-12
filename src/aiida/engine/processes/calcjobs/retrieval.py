###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Worker-local ownership of successfully retrieved temporary files."""

from pathlib import Path
from tempfile import TemporaryDirectory

from aiida.common.typing import FilePath


class TemporaryRetrieval:
    """Own a completed download until parsing or process closure releases it.

    Construct this only after retrieval succeeds. This owner is deliberately not
    checkpointed: a recovered process must retrieve temporary inputs again, even
    if its checkpoint's directory still exists.

    :param directory: The successful attempt's private staging directory.
    """

    def __init__(self, directory: TemporaryDirectory[str]) -> None:
        self._directory = directory

    @property
    def path(self) -> str:
        """Return the directory passed to the parser."""
        return self._directory.name

    def owns(self, path: FilePath | None) -> bool:
        """Return whether a parser path belongs to this completed attempt."""
        return path is not None and Path(path) == Path(self.path) and Path(self.path).is_dir()

    def cleanup(self) -> None:
        """Release this attempt's directory; repeated cleanup is harmless."""
        self._directory.cleanup()
