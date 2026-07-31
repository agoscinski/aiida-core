###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for :mod:`aiida.engine.processes.launcher`."""

from __future__ import annotations

import asyncio
import queue
import threading

from aiida.engine.processes.launcher import ProcessLauncher


def test_continue_schedules_work_on_process_loop():
    """Continuing a process should run on the configured process loop."""
    target_loop = asyncio.new_event_loop()
    observed_loop: queue.Queue[asyncio.AbstractEventLoop] = queue.Queue()
    thread = threading.Thread(target=target_loop.run_forever)
    thread.start()

    launcher = ProcessLauncher(loop=target_loop)

    async def continue_in_process_loop(*args, **kwargs):
        observed_loop.put(asyncio.get_running_loop())
        return 'result'

    launcher._continue_in_process_loop = continue_in_process_loop

    try:
        result = asyncio.run(launcher._continue(None, 1, False))
    finally:
        target_loop.call_soon_threadsafe(target_loop.stop)
        thread.join()
        target_loop.close()

    assert result == 'result'
    assert observed_loop.get_nowait() is target_loop
