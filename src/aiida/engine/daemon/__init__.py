###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Module with resources for the daemon.

TODO: Remove Circus dependency for namedpipe profiles.
The new scheduler architecture (ProcessSchedulerService + SubprocessWorkerExecutor)
handles worker lifecycle directly. Circus is only needed for RabbitMQ profiles.
Consider:
- Remove circus from dependencies (or make optional for rabbitmq)
- Remove DaemonClient for namedpipe profiles
- Clean up cmd_daemon.py (keep only 'verdi daemon worker' for executor to spawn)
"""

# AUTO-GENERATED

# fmt: off

from .client import *

__all__ = (
    'DaemonClient',
    'get_daemon_client',
)

# fmt: on
