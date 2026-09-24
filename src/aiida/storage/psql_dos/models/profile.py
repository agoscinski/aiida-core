###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Persistent identity and contact details of a configured profile."""

from sqlalchemy import Column
from sqlalchemy.types import Integer, String

from aiida.storage.psql_dos.models.base import Base


class DbProfile(Base):
    """Identity of a profile within a storage, independent of its contact email."""

    __tablename__ = 'db_dbprofile'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(36), nullable=False, unique=True)
    email = Column(String(254), nullable=False, default='')
    first_name = Column(String(254), nullable=False, default='')
    last_name = Column(String(254), nullable=False, default='')
    institution = Column(String(254), nullable=False, default='')
