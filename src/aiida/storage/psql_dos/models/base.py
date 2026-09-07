###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""PostgreSQL SQLAlchemy model base."""

from sqlalchemy import MetaData, event
from sqlalchemy.orm import declarative_base

from aiida.storage.models.base import Model, instant_defaults_listener, naming_convention

Base = declarative_base(cls=Model, name='Model', metadata=MetaData(naming_convention=naming_convention))
event.listen(Base, 'init', instant_defaults_listener, propagate=True)


def get_orm_metadata() -> MetaData:
    """Return the populated PostgreSQL metadata object."""
    # Importing instantiates all classes from the shared schema on ``Base``.
    from aiida.storage.psql_dos.models import schema  # noqa: F401

    return Base.metadata
