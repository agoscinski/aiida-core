"""Entity implementation moved to the core tree without its dependency."""

from partial_aiida.orm.pydantic import OrmModel


class Entity(OrmModel):
    """Example entity base class."""
