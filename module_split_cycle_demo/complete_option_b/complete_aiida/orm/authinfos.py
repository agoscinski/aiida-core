"""A public ORM module loaded early by the aggregate package."""

from complete_aiida._core.orm.entities import Entity


class AuthInfo(Entity):
    """Example ORM entity."""
