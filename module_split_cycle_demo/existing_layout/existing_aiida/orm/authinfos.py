"""A public ORM module loaded early by the aggregate package."""

from existing_aiida.orm import entities


class AuthInfo(entities.Entity):
    """Example ORM entity."""
