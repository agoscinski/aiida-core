"""Unified User model for AiiDA - works with all database backends."""

from sqlalchemy import Column, Integer, String, Index

from .base import Base


class DbUser(Base):
    """Database model to store data for User entities.

    This unified model works with both PostgreSQL and SQLite backends,
    eliminating the need for separate model definitions.

    Replaces:
    - aiida.storage.psql_dos.models.user.DbUser
    - aiida.storage.sqlite_zip.models.DbUser (generated via pg_to_sqlite)
    """

    __tablename__ = 'db_dbuser'

    id = Column(Integer, primary_key=True)
    email = Column(String(254), nullable=False, unique=True)
    first_name = Column(String(254), default='', nullable=False)
    last_name = Column(String(254), default='', nullable=False)
    institution = Column(String(254), default='', nullable=False)

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations

    def __str__(self):
        """String representation."""
        return self.email

    def __repr__(self):
        """Developer-friendly representation."""
        return f"<DbUser(id={self.id}, email='{self.email}')>"
