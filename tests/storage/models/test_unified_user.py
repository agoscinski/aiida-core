"""Tests for unified DbUser model."""

import pytest
from sqlalchemy.exc import IntegrityError

from aiida.storage.models.user import DbUser


class TestDbUserBasic:
    """Basic tests for DbUser model."""

    def test_create_user_sqlite(self, sqlite_session):
        """Test creating a user in SQLite."""
        user = DbUser(
            email='test@example.com',
            first_name='Test',
            last_name='User',
            institution='MIT'
        )
        sqlite_session.add(user)
        sqlite_session.commit()

        assert user.id is not None
        assert user.email == 'test@example.com'
        assert user.first_name == 'Test'
        assert user.last_name == 'User'
        assert user.institution == 'MIT'

    def test_create_user_postgresql(self, postgresql_session):
        """Test creating a user in PostgreSQL."""
        user = DbUser(
            email='test@example.com',
            first_name='Test',
            last_name='User',
            institution='EPFL'
        )
        postgresql_session.add(user)
        postgresql_session.commit()

        assert user.id is not None
        assert user.email == 'test@example.com'
        assert user.first_name == 'Test'


class TestDbUserMultiBackend:
    """Tests that run on both SQLite and PostgreSQL."""

    def test_user_creation(self, multi_backend_session):
        """Test user creation works identically on all backends."""
        user = DbUser(
            email='multi@example.com',
            first_name='Multi',
            last_name='Backend',
            institution='Test'
        )
        multi_backend_session.add(user)
        multi_backend_session.commit()

        # Verify user was created
        retrieved = multi_backend_session.query(DbUser).filter_by(
            email='multi@example.com'
        ).first()

        assert retrieved is not None
        assert retrieved.id == user.id
        assert retrieved.email == user.email
        assert retrieved.first_name == 'Multi'

    def test_email_unique_constraint(self, multi_backend_session):
        """Test that email uniqueness is enforced on all backends."""
        # Create first user
        user1 = DbUser(email='unique@example.com')
        multi_backend_session.add(user1)
        multi_backend_session.commit()

        # Try to create duplicate
        user2 = DbUser(email='unique@example.com')
        multi_backend_session.add(user2)

        with pytest.raises(IntegrityError):
            multi_backend_session.commit()

    def test_default_values(self, multi_backend_session):
        """Test that default values work on all backends."""
        user = DbUser(email='defaults@example.com')
        multi_backend_session.add(user)
        multi_backend_session.commit()

        assert user.first_name == ''
        assert user.last_name == ''
        assert user.institution == ''

    def test_query_by_email(self, multi_backend_session):
        """Test querying users by email works on all backends."""
        # Create multiple users
        users_data = [
            ('alice@example.com', 'Alice'),
            ('bob@example.com', 'Bob'),
            ('charlie@example.com', 'Charlie'),
        ]

        for email, first_name in users_data:
            user = DbUser(email=email, first_name=first_name)
            multi_backend_session.add(user)
        multi_backend_session.commit()

        # Query by email
        alice = multi_backend_session.query(DbUser).filter_by(
            email='alice@example.com'
        ).first()

        assert alice is not None
        assert alice.first_name == 'Alice'

    def test_update_user(self, multi_backend_session):
        """Test updating user attributes works on all backends."""
        user = DbUser(email='update@example.com', first_name='Old')
        multi_backend_session.add(user)
        multi_backend_session.commit()

        # Update
        user.first_name = 'New'
        user.institution = 'Updated'
        multi_backend_session.commit()

        # Verify
        retrieved = multi_backend_session.query(DbUser).filter_by(
            email='update@example.com'
        ).first()

        assert retrieved.first_name == 'New'
        assert retrieved.institution == 'Updated'

    def test_delete_user(self, multi_backend_session):
        """Test deleting users works on all backends."""
        user = DbUser(email='delete@example.com')
        multi_backend_session.add(user)
        multi_backend_session.commit()

        user_id = user.id

        # Delete
        multi_backend_session.delete(user)
        multi_backend_session.commit()

        # Verify deletion
        retrieved = multi_backend_session.query(DbUser).filter_by(
            id=user_id
        ).first()

        assert retrieved is None

    def test_string_representation(self, multi_backend_session):
        """Test __str__ and __repr__ methods."""
        user = DbUser(email='repr@example.com', first_name='Test')
        multi_backend_session.add(user)
        multi_backend_session.commit()

        # Test __str__
        assert str(user) == 'repr@example.com'

        # Test __repr__
        assert 'DbUser' in repr(user)
        assert 'repr@example.com' in repr(user)
