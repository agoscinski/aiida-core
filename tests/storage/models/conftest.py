"""Pytest fixtures for testing unified models with multiple database backends."""

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

# Import the unified base to ensure all models are registered
from aiida.storage.models.base import Base


def _enable_sqlite_foreign_keys(dbapi_conn, connection_record):
    """Enable foreign key constraints in SQLite."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


@pytest.fixture(scope='function')
def sqlite_engine():
    """Create a SQLite in-memory engine for testing."""
    engine = create_engine('sqlite:///:memory:', echo=False)
    # Enable foreign key constraints for SQLite
    event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope='function')
def sqlite_session(sqlite_engine):
    """Create a SQLite session for testing."""
    Session = sessionmaker(bind=sqlite_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture(scope='function')
def postgresql_engine():
    """Create a PostgreSQL engine for testing.

    Requires PostgreSQL server to be running and psycopg2 installed.
    Tests will be skipped if PostgreSQL is not available.
    """
    try:
        engine = create_engine(
            'postgresql://localhost/aiida_test_unified',
            echo=False
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute('SELECT 1')
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()
    except Exception as e:
        pytest.skip(f'PostgreSQL not available: {e}')


@pytest.fixture(scope='function')
def postgresql_session(postgresql_engine):
    """Create a PostgreSQL session for testing."""
    Session = sessionmaker(bind=postgresql_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture(params=['sqlite', 'postgresql'])
def multi_backend_session(request):
    """Parametrized fixture that runs tests on both SQLite and PostgreSQL.

    This ensures that unified models work identically on both backends.
    PostgreSQL tests will be skipped if the server is not available.
    """
    if request.param == 'sqlite':
        engine = create_engine('sqlite:///:memory:', echo=False)
        # Enable foreign key constraints for SQLite
        event.listen(engine, "connect", _enable_sqlite_foreign_keys)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()
    else:  # postgresql
        try:
            engine = create_engine(
                'postgresql://localhost/aiida_test_unified',
                echo=False
            )
            with engine.connect() as conn:
                conn.execute('SELECT 1')
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            session = Session()
            yield session
            session.close()
            Base.metadata.drop_all(engine)
            engine.dispose()
        except Exception as e:
            pytest.skip(f'PostgreSQL not available: {e}')
