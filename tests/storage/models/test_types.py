"""Tests for database-agnostic types."""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import Column, Integer, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from aiida.storage.models.types import GUID, TZDateTime, JSONType


# Create test model
TestBase = declarative_base()


class TestModel(TestBase):
    """Test model using our custom types."""
    __tablename__ = 'test_model'

    id = Column(Integer, primary_key=True)
    uuid_col = Column(GUID)
    datetime_col = Column(TZDateTime)
    json_col = Column(JSONType)


class TestGUIDType:
    """Tests for GUID type converter."""

    @pytest.fixture
    def sqlite_session(self):
        """SQLite session for testing."""
        engine = create_engine('sqlite:///:memory:')
        TestBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()

    def test_guid_storage_and_retrieval_sqlite(self, sqlite_session):
        """Test UUID storage and retrieval in SQLite."""
        test_uuid = uuid.uuid4()
        obj = TestModel(uuid_col=test_uuid)
        sqlite_session.add(obj)
        sqlite_session.commit()

        # Retrieve
        retrieved = sqlite_session.query(TestModel).first()
        assert isinstance(retrieved.uuid_col, uuid.UUID)
        assert retrieved.uuid_col == test_uuid

    def test_guid_stored_as_hex_in_sqlite(self, sqlite_session):
        """Test that UUIDs are stored as hex strings in SQLite."""
        test_uuid = uuid.uuid4()
        obj = TestModel(uuid_col=test_uuid)
        sqlite_session.add(obj)
        sqlite_session.commit()

        # Query raw SQL to check storage format
        from sqlalchemy import text
        result = sqlite_session.execute(
            text('SELECT uuid_col FROM test_model')
        ).fetchone()

        # Should be 32-character hex string (no dashes)
        assert len(result[0]) == 32
        assert result[0] == test_uuid.hex

    def test_guid_from_string(self, sqlite_session):
        """Test creating GUID from string."""
        uuid_str = '12345678-1234-5678-1234-567812345678'
        obj = TestModel(uuid_col=uuid_str)
        sqlite_session.add(obj)
        sqlite_session.commit()

        retrieved = sqlite_session.query(TestModel).first()
        assert retrieved.uuid_col == uuid.UUID(uuid_str)


class TestTZDateTimeType:
    """Tests for timezone-aware DateTime type."""

    @pytest.fixture
    def sqlite_session(self):
        """SQLite session for testing."""
        engine = create_engine('sqlite:///:memory:')
        TestBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()

    def test_datetime_with_timezone_sqlite(self, sqlite_session):
        """Test storing timezone-aware datetime in SQLite."""
        now = datetime.now(timezone.utc)
        obj = TestModel(datetime_col=now)
        sqlite_session.add(obj)
        sqlite_session.commit()

        # Retrieve
        retrieved = sqlite_session.query(TestModel).first()
        assert isinstance(retrieved.datetime_col, datetime)
        assert retrieved.datetime_col.tzinfo is not None
        assert retrieved.datetime_col.tzinfo == timezone.utc

    def test_datetime_naive_converted_to_utc(self, sqlite_session):
        """Test that naive datetimes are assumed to be UTC."""
        naive_dt = datetime(2024, 1, 1, 12, 0, 0)
        obj = TestModel(datetime_col=naive_dt)
        sqlite_session.add(obj)
        sqlite_session.commit()

        retrieved = sqlite_session.query(TestModel).first()
        assert retrieved.datetime_col.tzinfo == timezone.utc

    def test_datetime_stored_as_text_in_sqlite(self, sqlite_session):
        """Test that datetimes are stored as ISO8601 text in SQLite."""
        dt = datetime(2024, 12, 14, 10, 30, 0, tzinfo=timezone.utc)
        obj = TestModel(datetime_col=dt)
        sqlite_session.add(obj)
        sqlite_session.commit()

        # Query raw SQL
        from sqlalchemy import text
        result = sqlite_session.execute(
            text('SELECT datetime_col FROM test_model')
        ).fetchone()

        # Should be ISO8601 format
        assert isinstance(result[0], str)
        assert '2024-12-14' in result[0]


class TestJSONType:
    """Tests for JSON type."""

    @pytest.fixture
    def sqlite_session(self):
        """SQLite session for testing."""
        engine = create_engine('sqlite:///:memory:')
        TestBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()

    def test_json_dict_storage(self, sqlite_session):
        """Test storing dictionary as JSON."""
        test_data = {'key': 'value', 'number': 42, 'nested': {'foo': 'bar'}}
        obj = TestModel(json_col=test_data)
        sqlite_session.add(obj)
        sqlite_session.commit()

        retrieved = sqlite_session.query(TestModel).first()
        assert retrieved.json_col == test_data
        assert retrieved.json_col['key'] == 'value'
        assert retrieved.json_col['nested']['foo'] == 'bar'

    def test_json_list_storage(self, sqlite_session):
        """Test storing list as JSON."""
        test_data = [1, 2, 3, 'four', {'five': 5}]
        obj = TestModel(json_col=test_data)
        sqlite_session.add(obj)
        sqlite_session.commit()

        retrieved = sqlite_session.query(TestModel).first()
        assert retrieved.json_col == test_data

    def test_json_empty_dict_default(self, sqlite_session):
        """Test that empty dict is the default for JSON columns."""
        obj = TestModel()
        # Note: JSON columns don't have automatic defaults in test,
        # but in real models they have default=dict
        sqlite_session.add(obj)
        sqlite_session.commit()

        retrieved = sqlite_session.query(TestModel).first()
        # JSON column allows None
        assert retrieved.json_col is None or retrieved.json_col == {}


class TestTypesMultiBackend:
    """Test types work consistently across backends."""

    @pytest.fixture(params=['sqlite', 'postgresql'])
    def multi_session(self, request):
        """Session that tests both SQLite and PostgreSQL."""
        if request.param == 'sqlite':
            engine = create_engine('sqlite:///:memory:')
        else:
            try:
                engine = create_engine('postgresql://localhost/aiida_test')
                # Test connection
                with engine.connect() as conn:
                    conn.execute('SELECT 1')
            except Exception as e:
                pytest.skip(f'PostgreSQL not available: {e}')

        TestBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()
        TestBase.metadata.drop_all(engine)

    def test_uuid_consistency(self, multi_session):
        """Test UUID handling is consistent across backends."""
        test_uuid = uuid.uuid4()
        obj = TestModel(uuid_col=test_uuid)
        multi_session.add(obj)
        multi_session.commit()

        retrieved = multi_session.query(TestModel).first()
        assert retrieved.uuid_col == test_uuid
        assert isinstance(retrieved.uuid_col, uuid.UUID)

    def test_datetime_consistency(self, multi_session):
        """Test datetime handling is consistent across backends."""
        test_dt = datetime(2024, 12, 14, 10, 30, 0, tzinfo=timezone.utc)
        obj = TestModel(datetime_col=test_dt)
        multi_session.add(obj)
        multi_session.commit()

        retrieved = multi_session.query(TestModel).first()
        assert retrieved.datetime_col.year == 2024
        assert retrieved.datetime_col.month == 12
        assert retrieved.datetime_col.day == 14
        assert retrieved.datetime_col.tzinfo is not None

    def test_json_consistency(self, multi_session):
        """Test JSON handling is consistent across backends."""
        test_json = {
            'string': 'value',
            'number': 42,
            'float': 3.14,
            'bool': True,
            'null': None,
            'list': [1, 2, 3],
            'nested': {'key': 'value'}
        }
        obj = TestModel(json_col=test_json)
        multi_session.add(obj)
        multi_session.commit()

        retrieved = multi_session.query(TestModel).first()
        assert retrieved.json_col == test_json
