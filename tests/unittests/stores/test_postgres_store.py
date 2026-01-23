# import pytest
# import pickle
# from unittest.mock import MagicMock
# from pydantic_mini import BaseModel
#
# from volnux.backends.stores.postgres_store import PostgresStoreBackend
#
#
# class MockRecord(BaseModel):
#     id: str
#     name: str
#
#     def __setstate__(self, state):
#         self.__dict__.update(state)
#
#     def __getstate__(self):
#         return self.__dict__
#
#
# @pytest.fixture
# def postgres_store():
#     from unittest.mock import patch, MagicMock
#
#     with patch(
#         "volnux.backends.connectors.postgres.PostgresConnector", autospec=True
#     ) as mock_connector_class:
#         mock_instance = MagicMock()
#         mock_instance.connect.return_value = MagicMock()
#         mock_instance.is_connected.return_value = True
#         mock_instance.cursor = MagicMock()
#         mock_instance._cursor = mock_instance.cursor
#         mock_instance.connection = MagicMock()
#
#         mock_connector_class.return_value = mock_instance
#
#         with patch("psycopg.connect") as mock_connect:
#             mock_connect.return_value = MagicMock()
#
#             store = PostgresStoreBackend(
#                 host="localhost",
#                 port=5432,
#                 db="test_db",
#                 username="test_user",
#                 password="test_password",
#             )
#
#             yield store
#
#
# @pytest.fixture
# def sample_record():
#     return MockRecord(id="1", name="John Doe")
#
#
# def test_check_if_schema_exists(postgres_store):
#     postgres_store.connector.cursor.fetchone.return_value = (1,)
#     assert postgres_store._check_if_schema_exists("test_schema") is True
#
#     postgres_store.connector.cursor.fetchone.return_value = None
#     assert postgres_store._check_if_schema_exists("test_schema") is False
#
#
# def test_create_schema(postgres_store, sample_record):
#     postgres_store._check_if_schema_exists = MagicMock(return_value=False)
#     postgres_store._check_connection = MagicMock()
#
#     postgres_store.connector.cursor.execute.reset_mock()
#
#     postgres_store.create_schema("test_schema", sample_record)
#
#     postgres_store.connector.cursor.execute.assert_called_once()
#     call_args = postgres_store.connector.cursor.execute.call_args[0][0]
#     assert "CREATE TABLE test_schema" in call_args
#     assert "id TEXT PRIMARY KEY" in call_args
#     assert "name TEXT" in call_args
#
#
# def test_insert_record(postgres_store, sample_record):
#     postgres_store._check_if_schema_exists = MagicMock(return_value=True)
#     postgres_store.exists = MagicMock(return_value=False)
#     postgres_store._check_connection = MagicMock()
#
#     postgres_store.connector.cursor.execute.reset_mock()
#
#     postgres_store.insert_record("test_schema", "1", sample_record)
#
#     postgres_store.connector.cursor.execute.assert_called_once()
#     postgres_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_update_record(postgres_store, sample_record):
#     postgres_store._check_connection = MagicMock()
#
#     postgres_store.connector.cursor.rowcount = 1
#     postgres_store.connector.cursor.execute.reset_mock()
#
#     postgres_store.update_record("test_schema", "1", sample_record)
#
#     postgres_store.connector.cursor.execute.assert_called_once()
#     postgres_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_delete_record(postgres_store):
#     postgres_store._check_connection = MagicMock()
#     postgres_store.connector.cursor.rowcount = 1
#
#     postgres_store.connector.cursor.execute.reset_mock()
#
#     postgres_store.delete_record("test_schema", "1")
#
#     postgres_store.connector.cursor.execute.assert_called_once()
#     postgres_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_get_record(postgres_store, sample_record):
#     record_state = pickle.dumps(sample_record.__dict__)
#     postgres_store.connector.cursor.fetchone.return_value = (record_state,)
#
#     result = postgres_store.get_record("test_schema", MockRecord, "1")
#     assert isinstance(result, MockRecord)
#     assert result.name == sample_record.name
#
#
# def test_filter_record(postgres_store, sample_record):
#     postgres_store._check_if_schema_exists = MagicMock(return_value=True)
#     record_state = pickle.dumps(sample_record.__dict__)
#     postgres_store.connector.cursor.fetchall.return_value = [(record_state,)]
#
#     results = postgres_store.filter_record("test_schema", MockRecord, name="John Doe")
#     assert len(results) == 1
#     assert isinstance(results[0], MockRecord)
#     assert results[0].name == "John Doe"
#
#
# def test_count(postgres_store):
#     postgres_store._check_if_schema_exists = MagicMock(return_value=True)
#     postgres_store.connector.cursor.fetchone.return_value = (3,)
#
#     count = postgres_store.count("test_schema")
#     assert count == 3
