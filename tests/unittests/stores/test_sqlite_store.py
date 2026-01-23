# import pytest
# import pickle
# from unittest.mock import MagicMock
# from pydantic_mini import BaseModel
#
# from volnux.backends.stores.sqlite_store import SqliteStoreBackend
# from volnux.exceptions import (
#     ObjectDoesNotExist,
#     ObjectExistError,
#     SqlOperationError,
# )
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
# # Fixtures
# @pytest.fixture
# def sqlite_store():
#     store = SqliteStoreBackend(host="localhost", port=6379, db=":memory:")
#     store.connector = MagicMock()
#     store.connector.cursor = MagicMock()
#     store.connector.cursor.connection = MagicMock()
#     store.connector.is_connected = MagicMock(return_value=True)
#     return store
#
#
# @pytest.fixture
# def sample_record():
#     return MockRecord(id="1", name="John Doe")
#
#
# # Helper function to simulate cursor description
# def get_cursor_description():
#     return [
#         ("id", None, None, None, None, None, None),
#         ("name", None, None, None, None, None, None),
#     ]
#
#
# def test_check_connection(sqlite_store):
#     sqlite_store.connector.is_connected.return_value = False
#     with pytest.raises(ConnectionError):
#         sqlite_store._check_connection()
#
#
# def test_check_if_schema_exists(sqlite_store):
#     sqlite_store.connector.cursor.fetchone.return_value = (1,)
#     assert sqlite_store._check_if_schema_exists("test_schema") is True
#
#     sqlite_store.connector.cursor.fetchone.return_value = None
#     assert sqlite_store._check_if_schema_exists("test_schema") is False
#
#
# def test_create_schema(sqlite_store, sample_record):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=False)
#     sqlite_store.create_schema("test_schema", sample_record)
#
#     sqlite_store.connector.cursor.execute.assert_called_once()
#     assert (
#         "CREATE TABLE test_schema"
#         in sqlite_store.connector.cursor.execute.call_args[0][0]
#     )
#
#
# def test_create_existing_schema(sqlite_store, sample_record):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     with pytest.raises(ObjectExistError):
#         sqlite_store.create_schema("test_schema", sample_record)
#
#
# def test_insert_record(sqlite_store, sample_record):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     sqlite_store.exists = MagicMock(return_value=False)
#
#     sqlite_store.insert_record("test_schema", "1", sample_record)
#
#     sqlite_store.connector.cursor.execute.assert_called_once()
#     sqlite_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_insert_existing_record(sqlite_store, sample_record):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     sqlite_store.exists = MagicMock(return_value=True)
#
#     with pytest.raises(ObjectExistError):
#         sqlite_store.insert_record("test_schema", "1", sample_record)
#
#
# def test_update_record(sqlite_store, sample_record):
#     sqlite_store.connector.cursor.rowcount = 1
#     sqlite_store.update_record("test_schema", "1", sample_record)
#
#     sqlite_store.connector.cursor.execute.assert_called_once()
#     sqlite_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_delete_record(sqlite_store):
#     sqlite_store.connector.cursor.rowcount = 1
#     sqlite_store.delete_record("test_schema", "1")
#
#     sqlite_store.connector.cursor.execute.assert_called_once()
#     sqlite_store.connector.cursor.connection.commit.assert_called_once()
#
#
# def test_delete_nonexistent_record(sqlite_store):
#     sqlite_store.connector.cursor.rowcount = 0
#     with pytest.raises(ObjectDoesNotExist):
#         sqlite_store.delete_record("test_schema", "1")
#
#
# def test_get_record(sqlite_store, sample_record):
#     sqlite_store.connector.cursor.fetchone.return_value = (
#         pickle.dumps(sample_record.__dict__),
#     )
#
#     result = sqlite_store.get_record("test_schema", MockRecord, "1")
#     assert isinstance(result, MockRecord)
#     assert result.name == sample_record.name
#
#
# def test_get_nonexistent_record(sqlite_store):
#     sqlite_store.connector.cursor.fetchone.return_value = None
#
#     with pytest.raises(ObjectDoesNotExist):
#         sqlite_store.get_record("test_schema", MockRecord, "1")
#
#
# def test_filter_record_basic(sqlite_store):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     sqlite_store.connector.cursor.description = get_cursor_description()
#     sqlite_store.connector.cursor.fetchall.return_value = [
#         pickle.dumps(MockRecord(id="2", name="John Doe").__dict__),
#         pickle.dumps(MockRecord(id="3", name="Jane Doe").__dict__),
#     ]
#
#     results = sqlite_store.filter_record("test_schema", MockRecord, name="John Doe")
#     assert len(results) == 2
#     assert all(isinstance(r, MockRecord) for r in results)
#
#
# def test_filter_record_complex(sqlite_store):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     sqlite_store.connector.cursor.description = get_cursor_description()
#     sqlite_store.connector.cursor.fetchall.return_value = [
#         pickle.dumps(MockRecord(id="4", name="James Doe").__dict__)
#     ]
#
#     results = sqlite_store.filter_record(
#         "test_schema", MockRecord, age__gt=25, name__contains="John"
#     )
#     assert len(results) == 1
#
#
# def test_filter_record_nonexistent_schema(sqlite_store):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=False)
#
#     with pytest.raises(ObjectDoesNotExist):
#         sqlite_store.filter_record("test_schema", MockRecord, name="John")
#
#
# def test_build_sql_filter(sqlite_store):
#     where_clause, params = sqlite_store._build_sql_filter(
#         {"name": "John", "age__gt": 25, "email__contains": "@example.com"}
#     )
#
#     assert "name = ?" in where_clause
#     assert "age > ?" in where_clause
#     assert "email LIKE ?" in where_clause
#     assert len(params) == 3
#     assert params[0] == "John"
#     assert params[1] == 25
#     assert params[2] == "%@example.com%"
#
#
# def test_count(sqlite_store):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=True)
#     sqlite_store.connector.cursor.fetchone.return_value = (5,)
#
#     count = sqlite_store.count("test_schema")
#     assert count == 5
#
#
# def test_count_nonexistent_schema(sqlite_store):
#     sqlite_store._check_if_schema_exists = MagicMock(return_value=False)
#
#     with pytest.raises(ObjectDoesNotExist):
#         sqlite_store.count("test_schema")
