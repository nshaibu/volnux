# import pytest
# import pickle
# from unittest.mock import MagicMock, patch
# from pydantic_mini import BaseModel
# from volnux.backends.stores.redis_store import RedisStoreBackend
# from volnux.exceptions import ObjectDoesNotExist, ObjectExistError
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
# def redis_store():
#     store = RedisStoreBackend(host="localhost", port=6379, db=0)
#     store.connector = MagicMock()
#     store.connector.cursor = MagicMock()
#     return store
#
#
# def test_exists(redis_store):
#     redis_store.connector.cursor.hexists.return_value = True
#     assert redis_store.exists("test_schema", "test_key") is True
#     redis_store.connector.cursor.hexists.assert_called_once_with(
#         "test_schema", "test_key"
#     )
#
#
# def test_count(redis_store):
#     redis_store.connector.cursor.hlen.return_value = 5
#     assert redis_store.count("test_schema") == 5
#     redis_store.connector.cursor.hlen.assert_called_once_with("test_schema")
#
#
# def test_insert_record(redis_store):
#     redis_store.connector.cursor.hexists.return_value = False
#     record = MockRecord(id="1", name="Test")
#     redis_store.insert_record("test_schema", "test_key", record)
#     redis_store.connector.cursor.pipeline.assert_called_once()
#
#
# def test_insert_record_exists(redis_store):
#     redis_store.connector.cursor.hexists.return_value = True
#     record = MockRecord(id="1", name="Test")
#     with pytest.raises(ObjectExistError):
#         redis_store.insert_record("test_schema", "test_key", record)
#
#
# def test_update_record(redis_store):
#     redis_store.connector.cursor.hexists.return_value = True
#     record = MockRecord(id="1", name="Updated")
#     redis_store.update_record("test_schema", "test_key", record)
#     redis_store.connector.cursor.pipeline.assert_called_once()
#
#
# def test_update_record_not_exists(redis_store):
#     redis_store.connector.cursor.hexists.return_value = False
#     record = MockRecord(id="1", name="Updated")
#     with pytest.raises(ObjectDoesNotExist):
#         redis_store.update_record("test_schema", "test_key", record)
#
#
# def test_delete_record(redis_store):
#     redis_store.connector.cursor.hexists.return_value = True
#     redis_store.delete_record("test_schema", "test_key")
#     redis_store.connector.cursor.pipeline.assert_called_once()
#
#
# def test_delete_record_not_exists(redis_store):
#     redis_store.connector.cursor.hexists.return_value = False
#     with pytest.raises(ObjectDoesNotExist):
#         redis_store.delete_record("test_schema", "test_key")
#
#
# def test_get_record(redis_store):
#     redis_store.connector.cursor.hexists.return_value = True
#     redis_store.connector.cursor.hget.return_value = pickle.dumps(
#         {"id": "1", "name": "Test"}
#     )
#     record = redis_store.get_record("test_schema", MockRecord, "test_key")
#     assert record.id == "1"
#     assert record.name == "Test"
#
#
# def test_get_record_not_exists(redis_store):
#     redis_store.connector.cursor.hexists.return_value = False
#     with pytest.raises(ObjectDoesNotExist):
#         redis_store.get_record("test_schema", MockRecord, "test_key")
#
#
# def test_filter_record(redis_store):
#     redis_store.connector.cursor.hscan.side_effect = [
#         (1, {"key1": pickle.dumps({"id": "1", "name": "Test1"})}),
#         (0, {"key2": pickle.dumps({"id": "2", "name": "Test2"})}),
#     ]
#     records = redis_store.filter_record("test_schema", MockRecord, name="Test1")
#     assert len(records) == 1
#     assert records[0].name == "Test1"
