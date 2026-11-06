# import orjson as json
# from unittest.mock import MagicMock
#
# import pytest
#
# from volnux.backends.stores.memcache import MemcacheStoreBackend
# from volnux.exceptions import ObjectDoesNotExist, ObjectExistError
#
#
# class MockRecord:
#     def __init__(self, id: str, name: str, status: str = "active"):
#         self.id = id
#         self.name = name
#         self.status = status
#
#     def __getstate__(self):
#         return {
#             "id": self.id,
#             "name": self.name,
#             "status": self.status,
#         }
#
#     def __setstate__(self, state):
#         self.id = state["id"]
#         self.name = state["name"]
#         self.status = state["status"]
#
#
# @pytest.fixture
# def sample_record():
#     return MockRecord(id="1", name="John Doe", status="active")
#
#
# @pytest.fixture
# def memcache_store():
#     connector = MagicMock()
#     connector.is_connected = MagicMock(return_value=True)
#     connector.connect = MagicMock()
#     connector.cursor = MagicMock()
#
#     original_connector_klass = MemcacheStoreBackend.connector_klass
#     MemcacheStoreBackend.connector_klass = MagicMock(return_value=connector)
#
#     try:
#         store = MemcacheStoreBackend(host="localhost", port=11211)
#         store.connector = connector
#         yield store
#     finally:
#         MemcacheStoreBackend.connector_klass = original_connector_klass
#
#
# def test_init_connects_if_not_connected():
#     connector = MagicMock()
#     connector.is_connected = MagicMock(return_value=False)
#     connector.connect = MagicMock()
#     connector.cursor = MagicMock()
#
#     original_connector_klass = MemcacheStoreBackend.connector_klass
#     MemcacheStoreBackend.connector_klass = MagicMock(return_value=connector)
#
#     try:
#         MemcacheStoreBackend(host="localhost", port=11211)
#         connector.connect.assert_called_once()
#     finally:
#         MemcacheStoreBackend.connector_klass = original_connector_klass
#
#
# def test_build_index_key_without_namespace(memcache_store):
#     assert memcache_store._build_index_key("users") == "users_index"
#
#
# def test_build_index_key_with_namespace():
#     connector = MagicMock()
#     connector.is_connected = MagicMock(return_value=True)
#     connector.connect = MagicMock()
#     connector.cursor = MagicMock()
#
#     original_connector_klass = MemcacheStoreBackend.connector_klass
#     MemcacheStoreBackend.connector_klass = MagicMock(return_value=connector)
#
#     try:
#         store = MemcacheStoreBackend(
#             host="localhost",
#             port=11211,
#             namespace_prefix="app",
#         )
#         assert store._build_index_key("users") == "app:users_index"
#     finally:
#         MemcacheStoreBackend.connector_klass = original_connector_klass
#
#
# def test_exists_returns_true(memcache_store):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#
#     assert memcache_store.exists("users", "1") is True
#     memcache_store.connector.cursor.get.assert_called_once_with("users:1")
#
#
# def test_exists_returns_false(memcache_store):
#     memcache_store.connector.cursor.get.return_value = None
#
#     assert memcache_store.exists("users", "1") is False
#
#
# def test_insert_adds_record_and_updates_index(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = None
#     memcache_store.connector.cursor.add.return_value = True
#     memcache_store._update_schema_index = MagicMock()
#
#     memcache_store.insert("users", "1", sample_record, ttl=60)
#
#     memcache_store.connector.cursor.add.assert_called_once_with(
#         "users:1",
#         json.dumps(sample_record.__getstate__()),
#         expire=60,
#     )
#     memcache_store._update_schema_index.assert_called_once_with(
#         "users",
#         "1",
#         operation="add",
#     )
#
#
# def test_insert_uses_default_ttl_when_ttl_not_provided(sample_record):
#     connector = MagicMock()
#     connector.is_connected = MagicMock(return_value=True)
#     connector.connect = MagicMock()
#     connector.cursor = MagicMock()
#     connector.cursor.get.return_value = None
#     connector.cursor.add.return_value = True
#
#     original_connector_klass = MemcacheStoreBackend.connector_klass
#     MemcacheStoreBackend.connector_klass = MagicMock(return_value=connector)
#
#     try:
#         store = MemcacheStoreBackend(host="localhost", port=11211, default_ttl=120)
#         store._update_schema_index = MagicMock()
#
#         store.insert("users", "1", sample_record)
#
#         connector.cursor.add.assert_called_once_with(
#             "users:1",
#             json.dumps(sample_record.__getstate__()),
#             expire=120,
#         )
#     finally:
#         MemcacheStoreBackend.connector_klass = original_connector_klass
#
#
# def test_insert_raises_when_record_exists(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#
#     with pytest.raises(ObjectExistError):
#         memcache_store.insert("users", "1", sample_record)
#
#
# def test_insert_raises_when_add_returns_false(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = None
#     memcache_store.connector.cursor.add.return_value = False
#
#     with pytest.raises(ObjectExistError):
#         memcache_store.insert("users", "1", sample_record)
#
#
# def test_update_replaces_existing_record(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#     memcache_store.connector.cursor.replace.return_value = True
#
#     memcache_store.update("users", "1", sample_record, ttl=45)
#
#     memcache_store.connector.cursor.replace.assert_called_once_with(
#         "users:1",
#         json.dumps(sample_record.__getstate__()),
#         expire=45,
#     )
#
#
# def test_update_raises_when_record_missing(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = None
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.update("users", "1", sample_record)
#
#
# def test_update_raises_when_replace_returns_false(memcache_store, sample_record):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#     memcache_store.connector.cursor.replace.return_value = False
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.update("users", "1", sample_record)
#
#
# def test_upsert_sets_record_and_updates_index(memcache_store, sample_record):
#     memcache_store._update_schema_index = MagicMock()
#
#     memcache_store.upsert("users", "1", sample_record, ttl=90)
#
#     memcache_store.connector.cursor.set.assert_called_once_with(
#         "users:1",
#         json.dumps(sample_record.__getstate__()),
#         expire=90,
#     )
#     memcache_store._update_schema_index.assert_called_once_with(
#         "users",
#         "1",
#         operation="add",
#     )
#
#
# def test_delete_removes_record_and_updates_index(memcache_store):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#     memcache_store.connector.cursor.delete.return_value = True
#     memcache_store._update_schema_index = MagicMock()
#
#     memcache_store.delete("users", "1")
#
#     memcache_store.connector.cursor.delete.assert_called_once_with("users:1")
#     memcache_store._update_schema_index.assert_called_once_with(
#         "users",
#         "1",
#         operation="remove",
#     )
#
#
# def test_delete_raises_when_record_missing(memcache_store):
#     memcache_store.connector.cursor.get.return_value = None
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.delete("users", "1")
#
#
# def test_delete_raises_when_delete_returns_false(memcache_store):
#     memcache_store.connector.cursor.get.return_value = '{"id": "1"}'
#     memcache_store.connector.cursor.delete.return_value = False
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.delete("users", "1")
#
#
# def test_get_returns_deserialized_record(memcache_store):
#     memcache_store.connector.cursor.get.return_value = json.dumps(
#         {"id": "1", "name": "John Doe", "status": "active"}
#     )
#
#     record = memcache_store.get("users", "1", MockRecord)
#
#     assert isinstance(record, MockRecord)
#     assert record.id == "1"
#     assert record.name == "John Doe"
#     assert record.status == "active"
#
#
# def test_get_raises_when_record_missing(memcache_store):
#     memcache_store.connector.cursor.get.return_value = None
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.get("users", "1", MockRecord)
#
#
# def test_filter_returns_only_matching_records(memcache_store):
#     memcache_store._get_schema_index = MagicMock(return_value={"1", "2"})
#     memcache_store.connector.cursor.get_many.return_value = {
#         "users:1": json.dumps({"id": "1", "name": "John Doe", "status": "active"}),
#         "users:2": json.dumps({"id": "2", "name": "Jane Doe", "status": "inactive"}),
#     }
#
#     results = memcache_store.filter("users", MockRecord, status="active")
#
#     assert len(results) == 1
#     assert results[0].id == "1"
#     assert results[0].name == "John Doe"
#     assert results[0].status == "active"
#
#
# def test_filter_returns_empty_when_indexing_disabled(sample_record):
#     connector = MagicMock()
#     connector.is_connected = MagicMock(return_value=True)
#     connector.connect = MagicMock()
#     connector.cursor = MagicMock()
#
#     original_connector_klass = MemcacheStoreBackend.connector_klass
#     MemcacheStoreBackend.connector_klass = MagicMock(return_value=connector)
#
#     try:
#         store = MemcacheStoreBackend(
#             host="localhost",
#             port=11211,
#             enable_indexing=False,
#         )
#         results = store.filter("users", MockRecord, status="active")
#         assert results == []
#     finally:
#         MemcacheStoreBackend.connector_klass = original_connector_klass
#
#
# def test_count_without_filters_uses_schema_index(memcache_store):
#     memcache_store._get_schema_index = MagicMock(return_value={"1", "2", "3"})
#
#     result = memcache_store.count("users")
#
#     assert result == 3
#     memcache_store._get_schema_index.assert_called_once_with("users")
#
#
# def test_count_with_filters_uses_filter(memcache_store):
#     memcache_store.filter = MagicMock(
#         return_value=[MockRecord("1", "John"), MockRecord("2", "Jane")]
#     )
#
#     result = memcache_store.count("users", status="active")
#
#     assert result == 2
#     memcache_store.filter.assert_called_once()
#
#
# def test_reload_updates_existing_object(memcache_store):
#     memcache_store.connector.cursor.get.return_value = json.dumps(
#         {"id": "1", "name": "Updated Name", "status": "inactive"}
#     )
#     record = MockRecord(id="1", name="Old Name", status="active")
#
#     reloaded = memcache_store.reload("users", record)
#
#     assert reloaded is record
#     assert record.id == "1"
#     assert record.name == "Updated Name"
#     assert record.status == "inactive"
#
#
# def test_reload_requires_id_attribute(memcache_store):
#     class NoIdRecord:
#         pass
#
#     with pytest.raises(ValueError):
#         memcache_store.reload("users", NoIdRecord())
#
#
# def test_reload_raises_when_record_missing(memcache_store):
#     memcache_store.connector.cursor.get.return_value = None
#     record = MockRecord(id="1", name="John Doe")
#
#     with pytest.raises(ObjectDoesNotExist):
#         memcache_store.reload("users", record)
#
#
# def test_bulk_insert_sets_many_and_updates_index_for_successes(memcache_store):
#     records = {
#         "1": MockRecord("1", "John Doe", "active"),
#         "2": MockRecord("2", "Jane Doe", "inactive"),
#     }
#     memcache_store.connector.cursor.set_many.return_value = ["users:2"]
#     memcache_store._update_schema_index = MagicMock()
#
#     memcache_store.bulk_insert("users", records, ttl=30)
#
#     memcache_store.connector.cursor.set_many.assert_called_once_with(
#         {
#             "users:1": json.dumps(records["1"].__getstate__()),
#             "users:2": json.dumps(records["2"].__getstate__()),
#         },
#         expire=30,
#         noreply=False,
#     )
#     memcache_store._update_schema_index.assert_called_once_with(
#         "users",
#         "1",
#         operation="add",
#     )
#
#
# def test_bulk_insert_returns_early_for_empty_input(memcache_store):
#     memcache_store.bulk_insert("users", {})
#
#     memcache_store.connector.cursor.set_many.assert_not_called()
#
#
# def test_bulk_delete_deletes_many_and_updates_index(memcache_store):
#     memcache_store._update_schema_index = MagicMock()
#
#     memcache_store.bulk_delete("users", ["1", "2", "3"])
#
#     memcache_store.connector.cursor.delete_many.assert_called_once_with(
#         ["users:1", "users:2", "users:3"],
#         noreply=False,
#     )
#     assert memcache_store._update_schema_index.call_count == 3
#     memcache_store._update_schema_index.assert_any_call(
#         "users", "1", operation="remove"
#     )
#     memcache_store._update_schema_index.assert_any_call(
#         "users", "2", operation="remove"
#     )
#     memcache_store._update_schema_index.assert_any_call(
#         "users", "3", operation="remove"
#     )
#
#
# def test_bulk_delete_returns_early_for_empty_input(memcache_store):
#     memcache_store.bulk_delete("users", [])
#
#     memcache_store.connector.cursor.delete_many.assert_not_called()
#
#
# def test_clear_schema_deletes_all_records_and_index(memcache_store):
#     memcache_store._get_schema_index = MagicMock(return_value={"1", "2"})
#     memcache_store.bulk_delete = MagicMock()
#
#     memcache_store.clear_schema("users")
#
#     memcache_store.bulk_delete.assert_called_once()
#     memcache_store.connector.cursor.delete.assert_called_once_with("users_index")
#
#
# def test_clear_schema_noops_when_schema_is_empty(memcache_store):
#     memcache_store._get_schema_index = MagicMock(return_value=set())
#     memcache_store.bulk_delete = MagicMock()
#
#     memcache_store.clear_schema("users")
#
#     memcache_store.bulk_delete.assert_not_called()
#     memcache_store.connector.cursor.delete.assert_not_called()
#
#
# def test_list_all_delegates_to_filter(memcache_store):
#     expected = [MockRecord("1", "John Doe")]
#     memcache_store.filter = MagicMock(return_value=expected)
#
#     result = memcache_store.list_all("users", MockRecord)
#
#     assert result == expected
#     memcache_store.filter.assert_called_once_with("users", MockRecord)
#
#
# def test_ensure_connected_reconnects_when_disconnected(memcache_store):
#     memcache_store.connector.is_connected.return_value = False
#
#     memcache_store._ensure_connected()
#
#     memcache_store.connector.connect.assert_called_once()
