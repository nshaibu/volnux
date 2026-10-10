import pytest
import orjson as json
from unittest.mock import MagicMock

from volnux.backends.storage_route import StorageRoute
from volnux.backends.stores.redis import RedisStoreBackend
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError


class MockRecord:
    def __init__(self, id: str, name: str, status: str = "active"):
        self.id = id
        self.name = name
        self.status = status

    @classmethod
    def get_storage_route(cls):
        return StorageRoute(
            components=["volnux", "test_redis_store"]
        )

    def __getstate__(self):
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
        }

    def __setstate__(self, state):
        self.id = state["id"]
        self.name = state["name"]
        self.status = state["status"]


class MockPipeline:
    def __init__(self):
        self.hset = MagicMock()
        self.expire = MagicMock()
        self.hdel = MagicMock()
        self.hsetnx = MagicMock()
        self.hexists = MagicMock()
        self.execute = MagicMock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


@pytest.fixture
def redis_store():
    connector = MagicMock()
    connector.is_connected = MagicMock(return_value=True)
    connector.connect = MagicMock()
    connector.cursor = MagicMock()
    connector.get_pipeline = MagicMock(
        side_effect=lambda transaction=True: MockPipeline()
    )

    original_connector_klass = RedisStoreBackend.connector_klass
    RedisStoreBackend.connector_klass = MagicMock(return_value=connector)

    try:
        store = RedisStoreBackend(host="localhost", port=6379, database=0)
        store.connector = connector
        yield store
    finally:
        RedisStoreBackend.connector_klass = original_connector_klass


@pytest.fixture
def sample_record():
    return MockRecord(id="1", name="John Doe", status="active")


def test_init_connects_if_not_connected():
    connector = MagicMock()
    connector.is_connected = MagicMock(return_value=False)
    connector.connect = MagicMock()
    connector.cursor = MagicMock()
    connector.get_pipeline = MagicMock(
        side_effect=lambda transaction=True: MockPipeline()
    )

    original_connector_klass = RedisStoreBackend.connector_klass
    RedisStoreBackend.connector_klass = MagicMock(return_value=connector)

    try:
        RedisStoreBackend(host="localhost", port=6379, database=0)
        connector.connect.assert_called_once()
    finally:
        RedisStoreBackend.connector_klass = original_connector_klass


def test_exists_returns_true(redis_store):
    redis_store.connector.cursor.hexists.return_value = True

    assert redis_store.exists("users", "1") is True
    redis_store.connector.cursor.hexists.assert_called_once_with("users", "1")


def test_exists_returns_false(redis_store):
    redis_store.connector.cursor.hexists.return_value = False

    assert redis_store.exists("users", "1") is False


def test_insert_saves_record_and_ttl(redis_store, sample_record):
    redis_store.connector.cursor.hsetnx = MagicMock(return_value=True)
    redis_store.connector.cursor.expire = MagicMock()

    redis_store.insert("users", "1", sample_record, ttl=60)

    redis_store.connector.cursor.hsetnx.assert_called_once_with(
        "users",
        "1",
        json.dumps(sample_record.__getstate__()).decode(),
    )
    redis_store.connector.cursor.expire.assert_called_once_with("users", 60)
    # redis_store.connector.execute.assert_called_once()


def test_insert_raises_when_record_exists(redis_store, sample_record):
    redis_store.connector.cursor.hsetnx.return_value = False

    with pytest.raises(ObjectExistError):
        redis_store.insert("users", "1", sample_record)


def test_update_updates_existing_record(redis_store, sample_record):
    redis_store.connector.cursor.hexists.return_value = True
    pipeline = MockPipeline()
    redis_store.connector.cursor.pipeline = MagicMock(return_value=pipeline)

    redis_store.update("users", "1", sample_record)

    pipeline.hset.assert_called_once_with(
        "users",
        "1",
        json.dumps(sample_record.__getstate__()).decode(),
    )
    pipeline.execute.assert_called_once()


def test_update_raises_when_record_missing(redis_store, sample_record):
    redis_store.connector.cursor.hexists.return_value = False
    pipeline = MockPipeline()
    pipeline.execute.return_value = [None]
    redis_store.connector.cursor.pipeline = MagicMock(return_value=pipeline)

    with pytest.raises(ObjectDoesNotExist):
        redis_store.update("users", "1", sample_record)


def test_delete_removes_existing_record(redis_store):
    redis_store.connector.cursor.hdel.return_value = 1
    redis_store.delete("users", "1")

    redis_store.connector.cursor.hdel.assert_called_once_with("users", "1")


def test_delete_raises_when_record_missing(redis_store):
    redis_store.connector.cursor.hdel.return_value = 0

    with pytest.raises(ObjectDoesNotExist):
        redis_store.delete("users", "1")


def test_get_returns_deserialized_record(redis_store):
    redis_store.connector.cursor.hexists.return_value = True
    redis_store.connector.cursor.hget.return_value = json.dumps(
        {"id": "1", "name": "John Doe", "status": "active"}
    )

    record = redis_store.get("users", "1", MockRecord)

    assert isinstance(record, MockRecord)
    assert record.id == "1"
    assert record.name == "John Doe"
    assert record.status == "active"


def test_get_raises_when_record_missing_before_fetch(redis_store):
    redis_store.connector.cursor.hget.return_value = None

    with pytest.raises(ObjectDoesNotExist):
        redis_store.get("users", "1", MockRecord)


def test_get_raises_when_hget_returns_none(redis_store):
    redis_store.connector.cursor.hexists.return_value = True
    redis_store.connector.cursor.hget.return_value = None

    with pytest.raises(ObjectDoesNotExist):
        redis_store.get("users", "1", MockRecord)


def test_filter_returns_only_matching_records(redis_store):
    redis_store.connector.cursor.hscan.side_effect = [
        (
            1,
            {
                "1": json.dumps({"id": "1", "name": "John Doe", "status": "active"}),
            },
        ),
        (
            0,
            {
                "2": json.dumps({"id": "2", "name": "Jane Doe", "status": "inactive"}),
            },
        ),
    ]

    results = redis_store.filter("users", MockRecord, status="active")

    assert len(results) == 1
    assert results._keys == ["1"]


def test_count_without_filters_uses_hlen(redis_store):
    redis_store.connector.cursor.hlen.return_value = 2

    result = redis_store.count("users", MockRecord)

    assert result == 2
    redis_store.connector.cursor.hlen.assert_called_once_with("users")


def test_count_with_filters_uses_filter(redis_store):
    redis_store.filter = MagicMock(
        return_value=[MockRecord("1", "John"), MockRecord("2", "Jane")]
    )

    result = redis_store.count("users", MockRecord, status="active")

    assert result == 2
    redis_store.filter.assert_called_once_with("users", MockRecord, status="active")


def test_reload_updates_existing_object(redis_store):
    redis_store.connector.cursor.hexists.return_value = True
    redis_store.connector.cursor.hget.return_value = json.dumps(
        {"id": "1", "name": "Updated Name", "status": "inactive"}
    )
    record = MockRecord(id="1", name="Old Name", status="active")

    reloaded = redis_store.reload("users", record)

    assert reloaded is record
    assert record.id == "1"
    assert record.name == "Updated Name"
    assert record.status == "inactive"


def test_reload_requires_id_attribute(redis_store):
    class NoIdRecord:
        pass

    with pytest.raises(ValueError):
        redis_store.reload("users", NoIdRecord())


def test_bulk_insert_writes_all_records(redis_store):
    records = {
        "1": MockRecord("1", "John Doe", "active"),
        "2": MockRecord("2", "Jane Doe", "inactive"),
    }
    pipeline = MockPipeline()
    redis_store.connector.cursor.pipeline = MagicMock(return_value=pipeline)

    redis_store.bulk_insert("users", records)

    assert pipeline.hsetnx.call_count == 2
    pipeline.hsetnx.assert_any_call("users", "1", json.dumps(records["1"].__getstate__()))
    pipeline.hsetnx.assert_any_call("users", "2", json.dumps(records["2"].__getstate__()))
    pipeline.execute.assert_called_once()


def test_bulk_delete_removes_all_keys(redis_store):
    pipeline = MockPipeline()
    redis_store.connector.cursor.pipeline = MagicMock(return_value=pipeline)

    redis_store.bulk_delete("users", ["1", "2", "3"])

    assert pipeline.hdel.call_count == 3
    pipeline.hdel.assert_any_call("users", "1")
    pipeline.hdel.assert_any_call("users", "2")
    pipeline.hdel.assert_any_call("users", "3")
    pipeline.execute.assert_called_once()


def test_clear_schema_deletes_schema(redis_store):
    redis_store.clear_schema("users")

    redis_store.connector.cursor.delete.assert_called_once_with("users")


def test_list_schemas_returns_only_hash_keys(redis_store):
    redis_store.connector.cursor.keys.return_value = [b"users", b"jobs", b"counter"]
    redis_store.connector.cursor.type.side_effect = [b"hash", b"hash", b"string"]

    schemas = redis_store.list_schemas()

    assert schemas == ["users", "jobs"]


def test_ensure_connected_reconnects_when_disconnected(redis_store):
    redis_store.connector.is_connected.return_value = False

    redis_store._ensure_connected()

    redis_store.connector.connect.assert_called_once()
