import asyncio
import pickle
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import (
    ImproperlyConfigured,
    ObjectDoesNotExist,
    ObjectExistError,
    SerializationError,
)
from volnux.mixins.key_value_store_integration import (
    KeyValueStoreIntegrationMixin,
    backend_operation,
)

SCHEMA = "volnux_ExampleModel"


class ExampleModel(KeyValueStoreIntegrationMixin):
    def __init__(self, id="1", name="Alice", status="active"):
        self.name = name
        self.status = status
        # Formax models call __post_init__ automatically; plain classes must.
        self.__post_init__()
        self.change_object_id(id)

    def get_state(self):
        return {"name": self.name, "status": self.status}

    def set_state(self, state):
        self.name = state["name"]
        self.status = state["status"]
        if "id" in state:
            self.change_object_id(state["id"])


class BrokenStateModel(KeyValueStoreIntegrationMixin):
    def __init__(self):
        self.__post_init__()

    def get_state(self):
        raise NotImplementedError

    def set_state(self, state):
        raise NotImplementedError


class _AsyncList:
    """Minimal async iterable standing in for a ResultStream."""

    def __init__(self, items):
        self._items = items

    def __len__(self):
        return len(self._items)

    async def __aiter__(self):
        for item in self._items:
            yield item


def _reset(model):
    model._backend_store = None
    model._backend_config = None
    model._backend_initialized = False
    model._backend_lock = asyncio.Lock()


def _use_backend(model, backend):
    model._backend_store = backend
    model._backend_initialized = True


@pytest.fixture
def backend():
    backend = MagicMock()
    backend.connector.is_connected.return_value = True
    backend.resolve_physical_target.side_effect = lambda route, override=None: "_".join(
        route.components
    )
    backend.exists.return_value = True
    backend.filter.return_value = []
    backend.count.return_value = 0
    return backend


@pytest.fixture(autouse=True)
def reset_backend_store():
    for model in (ExampleModel, BrokenStateModel):
        _reset(model)
    yield
    for model in (ExampleModel, BrokenStateModel):
        _reset(model)


@pytest.fixture
def example_backend(backend):
    _use_backend(ExampleModel, backend)
    return backend


# ---------------------------------------------------------------------------
# backend_operation
# ---------------------------------------------------------------------------


class _DecoratedModel:
    def __init__(self):
        self.events = []

    async def save(self, force_insert=False, ttl=None):
        self.events.append(("save", force_insert, ttl))

    @backend_operation(auto_save=True, force_insert=True, ttl=5)
    def set_value(self, value):
        self.events.append(("set", value))
        return "done"

    @backend_operation(auto_save=False)
    def touch(self):
        return "ok"

    @backend_operation(auto_save=True)
    async def set_value_async(self, value):
        await asyncio.sleep(0)
        self.events.append(("set", value))
        return "async-done"


@pytest.mark.asyncio
async def test_backend_operation_with_auto_save():
    obj = _DecoratedModel()

    assert await obj.set_value("x") == "done"
    assert obj.events == [("set", "x"), ("save", True, 5)]


@pytest.mark.asyncio
async def test_backend_operation_without_auto_save():
    obj = _DecoratedModel()

    assert await obj.touch() == "ok"
    assert obj.events == []


@pytest.mark.asyncio
async def test_backend_operation_awaits_async_methods_before_saving():
    obj = _DecoratedModel()

    assert await obj.set_value_async("y") == "async-done"
    assert obj.events == [("set", "y"), ("save", False, None)]


# ---------------------------------------------------------------------------
# Backend initialisation
# ---------------------------------------------------------------------------


def _fake_backend_class(backend):
    class FakeBackendClass(KeyValueStoreBackendBase):
        def __new__(cls, **kwargs):
            backend.init_kwargs = kwargs
            return backend

    return FakeBackendClass


@pytest.mark.asyncio
async def test_initialize_backend_creates_backend_from_config(backend):
    config = {
        "ENGINE": "fake.backend.Class",
        "CONNECTOR_CONFIG": {"host": "localhost", "port": 11211},
    }

    with patch.object(ExampleModel, "get_backend_config", return_value=config), patch(
        "volnux.mixins.connection.import_string",
        return_value=_fake_backend_class(backend),
    ) as mock_import:
        await ExampleModel._initialize_backend()

    assert ExampleModel._backend_store is backend
    assert ExampleModel._backend_config == config
    assert ExampleModel._backend_initialized is True
    assert backend.init_kwargs == {"host": "localhost", "port": 11211}
    mock_import.assert_called_once_with("fake.backend.Class")
    backend.connector.connect.assert_not_called()


@pytest.mark.asyncio
async def test_initialize_backend_connects_if_connector_not_connected(backend):
    backend.connector.is_connected.return_value = False
    config = {"ENGINE": "fake.backend.Class", "CONNECTOR_CONFIG": {}}

    with patch.object(ExampleModel, "get_backend_config", return_value=config), patch(
        "volnux.mixins.connection.import_string",
        return_value=_fake_backend_class(backend),
    ):
        await ExampleModel._initialize_backend()

    backend.connector.connect.assert_called_once()


@pytest.mark.asyncio
async def test_initialize_backend_raises_when_engine_not_configured():
    with patch.object(
        ExampleModel, "get_backend_config", return_value={"CONNECTOR_CONFIG": {}}
    ):
        with pytest.raises(ImproperlyConfigured, match="Backend init failed"):
            await ExampleModel._initialize_backend()

    assert ExampleModel._backend_initialized is False


@pytest.mark.asyncio
async def test_initialize_backend_rejects_non_store_engine():
    with patch.object(
        ExampleModel, "get_backend_config", return_value={"ENGINE": "x.NotAStore"}
    ), patch("volnux.mixins.connection.import_string", return_value=dict):
        with pytest.raises(
            ImproperlyConfigured, match="not a KeyValueStoreBackendBase"
        ):
            await ExampleModel._initialize_backend()


@pytest.mark.asyncio
async def test_get_backend_initializes_once(backend):
    async def fake_init():
        _use_backend(ExampleModel, backend)

    with patch.object(
        ExampleModel, "_initialize_backend", AsyncMock(side_effect=fake_init)
    ) as mock_init:
        first = await ExampleModel.get_backend()
        second = await ExampleModel.get_backend()

    assert first is second is backend
    mock_init.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_schema_name_resolves_storage_route(example_backend):
    assert ExampleModel.get_storage_route().components == ["volnux", "ExampleModel"]
    assert await ExampleModel.get_schema_name() == SCHEMA


@pytest.mark.asyncio
async def test_change_backend_restores_previous_backend(example_backend):
    other = MagicMock()

    async with ExampleModel.change_backend(other):
        assert await ExampleModel.get_backend() is other

    assert await ExampleModel.get_backend() is example_backend


@pytest.mark.asyncio
async def test_change_backend_restores_on_error(example_backend):
    with pytest.raises(RuntimeError):
        async with ExampleModel.change_backend(MagicMock()):
            raise RuntimeError("boom")

    assert await ExampleModel.get_backend() is example_backend


@pytest.mark.asyncio
async def test_close_backend_prefers_backend_close(example_backend):
    ExampleModel._backend_config = {"x": 1}

    await ExampleModel.close_backend()

    example_backend.close.assert_called_once()
    example_backend.connector.disconnect.assert_not_called()
    assert ExampleModel._backend_store is None
    assert ExampleModel._backend_config is None
    assert ExampleModel._backend_initialized is False


@pytest.mark.asyncio
async def test_close_backend_falls_back_to_connector_disconnect(backend):
    del backend.close
    _use_backend(ExampleModel, backend)

    await ExampleModel.close_backend()

    backend.connector.disconnect.assert_called_once()
    assert ExampleModel._backend_store is None


@pytest.mark.asyncio
async def test_close_backend_is_noop_when_not_initialized():
    await ExampleModel.close_backend()

    assert ExampleModel._backend_store is None


# ---------------------------------------------------------------------------
# Identity and load state
# ---------------------------------------------------------------------------


def test_instances_get_unique_ids_by_default():
    first = BrokenStateModel()
    second = BrokenStateModel()

    assert first.id and second.id
    assert first.id != second.id


def test_change_object_id_rejects_blank_ids():
    obj = ExampleModel()

    with pytest.raises(ValueError, match="non-empty string"):
        obj.change_object_id("  ")


def test_mark_as_loaded_sets_flag():
    obj = ExampleModel()
    assert obj._is_loaded_from_backend() is False

    obj._mark_as_loaded()

    assert obj._is_loaded_from_backend() is True


def test_repr_reflects_load_state():
    obj = ExampleModel(id="123")
    assert repr(obj) == "<ExampleModel:123 [new]>"

    obj._mark_as_loaded()

    assert repr(obj) == "<ExampleModel:123 [exists]>"


# ---------------------------------------------------------------------------
# Instance operations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_force_insert_calls_backend_insert(example_backend):
    obj = ExampleModel(id="123")

    await obj.save(force_insert=True, ttl=60)

    example_backend.insert.assert_called_once_with(SCHEMA, "123", obj, ttl=60)
    example_backend.upsert.assert_not_called()
    assert obj._is_loaded_from_backend() is True


@pytest.mark.asyncio
async def test_save_uses_upsert_when_available(example_backend):
    obj = ExampleModel(id="123")

    await obj.save()

    example_backend.upsert.assert_called_once_with(SCHEMA, "123", obj)
    assert obj._is_loaded_from_backend() is True


@pytest.mark.asyncio
async def test_save_falls_back_to_insert_then_update_when_no_upsert(example_backend):
    del example_backend.upsert
    example_backend.insert.side_effect = ObjectExistError("exists")
    obj = ExampleModel(id="123")

    await obj.save()

    example_backend.insert.assert_called_once_with(SCHEMA, "123", obj, ttl=None)
    example_backend.update.assert_called_once_with(SCHEMA, "123", obj)
    assert obj._is_loaded_from_backend() is True


@pytest.mark.asyncio
async def test_save_force_insert_propagates_object_exist_error(example_backend):
    example_backend.insert.side_effect = ObjectExistError("exists")
    obj = ExampleModel(id="123")

    with pytest.raises(ObjectExistError):
        await obj.save(force_insert=True)

    assert obj._is_loaded_from_backend() is False


@pytest.mark.asyncio
async def test_update_calls_backend_update(example_backend):
    obj = ExampleModel(id="123")

    await obj.update()

    example_backend.update.assert_called_once_with(SCHEMA, "123", obj)


@pytest.mark.asyncio
async def test_delete_calls_backend_delete(example_backend):
    obj = ExampleModel(id="123")

    await obj.delete()

    example_backend.delete.assert_called_once_with(SCHEMA, "123")


@pytest.mark.asyncio
async def test_reload_calls_backend_reload_and_marks_loaded(example_backend):
    obj = ExampleModel(id="123")

    await obj.reload()

    example_backend.reload.assert_called_once_with(SCHEMA, obj)
    assert obj._is_loaded_from_backend() is True


@pytest.mark.asyncio
async def test_refresh_aliases_reload(example_backend):
    obj = ExampleModel(id="123")

    with patch.object(obj, "reload", AsyncMock()) as mock_reload:
        await obj.refresh()

    mock_reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_exists_returns_backend_result(example_backend):
    obj = ExampleModel(id="123")

    assert await obj.exists() is True
    example_backend.exists.assert_called_once_with(SCHEMA, "123")


@pytest.mark.asyncio
async def test_exists_returns_false_on_exception(example_backend):
    example_backend.exists.side_effect = RuntimeError("boom")

    assert await ExampleModel(id="123").exists() is False


# ---------------------------------------------------------------------------
# Class-level queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_loaded_instance(example_backend):
    loaded = ExampleModel(id="123")
    example_backend.get.return_value = loaded

    result = await ExampleModel.get("123")

    assert result is loaded
    example_backend.get.assert_called_once_with(SCHEMA, "123", ExampleModel)
    assert result._is_loaded_from_backend() is True


@pytest.mark.asyncio
async def test_get_propagates_missing_record(example_backend):
    example_backend.get.side_effect = ObjectDoesNotExist("missing")

    with pytest.raises(ObjectDoesNotExist):
        await ExampleModel.get("123")


@pytest.mark.asyncio
async def test_get_or_none_returns_none_when_missing(example_backend):
    example_backend.get.side_effect = ObjectDoesNotExist("missing")

    assert await ExampleModel.get_or_none("123") is None


@pytest.mark.asyncio
async def test_filter_returns_backend_stream(example_backend):
    stream = [ExampleModel(id="1"), ExampleModel(id="2")]
    example_backend.filter.return_value = stream

    result = await ExampleModel.filter(status="active")

    assert result is stream
    example_backend.filter.assert_called_once_with(
        SCHEMA, ExampleModel, status="active"
    )


@pytest.mark.asyncio
async def test_all_delegates_to_filter(example_backend):
    await ExampleModel.all()

    example_backend.filter.assert_called_once_with(SCHEMA, ExampleModel)


@pytest.mark.asyncio
async def test_count_calls_backend_count(example_backend):
    example_backend.count.return_value = 5

    assert await ExampleModel.count(status="active") == 5
    example_backend.count.assert_called_once_with(SCHEMA, status="active")


@pytest.mark.asyncio
async def test_exists_in_backend_calls_backend_exists(example_backend):
    assert await ExampleModel.exists_in_backend("123") is True
    example_backend.exists.assert_called_once_with(SCHEMA, "123")


@pytest.mark.asyncio
async def test_exists_in_backend_returns_false_on_exception(example_backend):
    example_backend.exists.side_effect = RuntimeError("boom")

    assert await ExampleModel.exists_in_backend("123") is False


@pytest.mark.asyncio
async def test_atomic_field_operations_delegate_to_backend(example_backend):
    example_backend.atomic_update_field.return_value = True
    example_backend.atomic_append_to_field.return_value = False

    assert await ExampleModel.atomic_update_field("1", "status", "x") is True
    assert await ExampleModel.atomic_append_to_field("1", "tags", "y") is False
    example_backend.atomic_update_field.assert_called_once_with(
        SCHEMA, "1", "status", "x"
    )
    example_backend.atomic_append_to_field.assert_called_once_with(
        SCHEMA, "1", "tags", "y"
    )


@pytest.mark.asyncio
async def test_atomic_field_operations_require_backend_support(example_backend):
    del example_backend.atomic_update_field

    with pytest.raises(NotImplementedError, match="atomic_update_field"):
        await ExampleModel.atomic_update_field("1", "status", "x")


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bulk_create_uses_bulk_insert_when_available(example_backend):
    instances = [ExampleModel(id="1"), ExampleModel(id="2")]

    await ExampleModel.bulk_create(instances)

    example_backend.bulk_insert.assert_called_once_with(
        SCHEMA, {"1": instances[0], "2": instances[1]}
    )
    assert all(instance._is_loaded_from_backend() for instance in instances)


@pytest.mark.asyncio
async def test_bulk_create_falls_back_to_save_force_insert(example_backend):
    del example_backend.bulk_insert
    one, two = ExampleModel(id="1"), ExampleModel(id="2")

    with patch.object(one, "save", AsyncMock()) as save_one, patch.object(
        two, "save", AsyncMock()
    ) as save_two:
        await ExampleModel.bulk_create([one, two])

    save_one.assert_awaited_once_with(force_insert=True)
    save_two.assert_awaited_once_with(force_insert=True)
    assert one._is_loaded_from_backend() and two._is_loaded_from_backend()


@pytest.mark.asyncio
async def test_bulk_delete_uses_backend_bulk_delete(example_backend):
    await ExampleModel.bulk_delete(["1", "2"])

    example_backend.bulk_delete.assert_called_once_with(SCHEMA, ["1", "2"])


@pytest.mark.asyncio
async def test_bulk_delete_falls_back_to_delete_one_by_one(example_backend):
    del example_backend.bulk_delete

    await ExampleModel.bulk_delete(["1", "2"])

    assert example_backend.delete.call_count == 2
    example_backend.delete.assert_any_call(SCHEMA, "1")
    example_backend.delete.assert_any_call(SCHEMA, "2")


@pytest.mark.asyncio
async def test_clear_all_uses_clear_schema_when_available(example_backend):
    await ExampleModel.clear_all()

    example_backend.clear_schema.assert_called_once_with(SCHEMA)


@pytest.mark.asyncio
async def test_clear_all_falls_back_to_all_then_bulk_delete(example_backend):
    del example_backend.clear_schema
    stream = _AsyncList([ExampleModel(id="1"), ExampleModel(id="2")])

    with patch.object(
        ExampleModel, "all", AsyncMock(return_value=stream)
    ), patch.object(ExampleModel, "bulk_delete", AsyncMock()) as mock_bulk_delete:
        await ExampleModel.clear_all()

    mock_bulk_delete.assert_awaited_once_with(["1", "2"])


# ---------------------------------------------------------------------------
# Atomic blocks and transactions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_atomic_saves_on_success(example_backend):
    obj = ExampleModel(id="1", name="Alice")

    with patch.object(obj, "save", AsyncMock()) as mock_save:
        async with obj.atomic():
            obj.name = "Bob"

    mock_save.assert_awaited_once()
    assert obj.name == "Bob"


@pytest.mark.asyncio
async def test_atomic_restores_original_state_on_failure(example_backend):
    obj = ExampleModel(id="1", name="Alice")

    with patch.object(obj, "save", AsyncMock()) as mock_save:
        with pytest.raises(RuntimeError, match="boom"):
            async with obj.atomic():
                obj.name = "Bob"
                raise RuntimeError("boom")

    mock_save.assert_not_awaited()
    assert obj.name == "Alice"
    assert obj.id == "1"


@pytest.mark.asyncio
async def test_atomic_restores_original_state_when_save_fails(example_backend):
    example_backend.upsert.side_effect = RuntimeError("write failed")
    obj = ExampleModel(id="1", name="Alice")

    with pytest.raises(RuntimeError, match="write failed"):
        async with obj.atomic():
            obj.name = "Bob"

    assert obj.name == "Alice"


@pytest.mark.asyncio
async def test_transaction_uses_connector_transaction_when_supported(example_backend):
    entered = []

    @contextmanager
    def fake_transaction():
        entered.append("enter")
        try:
            yield
        except RuntimeError:
            entered.append("rollback")
            raise
        finally:
            entered.append("exit")

    example_backend.connector.transaction = fake_transaction

    async with ExampleModel.transaction():
        entered.append("inside")

    assert entered == ["enter", "inside", "exit"]

    entered.clear()
    with pytest.raises(RuntimeError):
        async with ExampleModel.transaction():
            raise RuntimeError("boom")

    assert entered == ["enter", "rollback", "exit"]


@pytest.mark.asyncio
async def test_transaction_yields_without_connector_transaction(example_backend):
    del example_backend.connector.transaction

    async with ExampleModel.transaction():
        value = "inside"

    assert value == "inside"


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def test_getstate_returns_serializable_state(backend):
    _use_backend(ExampleModel, backend)
    obj = ExampleModel(id="1", name="Alice")

    state = obj.__getstate__()

    assert state["id"] == "1"
    assert state["name"] == "Alice"
    assert state["status"] == "active"
    assert "_backend_class" in state
    assert "_backend_store" not in state
    assert "_backend_config" not in state


def test_getstate_raises_serialization_error_when_get_state_not_implemented():
    with pytest.raises(SerializationError, match="Cannot serialise object"):
        BrokenStateModel().__getstate__()


def test_setstate_restores_state():
    obj = ExampleModel(id="1", name="Alice")

    obj.__setstate__(
        {"id": "2", "name": "Bob", "status": "inactive", "_backend_class": "x.Y"}
    )

    assert obj.id == "2"
    assert obj.name == "Bob"
    assert obj.status == "inactive"


def test_setstate_does_not_initialize_backend():
    obj = ExampleModel(id="1")

    with patch.object(ExampleModel, "_initialize_backend", AsyncMock()) as mock_init:
        obj.__setstate__({"id": "2", "name": "Bob", "status": "inactive"})

    # Initialisation is lazy (get_backend); an un-awaited call here would leak
    mock_init.assert_not_called()


def test_setstate_raises_serialization_error_when_set_state_not_implemented():
    with pytest.raises(SerializationError, match="Cannot deserialise object"):
        BrokenStateModel().__setstate__({"id": "1"})


def test_pickle_round_trip(backend):
    _use_backend(ExampleModel, backend)
    obj = ExampleModel(id="7", name="Carol", status="inactive")

    restored = pickle.loads(pickle.dumps(obj))

    assert restored.id == "7"
    assert restored.name == "Carol"
    assert restored.status == "inactive"
