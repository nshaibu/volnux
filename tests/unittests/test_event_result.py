import collections
import dataclasses
import datetime
import decimal
import json
import os
import pickle

import pytest

from volnux.exceptions import MultiValueError, SerializationError
from volnux.result import EventResult, ResultSet
from volnux.result.result import EntityContentType


class Payload:
    """Non-builtin content implementing the state protocol EventResult uses."""

    def __init__(self, value):
        self.value = value

    def get_state(self):
        return {"value": self.value}

    def __setstate__(self, state):
        self.value = state["value"]


class DictPayload:
    def __init__(self, value):
        self.value = value

    def to_dict(self):
        return {"value": self.value}


class ErrorWithDict(Exception):
    def as_dict(self):
        return {"message": str(self)}


def _result(**overrides) -> EventResult:
    values = dict(
        error=False,
        event_name="test_event",
        content={"key": "value"},
        workflow_id="wf-1",
        task_id="123",
    )
    values.update(overrides)
    return EventResult(**values)


# ---------------------------------------------------------------------------
# EventResult
# ---------------------------------------------------------------------------


def test_event_result_initialization():
    event_result = _result()

    assert event_result.error is False
    assert event_result.event_name == "test_event"
    assert event_result.content == {"key": "value"}
    assert event_result.workflow_id == "wf-1"
    assert event_result.task_id == "123"
    assert event_result.order == 0
    assert event_result.is_persisted is False
    assert event_result.process_id == os.getpid()
    assert isinstance(event_result.creation_time, float)
    assert event_result.id


def test_event_result_optional_fields_default_to_none():
    event_result = EventResult(error=False, event_name="test_event", content=None)

    assert event_result.workflow_id is None
    assert event_result.task_id is None


def test_event_result_rejects_removed_fields():
    with pytest.raises(TypeError):
        _result(init_params={"execution_context": "test_context"})


def test_event_results_have_unique_ids():
    assert _result().id != _result().id


def test_event_result_success_mirrors_error():
    event_result = _result()
    assert event_result.success is True
    assert event_result.is_error() is False

    event_result.success = False

    assert event_result.error is True
    assert event_result.is_error() is True


def test_event_result_should_persist_must_be_overridden():
    with pytest.raises(NotImplementedError):
        _result().should_persist()


def test_event_result_hash_uses_id():
    event_result = _result()

    assert hash(event_result) == hash(event_result.id)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def test_event_result_get_state():
    event_result = _result()

    state = event_result.get_state()

    assert state["content"] == {"key": "value"}
    assert state["event_name"] == "test_event"
    assert state["workflow_id"] == "wf-1"
    assert "_objectid_lock" not in state


def test_event_result_getstate_includes_id():
    event_result = _result()

    assert event_result.__getstate__()["id"] == event_result.id


def test_event_result_set_state():
    event_result = _result(content=None)
    state = _result(error=True, event_name="updated_event").__getstate__()

    event_result.set_state(state)

    assert event_result.error is True
    assert event_result.event_name == "updated_event"
    assert event_result.content == {"key": "value"}
    assert event_result.id == state["id"]


def test_event_result_set_state_does_not_mutate_input():
    state = _result(content=Payload(1)).__getstate__()
    snapshot = dict(state)

    _result().set_state(state)

    assert state == snapshot


def test_event_result_pickle_round_trip_keeps_identity():
    event_result = _result(order=3)

    restored = pickle.loads(pickle.dumps(event_result))

    assert restored.id == event_result.id
    assert restored.event_name == "test_event"
    assert restored.content == {"key": "value"}
    assert restored.order == 3
    assert restored in ResultSet([event_result])


def test_event_result_custom_content_round_trip():
    event_result = _result(content=Payload(42))

    state = event_result.get_state()
    assert state["content"] == {
        "content_type_import_str": f"{Payload.__module__}.Payload",
        "state": {"value": 42},
    }

    restored = pickle.loads(pickle.dumps(event_result))

    assert isinstance(restored.content, Payload)
    assert restored.content.value == 42


@dataclasses.dataclass
class DataclassPayload:
    value: int


class PlainPayload:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, PlainPayload) and other.value == self.value


@pytest.mark.parametrize(
    "content",
    [
        PlainPayload(1),
        DataclassPayload(2),
        datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        decimal.Decimal("1.50"),
        collections.OrderedDict(a=1, b=2),
    ],
    ids=["plain-class", "dataclass", "datetime", "decimal", "ordereddict"],
)
def test_event_result_content_without_setstate_is_pickled(content):
    event_result = _result(content=content)

    stored = event_result.get_state()["content"]
    assert set(stored) == {"content_type_import_str", "pickle"}
    # base64 text keeps the state JSON-safe for persistent backends
    json.dumps(stored)

    restored = pickle.loads(pickle.dumps(event_result))

    assert restored.content == content
    assert type(restored.content) is type(content)


def test_event_result_unpicklable_content_raises_serialization_error():
    class LocalPayload:
        pass

    event_result = _result(content=LocalPayload())

    with pytest.raises(SerializationError, match="cannot be serialized"):
        event_result.get_state()


def test_event_result_restores_legacy_state_format():
    event_result = _result(content=None)
    state = _result().__getstate__()
    state["content"] = {
        "content_type_import_str": f"{Payload.__module__}.Payload",
        "state": {"value": 7},
    }

    event_result.set_state(state)

    assert isinstance(event_result.content, Payload)
    assert event_result.content.value == 7


def test_event_result_corrupt_content_raises_serialization_error():
    state = _result().__getstate__()
    state["content"] = {"content_type_import_str": "x.Y", "pickle": "not-base64!"}

    with pytest.raises(SerializationError, match="Failed to restore"):
        _result().set_state(state)


def test_event_result_builtin_content_is_stored_as_is():
    event_result = _result(content=[1, 2, 3])

    assert event_result.get_state()["content"] == [1, 2, 3]


# ---------------------------------------------------------------------------
# as_dict
# ---------------------------------------------------------------------------


def test_event_result_as_dict():
    result_dict = _result().as_dict()

    assert result_dict["error"] is False
    assert result_dict["event_name"] == "test_event"
    assert result_dict["content"] == {"key": "value"}
    assert result_dict["workflow_id"] == "wf-1"
    assert result_dict["task_id"] == "123"


def test_event_result_as_dict_keeps_exception_content():
    error = ValueError("bad")
    event_result = _result(error=True, content=error)

    result_dict = event_result.as_dict()

    assert result_dict["content"] is error
    # as_dict must not mutate the result
    assert event_result.content is error


def test_event_result_as_dict_serializes_exception_with_as_dict():
    event_result = _result(error=True, content=ErrorWithDict("bad"))

    assert event_result.as_dict()["content"] == {"message": "bad"}
    assert isinstance(event_result.content, ErrorWithDict)


def test_event_result_as_dict_leaves_non_exception_content_to_asdict():
    # Only exceptions are special-cased; other objects are deep-copied as-is
    payload = DictPayload(1)

    content = _result(content=payload).as_dict()["content"]

    assert isinstance(content, DictPayload)
    assert content is not payload


# ---------------------------------------------------------------------------
# EntityContentType
# ---------------------------------------------------------------------------


def test_entity_content_type_initialization():
    entity_content_type = EntityContentType(model_path="entity.module.Model")

    assert entity_content_type.model_path == "entity.module.Model"
    assert entity_content_type.backend_alias == "default"


def test_entity_content_type_equality():
    entity1 = EntityContentType(model_path="entity.Model", backend_alias="primary")
    entity2 = EntityContentType(model_path="entity.Model", backend_alias="primary")

    assert entity1 == entity2
    assert entity1 != EntityContentType(model_path="entity.Model")


def test_entity_content_type_from_event_result():
    content_type = EntityContentType.from_entity(_result())

    assert content_type.model_path == "volnux.result.result.EventResult"
    assert content_type.resolve_model() is EventResult


# ---------------------------------------------------------------------------
# ResultSet with EventResults
# ---------------------------------------------------------------------------


def test_result_set_initialization():
    event_result = _result()

    result_set = ResultSet([event_result])

    assert len(result_set) == 1
    assert event_result in result_set
    assert repr(result_set) == "<ResultSet: 1 items, 1 content types>"


def test_result_set_add():
    result_set = ResultSet([])

    result_set.add(_result())

    assert len(result_set) == 1


def test_result_set_discard_by_identity():
    event_result = _result()
    result_set = ResultSet([event_result])

    result_set.discard(pickle.loads(pickle.dumps(event_result)))

    assert len(result_set) == 0


def test_result_set_filter():
    result_set = ResultSet([_result(), _result(event_name="other")])

    filtered = result_set.filter(event_name="test_event")

    assert len(filtered) == 1
    assert filtered.first().event_name == "test_event"


def test_result_set_filter_by_content():
    result_set = ResultSet(
        [_result(content={"score": 10}), _result(content={"score": 90})]
    )

    assert len(result_set.filter(content__score__gte=50)) == 1


def test_result_set_get():
    result_set = ResultSet([_result(), _result(event_name="other")])

    assert result_set.get(event_name="other").event_name == "other"


def test_result_set_get_raises_key_error():
    result_set = ResultSet([_result()])

    with pytest.raises(KeyError):
        result_set.get(event_name="nonexistent")


def test_result_set_get_raises_multi_value_error():
    result_set = ResultSet([_result(), _result()])

    with pytest.raises(MultiValueError):
        result_set.get(event_name="test_event")
