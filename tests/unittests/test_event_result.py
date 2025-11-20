from datetime import datetime

import pytest

from volnux.exceptions import MultiValueError
from volnux.result import EntityContentType, EventResult, ResultSet


def test_event_result_initialization():
    event_result = EventResult(
        error=False,
        event_name="test_event",
        content={"key": "value"},
        task_id="123",
        init_params={"execution_context": "test_context"},
        call_params={"param1": "value1"},
    )
    assert event_result.error is False
    assert event_result.event_name == "test_event"
    assert event_result.content == {"key": "value"}
    assert event_result.task_id == "123"
    assert event_result.init_params == {"execution_context": "test_context"}
    assert event_result.call_params == {"param1": "value1"}
    assert isinstance(event_result.process_id, int)
    assert isinstance(event_result.creation_time, float)


def test_event_result_get_state():
    event_result = EventResult(
        error=False,
        event_name="test_event",
        content={"key": "value"},
        init_params={"execution_context": "test_context"},
    )
    state = event_result.get_state()
    assert "content" in state
    assert "init_params" in state
    assert state["init_params"]["execution_context"] == "test_context"


def test_event_result_set_state():
    event_result = EventResult(
        error=False,
        event_name="test_event",
        content=None,
    )
    state = {
        "error": True,
        "event_name": "updated_event",
        "content": {"key": "value"},
    }
    event_result.set_state(state)
    assert event_result.error is True
    assert event_result.event_name == "updated_event"
    assert event_result.content == {"key": "value"}


def test_event_result_is_error():
    event_result = EventResult(
        error=True,
        event_name="test_event",
        content=None,
    )
    assert event_result.is_error() is True


def test_event_result_as_dict():
    event_result = EventResult(
        error=False,
        event_name="test_event",
        content={"key": "value"},
    )
    result_dict = event_result.as_dict()
    assert result_dict["error"] is False
    assert result_dict["event_name"] == "test_event"
    assert result_dict["content"] == {"key": "value"}


def test_entity_content_type_initialization():
    entity_content_type = EntityContentType(
        backend_import_str="backend.module",
        entity_content_type="entity.module",
    )
    assert entity_content_type.backend_import_str == "backend.module"
    assert entity_content_type.entity_content_type == "entity.module"


def test_entity_content_type_equality():
    entity1 = EntityContentType(
        backend_import_str="backend.module",
        entity_content_type="entity.module",
    )
    entity2 = EntityContentType(
        backend_import_str="backend.module",
        entity_content_type="entity.module",
    )
    assert entity1 == entity2


def test_result_set_initialization():
    class MockResult:
        id = "1"

        @property
        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    mock_result = MockResult()

    result_set = ResultSet([mock_result])
    assert len(result_set) == 1
    assert mock_result in result_set


def test_result_set_add():
    class MockResult:
        id = "1"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([])
    result_set.add(MockResult())
    assert len(result_set) == 1


def test_result_set_discard():
    class MockResult:
        id = "1"

        def __hash__(self):
            return 1

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([MockResult()])
    result_set.discard(MockResult())
    assert len(result_set) == 0


def test_result_set_filter():
    class MockResult:
        id = "1"
        name = "test"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([MockResult()])
    filtered = result_set.filter(name="test")
    assert len(filtered) == 1
    assert filtered.first().name == "test"


def test_result_set_get():
    class MockResult:
        id = "1"
        name = "test"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([MockResult()])
    result = result_set.get(name="test")
    assert result.name == "test"


def test_result_set_get_raises_key_error():
    class MockResult:
        id = "1"
        name = "test"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([MockResult()])
    with pytest.raises(KeyError):
        result_set.get(name="nonexistent")


def test_result_set_get_raises_multi_value_error():
    class MockResult:
        id = "1"
        name = "test"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    class MockResult2:
        id = "2"
        name = "test"

        def __object_import_str__(self):
            return f"{self.__class__.__module__}.{self.__class__.__qualname__}"

    result_set = ResultSet([MockResult(), MockResult2()])
    with pytest.raises(MultiValueError):
        result_set.get(name="test")
