"""Tests for volnux.execution.checkpoint.event_result_serializer — ExecResultSerializer.

Covers:
  - Primitive serialization/deserialization (pass-through).
  - List, tuple, set round-trip preservation.
  - Dict serialization with key-type preservation (int, float, bool).
  - Dict key collision detection.
  - Unsupported dict key type raises TypeError.
  - Custom object serialization via __getstate__ and __dict__.
  - Custom object without __getstate__ or __dict__ raises TypeError.
  - Nested structures round-trip.
  - None handling.
  - Failures raise (no silent None replacement).
  - Module-level singleton _EXEC_RESULT_SERIALIZER.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Set

import pytest

from volnux.execution.rehydrator.event.event_result_serializer import (
    ExecResultSerializer,
    EXEC_RESULT_SERIALIZER,
)


@pytest.fixture()
def serializer() -> ExecResultSerializer:
    """Provide a fresh ExecResultSerializer for tests that need isolation."""
    return ExecResultSerializer()


# ===========================================================================
# Serialization — Primitives
# ===========================================================================
class TestSerializePrimitives:
    def test_int(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result(42) == 42

    def test_float(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result(3.14) == 3.14

    def test_str(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result("hello") == "hello"

    def test_bool(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result(True) is True
        assert serializer.serialize_exec_result(False) is False

    def test_none(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result(None) is None

    @pytest.mark.parametrize("value", [42, "hello", 3.14, True, None])
    def test_primitive_round_trip(
        self, serializer: ExecResultSerializer, value: Any
    ) -> None:
        assert (
            serializer.deserialize_exec_result(serializer.serialize_exec_result(value))
            == value
        )


# ===========================================================================
# Serialization — Sequences
# ===========================================================================
class TestSerializeSequences:
    def test_list(self, serializer: ExecResultSerializer) -> None:
        data = [1, "two", 3.0]
        result = serializer.serialize_exec_result(data)
        assert isinstance(result, list)
        assert result == [1, "two", 3.0]

    def test_list_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = [1, "two", [3, 4], {"key": "val"}]
        assert (
            serializer.deserialize_exec_result(serializer.serialize_exec_result(data))
            == data
        )

    def test_tuple_becomes_type_marker(self, serializer: ExecResultSerializer) -> None:
        data = (1, 2, 3)
        result = serializer.serialize_exec_result(data)
        assert result == {"__type__": "tuple", "items": [1, 2, 3]}

    def test_tuple_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = (1, "two", 3.0)
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data
        assert isinstance(restored, tuple)

    def test_set_becomes_type_marker(self, serializer: ExecResultSerializer) -> None:
        data = {1, 2, 3}
        result = serializer.serialize_exec_result(data)
        assert result["__type__"] == "set"
        assert set(result["items"]) == {1, 2, 3}

    def test_set_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = {1, "two", 3}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data
        assert isinstance(restored, set)

    def test_empty_list(self, serializer: ExecResultSerializer) -> None:
        assert serializer.serialize_exec_result([]) == []

    def test_empty_tuple_round_trip(self, serializer: ExecResultSerializer) -> None:
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(())
        )
        assert restored == ()
        assert isinstance(restored, tuple)

    def test_empty_set_round_trip(self, serializer: ExecResultSerializer) -> None:
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(set())
        )
        assert restored == set()
        assert isinstance(restored, set)


# ===========================================================================
# Serialization — Dicts (key-type preservation)
# ===========================================================================
class TestSerializeDicts:
    def test_string_keys_unchanged(self, serializer: ExecResultSerializer) -> None:
        data = {"a": 1, "b": 2}
        result = serializer.serialize_exec_result(data)
        # No __key_types__ marker needed for string keys
        assert "__key_types__" not in result
        assert result == {"a": 1, "b": 2}

    def test_string_key_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = {"a": 1, "b": [2, 3]}
        assert (
            serializer.deserialize_exec_result(serializer.serialize_exec_result(data))
            == data
        )

    def test_int_keys_preserved(self, serializer: ExecResultSerializer) -> None:
        data = {1: "one", 2: "two"}
        serialized = serializer.serialize_exec_result(data)
        assert "__key_types__" in serialized
        assert serialized["__key_types__"]["1"] == "int"
        assert serialized["__key_types__"]["2"] == "int"

    def test_int_key_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = {1: "one", 2: "two"}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data
        # Keys are ints, not strings
        assert 1 in restored
        assert 2 in restored

    def test_float_keys_preserved(self, serializer: ExecResultSerializer) -> None:
        data = {1.5: "a", 2.7: "b"}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert 1.5 in restored
        assert 2.7 in restored

    def test_bool_keys_preserved(self, serializer: ExecResultSerializer) -> None:
        data = {True: "yes", False: "no"}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert isinstance(restored[True], str)
        assert isinstance(restored[False], str)

    def test_mixed_key_types(self, serializer: ExecResultSerializer) -> None:
        data = {1: "int", "str": "string", 2.5: "float", True: "bool"}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored[1] == "int"
        assert restored["str"] == "string"
        assert restored[2.5] == "float"
        assert restored[True] == "bool"

    def test_unsupported_key_type_raises(
        self, serializer: ExecResultSerializer
    ) -> None:
        class BadKey:
            pass

        data = {BadKey(): "value"}
        with pytest.raises(TypeError, match="Cannot serialize dict key"):
            serializer.serialize_exec_result(data)

    def test_key_collision_raises(self, serializer: ExecResultSerializer) -> None:
        """int(1) and str("1") both serialize to "1" → collision."""
        data = {1: "int", "1": "string"}
        with pytest.raises(TypeError, match="key collision"):
            serializer.serialize_exec_result(data)

    def test_empty_dict_round_trip(self, serializer: ExecResultSerializer) -> None:
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result({})
        )
        assert restored == {}


# ===========================================================================
# Serialization — Custom Objects
# ===========================================================================
class TestSerializeCustomObjects:

    def test_object_with_getstate(self, serializer: ExecResultSerializer) -> None:
        class MyObj:
            def __init__(self, x: int) -> None:
                self.x = x

            def __getstate__(self) -> Dict[str, int]:
                return {"x": self.x}

        obj = MyObj(42)
        serialized = serializer.serialize_exec_result(obj)
        assert serialized["__type__"] == "custom_object"
        assert serialized["uses_getstate"] is True
        assert serialized["state"] == {"x": 42}
        assert "__class_path__" in serialized

    def test_object_with_dict_fallback(self, serializer: ExecResultSerializer) -> None:
        class MyObj:
            def __init__(self, name: str) -> None:
                self.name = name

        obj = MyObj("test")
        serialized = serializer.serialize_exec_result(obj)
        assert serialized["__type__"] == "custom_object"
        assert serialized["uses_getstate"] is False
        assert serialized["state"] == {"name": "test"}

    def test_object_without_getstate_or_dict_raises(
        self, serializer: ExecResultSerializer
    ) -> None:
        class SlotOnly:
            __slots__ = ()

        obj = SlotOnly()
        with pytest.raises(TypeError, match="no __getstate__ or __dict__"):
            serializer.serialize_exec_result(obj)

    def test_nested_object_round_trip(self, serializer: ExecResultSerializer) -> None:
        class Inner:
            def __init__(self, val: str) -> None:
                self.val = val

        class Outer:
            def __init__(self, inner: Inner) -> None:
                self.inner = inner

        obj = Outer(Inner("deep"))
        serialized = serializer.serialize_exec_result(obj)
        restored = serializer.deserialize_exec_result(serialized)
        assert isinstance(restored, Outer)
        assert isinstance(restored.inner, Inner)
        assert restored.inner.val == "deep"

    def test_object_round_trip_via_dict(self, serializer: ExecResultSerializer) -> None:
        class MyObj:
            def __init__(self, a: int, b: str) -> None:
                self.a = a
                self.b = b

        obj = MyObj(10, "hello")
        serialized = serializer.serialize_exec_result(obj)
        restored = serializer.deserialize_exec_result(serialized)
        assert restored.a == 10
        assert restored.b == "hello"


# ===========================================================================
# Serialization — Nested / Complex
# ===========================================================================
class TestSerializeNested:
    def test_deeply_nested_list(self, serializer: ExecResultSerializer) -> None:
        data = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data

    def test_dict_of_lists(self, serializer: ExecResultSerializer) -> None:
        data = {"ints": [1, 2, 3], "strings": ["a", "b"]}
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data

    def test_list_of_dicts(self, serializer: ExecResultSerializer) -> None:
        data = [{"id": 1}, {"id": 2}]
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == data

    def test_tuple_inside_dict(self, serializer: ExecResultSerializer) -> None:
        data = {"coords": (10, 20)}
        serialized = serializer.serialize_exec_result(data)
        restored = serializer.deserialize_exec_result(serialized)
        assert restored["coords"] == (10, 20)
        assert isinstance(restored["coords"], tuple)

    def test_set_inside_list(self, serializer: ExecResultSerializer) -> None:
        data = [{1, 2, 3}, {4, 5}]
        restored = serializer.deserialize_exec_result(
            serializer.serialize_exec_result(data)
        )
        assert restored == [{1, 2, 3}, {4, 5}]


# ===========================================================================
# Deserialization — Unknown / Edge Cases
# ===========================================================================
class TestDeserializeEdgeCases:

    def test_unknown_data_type_warns_and_returns_as_is(
        self, serializer: ExecResultSerializer, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Create a custom object that isn't a recognized serialized form
        class Custom:
            pass

        with caplog.at_level(logging.WARNING):
            result = serializer.deserialize_exec_result(Custom())
        assert "Unknown data type" in caplog.text
        assert isinstance(result, Custom)

    def test_list_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = [1, "two", 3.0, None]
        assert (
            serializer.deserialize_exec_result(serializer.serialize_exec_result(data))
            == data
        )

    def test_dict_round_trip(self, serializer: ExecResultSerializer) -> None:
        data = {"a": 1, "b": None, "c": [1, 2]}
        assert (
            serializer.deserialize_exec_result(serializer.serialize_exec_result(data))
            == data
        )


# ===========================================================================
# Module-level singleton
# ===========================================================================
class TestSingleton:
    def test_singleton_is_exec_result_serializer(self) -> None:
        assert isinstance(EXEC_RESULT_SERIALIZER, ExecResultSerializer)

    def test_singleton_is_same_instance(self) -> None:
        # Import twice — should be the same object
        from volnux.execution.rehydrator.event.event_result_serializer import (
            EXEC_RESULT_SERIALIZER as s2,
        )

        assert EXEC_RESULT_SERIALIZER is s2

    def test_singleton_functional(self) -> None:
        assert EXEC_RESULT_SERIALIZER.serialize_exec_result(42) == 42
        assert EXEC_RESULT_SERIALIZER.deserialize_exec_result(42) == 42
