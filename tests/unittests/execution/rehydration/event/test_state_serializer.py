"""Tests for volnux.execution.rehydrator.event.serializer — StateSerializer.

Covers:
  - serialize_init_args (with and without execution_context, all field paths).
  - serialize_call_args (args and kwargs).
  - serialize_exec_result delegation to ExecResultSerializer.
  - _serialize_stop_condition (None, single, list, nested list).
  - _build_keyword_args (nested dicts, lists, tuples, scalars).
  - serialize_attribs (exclusion of framework attrs, tracked keys, non-serializable warning).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from volnux.execution.rehydrator.event.serializer import (
    StateSerializer,
    is_json_serializable,
)
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.parser.options import StopCondition


# Helper: _is_json_serializable
class TestIsJsonSerializable:

    @pytest.mark.parametrize(
        "value",
        [
            42,
            3.14,
            "hello",
            True,
            False,
            None,
        ],
    )
    def test_primitives(self, value: Any) -> None:
        assert is_json_serializable(value) is True

    def test_list_of_primitives(self) -> None:
        assert is_json_serializable([1, "two", 3.0]) is True

    def test_tuple_of_primitives(self) -> None:
        assert is_json_serializable((1, 2)) is True

    def test_dict_with_string_keys(self) -> None:
        assert is_json_serializable({"a": 1, "b": [2, 3]}) is True

    def test_dict_with_non_string_keys(self) -> None:
        assert is_json_serializable({1: "a"}) is False

    def test_nested_valid_structure(self) -> None:
        assert is_json_serializable({"a": [1, {"b": True}]}) is True

    @pytest.mark.parametrize(
        "value",
        [
            b"bytes",
            set(),
            frozenset(),
            object(),
            Exception("err"),
        ],
    )
    def test_non_serializable_types(self, value: Any) -> None:
        assert is_json_serializable(value) is False

    def test_nested_list_with_non_serializable(self) -> None:
        assert is_json_serializable([1, 2, b"bytes"]) is False

    def test_empty_list(self) -> None:
        assert is_json_serializable([]) is True

    def test_empty_dict(self) -> None:
        assert is_json_serializable({}) is True


# _serialize_stop_condition
class TestSerializeStopCondition:

    def test_none_returns_never(self) -> None:
        result = StateSerializer._serialize_stop_condition(None)
        assert result == [StopCondition.NEVER.value]

    def test_single_condition(self) -> None:
        result = StateSerializer._serialize_stop_condition(StopCondition.NEVER)
        assert result == [StopCondition.NEVER.value]

    def test_list_of_conditions(self) -> None:
        result = StateSerializer._serialize_stop_condition(
            [
                StopCondition.NEVER,
                StopCondition.ON_ERROR,
            ]
        )
        assert result == [StopCondition.NEVER.value, StopCondition.ON_ERROR.value]

    def test_nested_list(self) -> None:
        result = StateSerializer._serialize_stop_condition(
            [
                StopCondition.ON_SUCCESS,
                [StopCondition.ON_ERROR, StopCondition.ON_EXCEPTION],
            ]
        )
        assert result == [
            StopCondition.ON_SUCCESS.value,
            StopCondition.ON_ERROR.value,
            StopCondition.ON_EXCEPTION.value,
        ]

    def test_tuple_same_as_list(self) -> None:
        result = StateSerializer._serialize_stop_condition((StopCondition.ON_ANY,))
        assert result == [StopCondition.ON_ANY.value]


class TestBuildKeywordArgs:

    def test_flat_dict(self) -> None:
        assert StateSerializer._build_keyword_args({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_nested_dict(self) -> None:
        input_data = {"outer": {"inner": 42}}
        assert StateSerializer._build_keyword_args(input_data) == {
            "outer": {"inner": 42}
        }

    def test_list_values(self) -> None:
        input_data = {"items": [{"a": 1}, {"b": 2}]}
        assert StateSerializer._build_keyword_args(input_data) == {
            "items": [{"a": 1}, {"b": 2}]
        }

    def test_tuple_values_preserved(self) -> None:
        input_data = {"coords": (10, 20)}
        result = StateSerializer._build_keyword_args(input_data)
        assert result["coords"] == (10, 20)
        assert isinstance(result["coords"], tuple)

    def test_scalar_passthrough(self) -> None:
        input_data = {"name": "test", "count": 5, "active": True}
        assert StateSerializer._build_keyword_args(input_data) == input_data

    def test_empty_dict(self) -> None:
        assert StateSerializer._build_keyword_args({}) == {}

    def test_deeply_nested(self) -> None:
        input_data = {"l1": {"l2": {"l3": "deep"}}}
        assert StateSerializer._build_keyword_args(input_data) == input_data


class TestSerializeInitArgs:

    @patch(
        "volnux.execution.rehydrator.event.serializer.StateSerializer._build_keyword_args"
    )
    @patch(
        "volnux.execution.rehydrator.event.serializer.StateSerializer._serialize_stop_condition"
    )
    def test_full_init_args(
        self,
        mock_stop_cond: MagicMock,
        mock_build_kwargs: MagicMock,
    ) -> None:
        mock_stop_cond.return_value = ["NEVER"]
        mock_build_kwargs.return_value = {"key": "val"}

        mock_execution_context = MagicMock()
        mock_execution_context.state_id = "ctx-001"

        mock_options = MagicMock()
        mock_options.as_dict.return_value = {"opt1": True}

        init_arg_dict = {
            "execution_context": mock_execution_context,
            "task_id": "task-001",
            "stop_condition": None,
            "run_bypass_event_checks": True,
            "options": mock_options,
            "sequence_number": 5,
            "kwargs": {"key": "val"},
            "previous_result": [SimpleNamespace(id="result-1")],
        }

        result = StateSerializer.serialize_init_args(init_arg_dict)

        assert result["execution_context_id"] == "ctx-001"
        assert result["task_id"] == "task-001"
        assert result["stop_condition"] == ["NEVER"]
        assert result["run_bypass_event_checks"] is True
        assert result["options"] == {"opt1": True}
        assert result["sequence_number"] == 5
        assert result["kwargs"] == {"key": "val"}
        # EventResult is persisted: the snapshot references it by id
        assert result["previous_result"] == ["result-1"]

    def test_minimal_init_args(self) -> None:
        result = StateSerializer.serialize_init_args({})
        assert result["execution_context_id"] is None
        assert result["task_id"] is None
        assert result["stop_condition"] == [StopCondition.NEVER.value]
        assert result["run_bypass_event_checks"] is False
        assert result["options"] is None
        assert result["sequence_number"] is None
        assert result["kwargs"] == {}
        assert result["previous_result"] == []


class TestSerializeCallArgs:

    @patch(
        "volnux.execution.rehydrator.event.serializer.StateSerializer.serialize_exec_result"
    )
    def test_with_args_and_kwargs(self, mock_serialize: MagicMock) -> None:
        mock_serialize.side_effect = lambda x: f"serialized_{x}"

        result = StateSerializer.serialize_call_args(
            {
                "args": [1, 2],
                "kwargs": {"key": "val"},
            }
        )

        assert result["args"] == ["serialized_1", "serialized_2"]
        assert result["kwargs"] == {"key": "serialized_val"}

    def test_empty_call_args(self) -> None:
        result = StateSerializer.serialize_call_args({})
        assert result["args"] == []
        assert result["kwargs"] == {}


class TestSerializeExecResult:

    @patch("volnux.execution.rehydrator.event.serializer.ExecResultSerializer")
    def test_delegates_to_exec_result_serializer(self, mock_cls: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_instance.serialize_exec_result.return_value = "serialized_result"
        mock_cls.return_value = mock_instance

        # Re-import to pick up the mock
        from volnux.execution.rehydrator.event.serializer import StateSerializer

        result = StateSerializer.serialize_exec_result("input")
        assert result == "serialized_result"


class TestSerializeAttribs:

    def _make_mock_event(self, attrs: Dict[str, Any]) -> MagicMock:
        """An event whose ``vars()`` are exactly ``attrs`` plus a class_path."""
        return SimpleNamespace(class_path="test.events.MyEvent", **attrs)

    def test_captures_user_attrs(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_page": 5,
                "_filter": "active",
                "_offset": 100,
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result == {"_page": 5, "_filter": "active", "_offset": 100}

    def test_excludes_framework_internal_attrs(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_pipeline": MagicMock(),  # not serializable, but excluded first
                "task_id": "task-001",
                "_page": 10,
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result == {"_page": 10}

    def test_excludes_tracked_keys(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {"task_id": "task-001"},
                "call_args": {"args": [], "kwargs": {}},
                "external_resources": {"db_cursor": {"data": {}}},
                "task_id": "override_value",
                "args": [1, 2],
                "kwargs": {"key": "val"},
                "db_cursor": "cursor_data",
                "_safe_attr": "preserved",
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result == {"_safe_attr": "preserved"}

    def test_warns_on_non_serializable(self, caplog: pytest.LogCaptureFixture) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_serializable": 42,
                "_non_serializable": MagicMock(),  # Mock is not JSON-safe
            }
        )
        with caplog.at_level(logging.WARNING):
            result = StateSerializer.serialize_attribs(event)
        assert result == {"_serializable": 42}
        assert any("Cannot checkpoint attribute" in r.message for r in caplog.records)
        assert any("_non_serializable" in r.message for r in caplog.records)

    def test_warns_includes_class_path(self, caplog: pytest.LogCaptureFixture) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "bad_attr": MagicMock(),
            }
        )
        event.class_path = "myapp.tasks.ProcessTask"

        with caplog.at_level(logging.WARNING):
            StateSerializer.serialize_attribs(event)

        assert any("myapp.tasks.ProcessTask" in r.message for r in caplog.records)

    def test_empty_attribs_when_all_excluded(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {"task_id": "t1"},
                "call_args": {},
                "external_resources": {},
                "_pipeline": MagicMock(),
                "task_id": "t1",
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result == {}

    def test_empty_event(self) -> None:
        event = self._make_mock_event({})
        result = StateSerializer.serialize_attribs(event)
        assert result == {}

    def test_nested_serializable_attr(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_config": {"nested": {"deep": [1, 2, 3]}},
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result["_config"] == {"nested": {"deep": [1, 2, 3]}}

    def test_list_attr(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_items": ["a", "b", "c"],
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result["_items"] == ["a", "b", "c"]

    def test_none_attr(self) -> None:
        event = self._make_mock_event(
            {
                "init_args": {},
                "call_args": {},
                "external_resources": {},
                "_optional": None,
            }
        )
        result = StateSerializer.serialize_attribs(event)
        assert result["_optional"] is None
