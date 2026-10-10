"""Tests for volnux.execution.checkpoint.deserializer — StateDeserializer.

Covers:
  - deserialize_init_args (with and without execution_context, all field paths).
  - deserialize_call_args (args and kwargs).
  - deserialize_exec_result delegation.
  - deserialize_external_resources (with/without provider_path, import errors).
  - _deserialize_stop_condition (valid, invalid, empty list, None returns NEVER fallback).
  - _restore_keyword_args (recursive passthrough).
"""

from __future__ import annotations

import logging
from typing import Any, Dict
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from volnux.execution.rehydrator.event.deserializer import StateDeserializer
from volnux.execution.rehydrator.event.snapshot import (
    InitArgsTemplate,
    CallArgsTemplate,
    ResourceState,
)
from volnux.parser.options import StopCondition


# _deserialize_stop_condition
class TestDeserializeStopCondition:

    def test_single_condition(self) -> None:
        result = StateDeserializer._deserialize_stop_condition(["NEVER"])
        assert len(result) == 1
        assert result[0] == StopCondition.NEVER

    def test_multiple_conditions(self) -> None:
        result = StateDeserializer._deserialize_stop_condition(
            [
                "MAX_RETRIES_EXCEEDED",
                "EXTERNAL_SIGNAL",
            ]
        )
        assert len(result) == 2
        assert result[0] == StopCondition.MAX_RETRIES_EXCEEDED
        assert result[1] == StopCondition.EXTERNAL_SIGNAL

    def test_unknown_condition_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            result = StateDeserializer._deserialize_stop_condition(
                ["NEVER", "INVALID_CONDITION"]
            )
        assert len(result) == 1  # only NEVER deserialized
        assert any("Unknown stop condition" in r.message for r in caplog.records)
        assert "INVALID_CONDITION" in caplog.text

    def test_empty_list_returns_never(self) -> None:
        result = StateDeserializer._deserialize_stop_condition([])
        assert result == [StopCondition.NEVER]


# _restore_keyword_args
class TestRestoreKeywordArgs:

    def test_passthrough(self) -> None:
        data = {"a": 1, "b": {"nested": True}, "c": [1, 2]}
        result = StateDeserializer._restore_keyword_args(data)
        assert result == data

    def test_empty(self) -> None:
        assert StateDeserializer._restore_keyword_args({}) == {}

    def test_preserves_tuple_values(self) -> None:
        data = {"coords": (10, 20)}
        result = StateDeserializer._restore_keyword_args(data)
        assert result["coords"] == (10, 20)
        assert isinstance(result["coords"], tuple)


# deserialize_init_args
class TestDeserializeInitArgs:

    @patch(
        "volnux.execution.checkpoint.deserializer.StateDeserializer._restore_keyword_args"
    )
    @patch(
        "volnux.execution.checkpoint.deserializer.StateDeserializer._deserialize_stop_condition"
    )
    @patch("volnux.execution.checkpoint.deserializer.EngineSerializer")
    def test_full_init_args(
        self,
        mock_engine_cls: MagicMock,
        mock_stop_cond: MagicMock,
        mock_restore_kwargs: MagicMock,
    ) -> None:
        mock_stop_cond.return_value = [StopCondition.ON_ERROR]
        mock_restore_kwargs.return_value = {"key": "val"}
        mock_engine_cls.deserialize_result.return_value = {"deserialized": True}

        mock_ctx_class = MagicMock()
        mock_ctx_class.get.return_value = MagicMock(spec=["state_id"])
        mock_ctx_class.get.return_value.state_id = "ctx-001"

        init_args: InitArgsTemplate = {
            "execution_context_id": "ctx-001",
            "task_id": "task-001",
            "stop_condition": ["MAX_RETRIES_EXCEEDED"],
            "run_bypass_event_checks": True,
            "options": {"opt1": True},
            "sequence_number": 5,
            "kwargs": {"key": "val"},
            "previous_result": [{"result": "data"}],
        }

        with patch(
            "volnux.execution.checkpoint.deserializer.ExecutionContext",
            mock_ctx_class,
        ):
            result = StateDeserializer.deserialize_init_args(init_args)

        assert result["task_id"] == "task-001"
        assert result["run_bypass_event_checks"] is True
        assert result["sequence_number"] == 5
        assert result["kwargs"] == {"key": "val"}
        assert result["previous_result"] == [{"deserialized": True}]

    def test_minimal_init_args(self) -> None:
        init_args: InitArgsTemplate = {}
        result = StateDeserializer.deserialize_init_args(init_args)
        assert result["run_bypass_event_checks"] is False
        assert result["kwargs"] == {}

    def test_none_execution_context_id_skipped(self) -> None:
        init_args: InitArgsTemplate = {"execution_context_id": None}
        with patch(
            "volnux.execution.checkpoint.deserializer.ExecutionContext"
        ) as mock_ctx:
            result = StateDeserializer.deserialize_init_args(init_args)
            mock_ctx.get.assert_not_called()


# deserialize_call_args
class TestDeserializeCallArgs:

    @patch("volnux.execution.checkpoint.deserializer.EngineSerializer")
    def test_with_args_and_kwargs(self, mock_engine_cls: MagicMock) -> None:
        mock_engine_cls.deserialize_result.side_effect = lambda x: x

        call_args: CallArgsTemplate = {
            "args": ["arg1", "arg2"],
            "kwargs": {"key": "val"},
        }

        result = StateDeserializer.deserialize_call_args(call_args)
        assert result["args"] == ["arg1", "arg2"]
        assert result["kwargs"] == {"key": "val"}

    def test_empty_call_args(self) -> None:
        result = StateDeserializer.deserialize_call_args({})
        assert result["args"] == []
        assert result["kwargs"] == {}


# deserialize_exec_result
class TestDeserializeExecResult:

    @patch("volnux.execution.checkpoint.deserializer.ExecResultSerializer")
    def test_delegates_to_singleton(self, mock_cls: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_instance.deserialize_exec_result.return_value = "restored"
        mock_cls.return_value = mock_instance

        result = StateDeserializer.deserialize_exec_result("input")
        assert result == "restored"


# deserialize_external_resources
class TestDeserializeExternalResources:

    def test_valid_resource_restored(self) -> None:
        mock_provider = MagicMock()
        mock_provider.restore.return_value = "restored_cursor"

        resources: Dict[str, ResourceState] = {
            "db_cursor": {
                "resource_name": "db_cursor",
                "data": {"offset": 100},
                "provider_path": "myapp.providers.CursorProvider",
            }
        }

        with patch(
            "volnux.execution.checkpoint.deserializer.import_class",
            return_value=mock_provider,
        ):
            result = StateDeserializer.deserialize_external_resources(resources)

        assert result["db_cursor"] == "restored_cursor"
        mock_provider.restore.assert_called_once_with({"offset": 100})

    def test_missing_provider_path_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        resources: Dict[str, ResourceState] = {
            "bad_resource": {
                "resource_name": "bad_resource",
                "data": {},
                "provider_path": None,
            }
        }

        with caplog.at_level(logging.WARNING):
            result = StateDeserializer.deserialize_external_resources(resources)

        assert result == {}
        assert any("no provider_path" in r.message for r in caplog.records)

    def test_import_error_raises(self) -> None:
        resources: Dict[str, ResourceState] = {
            "broken": {
                "resource_name": "broken",
                "data": {},
                "provider_path": "nonexistent.module.Provider",
            }
        }

        with patch(
            "volnux.execution.checkpoint.deserializer.import_class",
            side_effect=ImportError("Module not found"),
        ), pytest.raises(ImportError, match="Module not found"):
            StateDeserializer.deserialize_external_resources(resources)

    def test_restore_error_raises(self) -> None:
        mock_provider = MagicMock()
        mock_provider.restore.side_effect = RuntimeError("Connection refused")

        resources: Dict[str, ResourceState] = {
            "db": {
                "resource_name": "db",
                "data": {},
                "provider_path": "myapp.providers.DBProvider",
            }
        }

        with patch(
            "volnux.execution.checkpoint.deserializer.import_class",
            return_value=mock_provider,
        ), pytest.raises(RuntimeError, match="Connection refused"):
            StateDeserializer.deserialize_external_resources(resources)

    def test_multiple_resources(self) -> None:
        provider_a = MagicMock()
        provider_a.restore.return_value = "cursor_a"
        provider_b = MagicMock()
        provider_b.restore.return_value = "cursor_b"

        resources: Dict[str, ResourceState] = {
            "cursor_a": {
                "resource_name": "cursor_a",
                "data": {"offset": 0},
                "provider_path": "providers.A",
            },
            "cursor_b": {
                "resource_name": "cursor_b",
                "data": {"offset": 50},
                "provider_path": "providers.B",
            },
        }

        with patch(
            "volnux.execution.checkpoint.deserializer.import_class",
            side_effect=[provider_a, provider_b],
        ):
            result = StateDeserializer.deserialize_external_resources(resources)

        assert len(result) == 2
        assert result["cursor_a"] == "cursor_a"
        assert result["cursor_b"] == "cursor_b"

    def test_empty_resources(self) -> None:
        assert StateDeserializer.deserialize_external_resources({}) == {}
