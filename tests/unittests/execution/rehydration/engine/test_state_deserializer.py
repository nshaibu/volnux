"""Tests for rehydrator/engine/deserializer — StateDeserializer.

Covers:
  - deserialize_context (with traversal dict, without traversal, already
    ContextSnapshot).
  - deserialize_traversal (from dict).
  - deserialize_task (from dict).
  - deserialize_result (dict → from_dict, string ID → get_async, invalid type
    → ValueError, missing ID → ObjectDoesNotExist).
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.execution.rehydrator.engine.deserializer import StateDeserializer
from volnux.execution.rehydrator.engine.snapshot import TraversalSnapshot


# ===========================================================================
# Helpers
# ===========================================================================
def _make_traversal_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "engine_class_path": "volnux.engine.base.WorkflowEngine",
        "current_task": None,
        "current_task_checkpoint": None,
        "task_queue_snapshot": [],
        "queue_index": 0,
        "current_task_queue_size": 0,
        "current_sink_queue_size": 0,
        "sink_queue_snapshot": [],
        "tasks_processed": 0,
    }
    defaults.update(overrides)
    return defaults


def _make_context_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "state_id": "deser-001",
        "workflow_id": "wf-deser",
        "parent_id": None,
        "child_ids": [],
        "depth": 0,
        "previous_context_id": None,
        "next_context_id": None,
        "traversal": _make_traversal_data(),
        "pipeline_id": "pipe-deser",
        "pipeline_state": {},
        "pipeline_class_path": "app.Pipeline",
        "status": "RUNNING",
        "errors": [],
        "results": [],
        "metrics": {},
    }
    defaults.update(overrides)
    return defaults


def _make_task_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "context_id": "ctx-td",
        "task_id": "task-deser",
        "task_type": "normal",
        "task_checkpoint": None,
        "event_name": "process_data",
        "event_class_import_path": "myapp.events.ProcessDataTask",
        "sink_task_id": None,
        "sink_task_pipe": None,
        "options": {"size": 10},
        "condition_node": {},
        "sequence_number": 0,
        "descriptor": None,
        "descriptor_pipe_type": None,
    }
    defaults.update(overrides)
    return defaults


# ===========================================================================
# deserialize_traversal
# ===========================================================================
class TestDeserializeTraversal:

    def test_from_dict(self) -> None:
        data = _make_traversal_data(tasks_processed=5)
        traversal = StateDeserializer.deserialize_traversal(data)

        assert isinstance(traversal, TraversalSnapshot)
        assert traversal.tasks_processed == 5
        assert traversal.engine_class_path == "volnux.engine.base.WorkflowEngine"

    def test_with_current_task(self) -> None:
        data = _make_traversal_data(
            current_task={
                "task_id": "t-active",
                "previous_context_id": "ctx-a",
                "position_in_queue": 0,
            },
        )
        traversal = StateDeserializer.deserialize_traversal(data)
        assert traversal.current_task["task_id"] == "t-active"

    def test_with_queues(self) -> None:
        data = _make_traversal_data(
            task_queue_snapshot=[
                {"task_id": "t1", "previous_context_id": "c1", "position_in_queue": 0},
            ],
            sink_queue_snapshot=[
                {"task_id": "s1", "previous_context_id": "sc1", "position_in_queue": 0},
            ],
            current_task_queue_size=1,
            current_sink_queue_size=1,
        )
        traversal = StateDeserializer.deserialize_traversal(data)
        assert len(traversal.task_queue_snapshot) == 1
        assert len(traversal.sink_queue_snapshot) == 1


# ===========================================================================
# deserialize_context
# ===========================================================================
class TestDeserializeContext:

    def test_from_dict_with_traversal(self) -> None:
        data = _make_context_data()
        snapshot = StateDeserializer.deserialize_context(data)

        from rehydrator.engine.snapshot import ContextSnapshot

        assert isinstance(snapshot, ContextSnapshot)
        assert snapshot.state_id == "deser-001"
        assert isinstance(snapshot.traversal, TraversalSnapshot)

    def test_traversal_dict_rebuilt_into_dataclass(self) -> None:
        data = _make_context_data()
        data["traversal"]["tasks_processed"] = 42

        snapshot = StateDeserializer.deserialize_context(data)
        assert snapshot.traversal.tasks_processed == 42

    def test_does_not_mutate_input_dict(self) -> None:
        data = _make_context_data()
        original_status = data["status"]
        data["status"] = "MUTATED"

        StateDeserializer.deserialize_context(data)
        # The data dict should still have the value we set
        assert data["status"] == "MUTATED"

    def test_with_parent_and_children(self) -> None:
        data = _make_context_data(
            parent_id="parent-x",
            child_ids=["ch1", "ch2"],
            depth=3,
        )
        snapshot = StateDeserializer.deserialize_context(data)
        assert snapshot.parent_id == "parent-x"
        assert snapshot.child_ids == ["ch1", "ch2"]
        assert snapshot.depth == 3

    def test_with_errors(self) -> None:
        data = _make_context_data(
            errors=[
                {"type": "ValueError", "message": "oops"},
            ],
        )
        snapshot = StateDeserializer.deserialize_context(data)
        assert len(snapshot.errors) == 1
        assert snapshot.errors[0]["type"] == "ValueError"


# ===========================================================================
# deserialize_task
# ===========================================================================
class TestDeserializeTask:

    def test_from_dict(self) -> None:
        data = _make_task_data()
        snapshot = StateDeserializer.deserialize_task(data)

        from rehydrator.engine.snapshot import TaskSnapshot

        assert isinstance(snapshot, TaskSnapshot)
        assert snapshot.task_id == "task-deser"
        assert snapshot.event_name == "process_data"
        assert snapshot.task_type == "normal"

    def test_group_task(self) -> None:
        data = _make_task_data(task_type="group")
        snapshot = StateDeserializer.deserialize_task(data)
        assert snapshot.task_type == "group"

    def test_with_checkpoint(self) -> None:
        data = _make_task_data(task_checkpoint={"offset": 50})
        snapshot = StateDeserializer.deserialize_task(data)
        assert snapshot.task_checkpoint == {"offset": 50}


# ===========================================================================
# deserialize_result
# ===========================================================================
class TestDeserializeResult:

    @patch("volnux.result.EventResult")
    async def test_dict_result_calls_from_dict(
        self, mock_result_cls: MagicMock
    ) -> None:
        mock_instance = MagicMock()
        mock_result_cls.from_dict.return_value = mock_instance

        data = {"data": [1, 2, 3], "status": "done"}
        result = await StateDeserializer.deserialize_result(data)

        mock_result_cls.from_dict.assert_called_once_with(data)
        assert result is mock_instance

    @patch("volnux.result.EventResult")
    async def test_string_id_calls_get_async(self, mock_result_cls: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_result_cls.get_async = AsyncMock(return_value=mock_instance)

        result = await StateDeserializer.deserialize_result("result-id-xyz")

        mock_result_cls.get_async.assert_awaited_once_with("result-id-xyz")
        assert result is mock_instance

    @patch("volnux.result.EventResult")
    async def test_string_id_not_found_raises(self, mock_result_cls: MagicMock) -> None:
        from volnux.exceptions import ObjectDoesNotExist

        mock_result_cls.get_async = AsyncMock(
            side_effect=ObjectDoesNotExist("result-id-missing")
        )

        with pytest.raises(ObjectDoesNotExist):
            await StateDeserializer.deserialize_result("result-id-missing")

    @patch("volnux.result.EventResult")
    async def test_string_id_not_found_logs_warning(
        self, mock_result_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        from volnux.exceptions import ObjectDoesNotExist

        mock_result_cls.get_async = AsyncMock(
            side_effect=ObjectDoesNotExist("result-id-missing")
        )

        with pytest.raises(ObjectDoesNotExist), caplog.at_level("WARNING"):
            await StateDeserializer.deserialize_result("result-id-missing")

        assert any("result-id-missing" in r.message for r in caplog.records)

    async def test_invalid_type_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot deserialize result"):
            await StateDeserializer.deserialize_result(42)

    async def test_invalid_type_none_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot deserialize result"):
            await StateDeserializer.deserialize_result(None)

    async def test_invalid_type_list_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot deserialize result"):
            await StateDeserializer.deserialize_result([1, 2, 3])

    @patch("volnux.result.EventResult")
    async def test_empty_dict_result(self, mock_result_cls: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_result_cls.from_dict.return_value = mock_instance

        result = await StateDeserializer.deserialize_result({})
        mock_result_cls.from_dict.assert_called_once_with({})
        assert result is mock_instance
