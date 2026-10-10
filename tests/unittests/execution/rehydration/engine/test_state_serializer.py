"""Tests for rehydrator/engine/serializer — StateSerializer.

Covers:
  - serialize_task (normal task, group task, optional fields).
  - serialize_result (persisted via as_dict, lightweight via id).
  - serialize_queue_task (position, task_id, previous_context_id).
  - serialize_exception (type, message, traceback fields).
  - serialize_pipeline_ref (id and class_path tuple).
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from volnux.execution.rehydrator.engine.serializer import StateSerializer


# ===========================================================================
# Helpers
# ===========================================================================
def _make_mock_task(**overrides: Any) -> MagicMock:
    """Build a mock PipelineTask with sensible defaults."""
    task = MagicMock()

    mock_event_class = MagicMock()
    mock_event_class.__module__ = "myapp.events"
    mock_event_class.__name__ = "ProcessDataTask"
    task.get_event_class.return_value = mock_event_class

    task.get_id.return_value = "task-001"
    task.get_event_name.return_value = "process_data"

    # Optional fields
    task.sink_node = None
    task.sink_pipe = None
    task.sequence_number = 0
    task.descriptor = None
    task.descriptor_pipe = None
    task.is_grouping = False

    mock_options = MagicMock()
    mock_options.as_dict.return_value = {"batch_size": 100}
    task.options = mock_options

    mock_condition = MagicMock()
    mock_condition.as_dict.return_value = {
        "field": "status",
        "op": "eq",
        "value": "active",
    }
    task.condition_node = mock_condition

    for key, value in overrides.items():
        setattr(task, key, value)

    return task


# ===========================================================================
# serialize_task
# ===========================================================================
class TestSerializeTask:

    def test_normal_task(self) -> None:
        task = _make_mock_task()
        result = StateSerializer.serialize_task(task)

        assert result["task_id"] == "task-001"
        assert result["task_type"] == "normal"
        assert result["event_name"] == "process_data"
        assert result["event_class_import_path"] == "myapp.events.ProcessDataTask"
        assert result["sink_task_id"] is None
        assert result["sink_task_pipe"] is None
        assert result["options"] == {"batch_size": 100}
        assert result["sequence_number"] == 0
        assert result["descriptor"] is None
        assert result["descriptor_pipe_type"] is None
        assert result["condition_node"] == {
            "field": "status",
            "op": "eq",
            "value": "active",
        }

    def test_group_task(self) -> None:
        task = _make_mock_task(is_grouping=True)
        result = StateSerializer.serialize_task(task)
        assert result["task_type"] == "group"

    def test_task_with_sink(self) -> None:
        mock_sink = MagicMock()
        mock_sink.get_id.return_value = "sink-001"
        mock_pipe = MagicMock()
        mock_pipe.value = "PIPE_FILTER"

        task = _make_mock_task(sink_node=mock_sink, sink_pipe=mock_pipe)
        result = StateSerializer.serialize_task(task)

        assert result["sink_task_id"] == "sink-001"
        assert result["sink_task_pipe"] == "PIPE_FILTER"

    def test_task_with_descriptor(self) -> None:
        mock_pipe = MagicMock()
        mock_pipe.value = "PIPE_MAP"

        task = _make_mock_task(
            descriptor=42, descriptor_pipe=mock_pipe, sequence_number=5
        )
        result = StateSerializer.serialize_task(task)

        assert result["descriptor"] == 42
        assert result["descriptor_pipe_type"] == "PIPE_MAP"
        assert result["sequence_number"] == 5

    def test_get_id_called(self) -> None:
        task = _make_mock_task()
        StateSerializer.serialize_task(task)
        task.get_id.assert_called_once()


# ===========================================================================
# serialize_result
# ===========================================================================
class TestSerializeResult:

    def test_persisted_result_returns_dict(self) -> None:
        result = MagicMock()
        result.should_persist.return_value = True
        result.as_dict.return_value = {"data": [1, 2, 3], "count": 3}

        serialized = StateSerializer.serialize_result(result)
        assert serialized == {"data": [1, 2, 3], "count": 3}
        assert isinstance(serialized, dict)

    def test_lightweight_result_returns_id_string(self) -> None:
        result = MagicMock()
        result.should_persist.return_value = False
        result.id = "result-abc-123"

        serialized = StateSerializer.serialize_result(result)
        assert serialized == "result-abc-123"
        assert isinstance(serialized, str)

    def test_should_persist_checked(self) -> None:
        result = MagicMock()
        StateSerializer.serialize_result(result)
        result.should_persist.assert_called_once()


# ===========================================================================
# serialize_queue_task
# ===========================================================================
class TestSerializeQueueTask:

    def test_basic_queue_entry(self) -> None:
        task_node = MagicMock()
        task_node.task = MagicMock()
        task_node.task.get_id.return_value = "task-xyz"
        task_node.previous_context = MagicMock()
        task_node.previous_context.state_id = "prev-ctx-001"

        result = StateSerializer.serialize_queue_task(3, task_node)

        assert result["position_in_queue"] == 3
        assert result["task_id"] == "task-xyz"
        assert result["previous_context_id"] == "prev-ctx-001"

    def test_position_zero(self) -> None:
        task_node = MagicMock()
        task_node.task.get_id.return_value = "task-0"
        task_node.previous_context.state_id = "root-ctx"

        result = StateSerializer.serialize_queue_task(0, task_node)
        assert result["position_in_queue"] == 0


# ===========================================================================
# serialize_exception
# ===========================================================================
class TestSerializeException:

    def test_basic_exception(self) -> None:
        exc = ValueError("invalid input value")
        result = StateSerializer.serialize_exception(exc)

        assert result["type"] == "ValueError"
        assert result["message"] == "invalid input value"
        assert "traceback" in result
        assert isinstance(result["traceback"], str)

    def test_runtime_error(self) -> None:
        exc = RuntimeError("something broke")
        result = StateSerializer.serialize_exception(exc)

        assert result["type"] == "RuntimeError"
        assert "something broke" in result["message"]

    def test_custom_exception(self) -> None:
        class CustomError(Exception):
            pass

        exc = CustomError("custom fault")
        result = StateSerializer.serialize_exception(exc)

        assert result["type"] == "CustomError"
        assert result["message"] == "custom fault"

    def test_traceback_contains_exception_type(self) -> None:
        exc = TypeError("wrong type")
        result = StateSerializer.serialize_exception(exc)
        assert "TypeError" in result["traceback"]

    def test_exception_with_multiline_message(self) -> None:
        exc = ValueError("line1\nline2\nline3")
        result = StateSerializer.serialize_exception(exc)
        assert result["message"] == "line1\nline2\nline3"


# ===========================================================================
# serialize_pipeline_ref
# ===========================================================================
class TestSerializePipelineRef:

    def test_returns_id_and_class_path(self) -> None:
        pipeline = MagicMock()
        pipeline.__class__.__module__ = "myapp.pipelines"
        pipeline.__class__.__name__ = "DataProcessingPipeline"
        pipeline.id = "pipe-001"

        pid, class_path = StateSerializer.serialize_pipeline_ref(pipeline)

        assert pid == "pipe-001"
        assert class_path == "myapp.pipelines.DataProcessingPipeline"

    def test_nested_module_path(self) -> None:
        pipeline = MagicMock()
        pipeline.__class__.__module__ = "app.workflows.internal.pipelines"
        pipeline.__class__.__name__ = "ETLPipeline"
        pipeline.id = "pipe-etl-42"

        pid, class_path = StateSerializer.serialize_pipeline_ref(pipeline)

        assert pid == "pipe-etl-42"
        assert class_path == "app.workflows.internal.pipelines.ETLPipeline"

    def test_returns_tuple(self) -> None:
        pipeline = MagicMock()
        pipeline.__class__.__module__ = "app.pipe"
        pipeline.__class__.__name__ = "P"
        pipeline.id = "id-1"

        result = StateSerializer.serialize_pipeline_ref(pipeline)
        assert isinstance(result, tuple)
        assert len(result) == 2
