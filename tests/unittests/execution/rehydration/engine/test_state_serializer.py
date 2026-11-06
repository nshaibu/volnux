"""Tests for rehydrator/engine/serializer — StateSerializer.

Covers:
  - serialize_task (normal task, group task, optional fields).
  - serialize_queue_task (position, task_id, previous_context_id).

Context state (errors, results, pipeline reference) is serialized by the
ExecutionContext fields themselves, not by this class.
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
    task.namespace = "local"
    task.descriptors = {}

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
        mock_sink = _make_mock_task()
        mock_sink.get_id.return_value = "sink-001"
        mock_pipe = MagicMock()
        mock_pipe.value = "PIPE_FILTER"

        task = _make_mock_task(sink_node=mock_sink, sink_pipe=mock_pipe)
        result = StateSerializer.serialize_task(task)

        assert result["sink_task_id"] == "sink-001"
        assert result["sink_task"]["task_id"] == "sink-001"
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

    def test_namespace_and_descriptor_map(self) -> None:
        task = _make_mock_task(
            namespace="billing", descriptors={"out_of_balance": 2, "unsupported": 3}
        )
        result = StateSerializer.serialize_task(task)

        assert result["namespace"] == "billing"
        assert result["descriptors"] == {"out_of_balance": 2, "unsupported": 3}

    def test_descriptor_map_is_a_copy(self) -> None:
        task = _make_mock_task(descriptors={"a": 2})
        result = StateSerializer.serialize_task(task)
        result["descriptors"]["b"] = 3
        assert task.descriptors == {"a": 2}

    def test_task_without_options(self) -> None:
        task = _make_mock_task(options=None)
        assert StateSerializer.serialize_task(task)["options"] == {}

    def test_group_serializes_chains_and_strategy(self) -> None:
        chain = _make_mock_task()
        chain.get_id.return_value = "chain-1"
        strategy = MagicMock()
        strategy.name = "SINGLE_CHAIN"
        group = _make_mock_task(is_grouping=True, chains=[chain], strategy=strategy)

        result = StateSerializer.serialize_task(group)

        assert [c["task_id"] for c in result["chains"]] == ["chain-1"]
        assert result["strategy"] == "SINGLE_CHAIN"

    def test_plain_task_has_no_chains(self) -> None:
        result = StateSerializer.serialize_task(_make_mock_task())
        assert result["chains"] is None
        assert result["strategy"] is None
        assert result["sink_task"] is None

    def test_options_are_json_safe(self) -> None:
        from volnux.parser.options import Options

        task = _make_mock_task(
            options=Options.from_dict({"stop_condition": "on_error", "x": 1})
        )
        options = StateSerializer.serialize_task(task)["options"]

        assert options["stop_condition"] == "on_error"
        assert options["extras"] == {"x": 1}
        import json

        json.dumps(options)

    def test_group_has_no_event_class(self) -> None:
        task = _make_mock_task(is_grouping=True)
        result = StateSerializer.serialize_task(task)

        assert result["event_class_import_path"] is None
        task.get_event_class.assert_not_called()

    def test_real_task_and_group_types(self) -> None:
        from volnux import EventBase
        from volnux.task import PipelineTask, PipelineTaskGrouping

        class SerializedEvent(EventBase):
            def process(self, *args, **kwargs):
                return True, None

        normal = PipelineTask(event=SerializedEvent)
        group = PipelineTaskGrouping([normal])
        group.namespace = "ns"

        assert StateSerializer.serialize_task(normal)["task_type"] == "normal"
        result = StateSerializer.serialize_task(group)
        assert result["task_type"] == "group"
        assert result["namespace"] == "ns"
        assert result["event_class_import_path"] is None

    def test_get_id_called(self) -> None:
        task = _make_mock_task()
        StateSerializer.serialize_task(task)
        task.get_id.assert_called_once()


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

        result = StateSerializer.serialise_queue_task(3, task_node)

        assert result["position_in_queue"] == 3
        assert result["task_id"] == "task-xyz"
        assert result["previous_context_id"] == "prev-ctx-001"

    def test_position_zero(self) -> None:
        task_node = MagicMock()
        task_node.task.get_id.return_value = "task-0"
        task_node.previous_context.state_id = "root-ctx"

        result = StateSerializer.serialise_queue_task(0, task_node)
        assert result["position_in_queue"] == 0
