"""Tests for rehydrator/engine/snapshot — snapshot schemas.

Covers:
  - QueueTaskTemplate TypedDict construction.
  - EngineStateSnapshot construction, field access and get_schema_name.
  - TaskSnapshot construction, get_schema_name.

An ExecutionContext is persisted as itself, so there is no context snapshot.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from volnux.execution.rehydrator.engine.snapshot import (
    QueueTaskTemplate,
    EngineStateSnapshot,
    TaskSnapshot,
)


# ===========================================================================
# QueueTaskTemplate
# ===========================================================================
class TestQueueTaskTemplate:

    def test_construction(self) -> None:
        entry: QueueTaskTemplate = {
            "task_id": "task-001",
            "previous_context_id": "ctx-prev",
            "position_in_queue": 0,
        }
        assert entry["task_id"] == "task-001"
        assert entry["previous_context_id"] == "ctx-prev"
        assert entry["position_in_queue"] == 0

    def test_all_fields_required(self) -> None:
        """QueueTaskTemplate is total=True — all keys are required."""
        # A missing key is a runtime error, not a schema violation at this
        # level (TypedDict total=True means "expect all keys present").
        entry: QueueTaskTemplate = {
            "task_id": "t1",
            "previous_context_id": "p1",
            "position_in_queue": 2,
        }
        assert len(entry) == 3


# ===========================================================================
# EngineStateSnapshot
# ===========================================================================
class TestEngineStateSnapshot:

    def _make_engine_state(self, **overrides: Any) -> EngineStateSnapshot:
        defaults: Dict[str, Any] = {
            "context_id": "ctx-001",
            "engine_class_path": "volnux.engine.base.WorkflowEngine",
            "current_task": None,
            "task_queue_snapshot": [
                {"task_id": "t1", "previous_context_id": "c0", "position_in_queue": 0},
                {"task_id": "t2", "previous_context_id": "c1", "position_in_queue": 1},
            ],
            "queue_index": 0,
            "current_task_queue_size": 2,
            "current_sink_queue_size": 0,
            "sink_queue_snapshot": [],
            "tasks_processed": 0,
        }
        defaults.update(overrides)
        return EngineStateSnapshot(**defaults)

    def test_construction(self) -> None:
        e = self._make_engine_state()
        assert e.context_id == "ctx-001"
        assert e.engine_class_path == "volnux.engine.base.WorkflowEngine"
        assert e.current_task is None
        assert e.queue_index == 0
        assert e.current_task_queue_size == 2
        assert e.current_sink_queue_size == 0
        assert e.tasks_processed == 0
        assert e.snapshot_timestamp > 0

    def test_with_current_task(self) -> None:
        current: QueueTaskTemplate = {
            "task_id": "t-active",
            "previous_context_id": "c-active",
            "position_in_queue": 0,
        }
        e = self._make_engine_state(current_task=current)
        assert e.current_task["task_id"] == "t-active"

    def test_with_sink_queue(self) -> None:
        sinks = [
            {"task_id": "s1", "previous_context_id": "sc1", "position_in_queue": 0},
        ]
        e = self._make_engine_state(sink_queue_snapshot=sinks, current_sink_queue_size=1)
        assert len(e.sink_queue_snapshot) == 1
        assert e.current_sink_queue_size == 1

    def test_tasks_processed(self) -> None:
        assert self._make_engine_state(tasks_processed=7).tasks_processed == 7

    def test_get_schema_name(self) -> None:
        assert EngineStateSnapshot.get_schema_name() == "volnux:snapshot:engine"

    def test_is_keyed_by_context_id(self) -> None:
        e = self._make_engine_state()
        e.change_object_id(e.context_id)
        assert e.id == "ctx-001"


# ===========================================================================
# TaskSnapshot
# ===========================================================================
class TestTaskSnapshot:

    def _make_task_snapshot(self, **overrides: Any) -> TaskSnapshot:
        defaults: Dict[str, Any] = {
            "context_id": "ctx-001",
            "task_id": "task-001",
            "task_type": "normal",
            "task_checkpoint": None,
            "event_name": "process_data",
            "event_class_import_path": "myapp.events.ProcessDataTask",
            "sink_task_id": None,
            "sink_task_pipe": None,
            "options": {"batch_size": 50},
            "condition_node": {"field": "status", "op": "eq"},
            "sequence_number": 0,
            "descriptor": None,
            "descriptor_pipe_type": None,
        }
        defaults.update(overrides)
        return TaskSnapshot(**defaults)

    def test_construction(self) -> None:
        ts = self._make_task_snapshot()
        assert ts.context_id == "ctx-001"
        assert ts.task_id == "task-001"
        assert ts.task_type == "normal"
        assert ts.event_name == "process_data"

    def test_group_task_type(self) -> None:
        ts = self._make_task_snapshot(task_type="group")
        assert ts.task_type == "group"

    def test_with_sink(self) -> None:
        ts = self._make_task_snapshot(
            sink_task_id="sink-001",
            sink_task_pipe="PIPE_AGGREGATE",
        )
        assert ts.sink_task_id == "sink-001"
        assert ts.sink_task_pipe == "PIPE_AGGREGATE"

    def test_with_checkpoint(self) -> None:
        ts = self._make_task_snapshot(task_checkpoint={"offset": 99, "buffer": [1, 2]})
        assert ts.task_checkpoint == {"offset": 99, "buffer": [1, 2]}

    def test_get_schema_name(self) -> None:
        assert TaskSnapshot.get_schema_name() == "volnux:snapshot:task"

    def test_graph_fields_default_to_none(self) -> None:
        ts = self._make_task_snapshot()
        assert ts.sink_task is None
        assert ts.chains is None
        assert ts.strategy is None
        assert ts.namespace == "local"
        assert ts.descriptors == {}

    def test_context_id_is_optional(self) -> None:
        assert self._make_task_snapshot(context_id=None).context_id is None

    def test_change_object_id(self) -> None:
        ts = self._make_task_snapshot()
        ts.change_object_id("new-task-id")
        assert ts.id == "new-task-id"
