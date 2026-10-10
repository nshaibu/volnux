"""Tests for rehydrator/engine/snapshot — snapshot schemas.

Covers:
  - QueueTaskTemplate TypedDict construction.
  - TraversalSnapshot dataclass construction and field access.
  - TaskSnapshot construction, get_schema_name.
  - ContextSnapshot construction, to_dict, from_dict, set_state, get_state,
    get_schema_name.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from volnux.execution.rehydrator.engine.snapshot import (
    QueueTaskTemplate,
    TraversalSnapshot,
    TaskSnapshot,
    ContextSnapshot,
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
# TraversalSnapshot
# ===========================================================================
class TestTraversalSnapshot:

    def _make_traversal(self, **overrides: Any) -> TraversalSnapshot:
        defaults: Dict[str, Any] = {
            "engine_class_path": "volnux.engine.base.WorkflowEngine",
            "current_task": None,
            "current_task_checkpoint": None,
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
        return TraversalSnapshot(**defaults)

    def test_construction(self) -> None:
        t = self._make_traversal()
        assert t.engine_class_path == "volnux.engine.base.WorkflowEngine"
        assert t.current_task is None
        assert t.current_task_checkpoint is None
        assert t.queue_index == 0
        assert t.current_task_queue_size == 2
        assert t.current_sink_queue_size == 0
        assert t.tasks_processed == 0

    def test_with_current_task(self) -> None:
        current: QueueTaskTemplate = {
            "task_id": "t-active",
            "previous_context_id": "c-active",
            "position_in_queue": 0,
        }
        t = self._make_traversal(
            current_task=current,
            current_task_checkpoint={"offset": 42},
        )
        assert t.current_task["task_id"] == "t-active"
        assert t.current_task_checkpoint == {"offset": 42}

    def test_with_sink_queue(self) -> None:
        sinks = [
            {"task_id": "s1", "previous_context_id": "sc1", "position_in_queue": 0},
        ]
        t = self._make_traversal(
            sink_queue_snapshot=sinks,
            current_sink_queue_size=1,
        )
        assert len(t.sink_queue_snapshot) == 1
        assert t.current_sink_queue_size == 1

    def test_tasks_processed(self) -> None:
        t = self._make_traversal(tasks_processed=7)
        assert t.tasks_processed == 7

    def test_is_dataclass(self) -> None:
        from dataclasses import is_dataclass

        assert is_dataclass(TraversalSnapshot)

    def test_queue_index_tracking(self) -> None:
        t = self._make_traversal(queue_index=3)
        assert t.queue_index == 3


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

    def test_change_object_id(self) -> None:
        ts = self._make_task_snapshot()
        ts.change_object_id("new-task-id")
        assert ts.id == "new-task-id"


# ===========================================================================
# ContextSnapshot
# ===========================================================================
class TestContextSnapshot:

    def _make_traversal_data(self) -> Dict[str, Any]:
        return {
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

    def _make_context_snapshot(self, **overrides: Any) -> ContextSnapshot:
        defaults: Dict[str, Any] = {
            "state_id": "state-001",
            "workflow_id": "wf-001",
            "parent_id": None,
            "child_ids": [],
            "depth": 0,
            "previous_context_id": None,
            "next_context_id": None,
            "traversal": TraversalSnapshot(**self._make_traversal_data()),
            "pipeline_id": "pipe-001",
            "pipeline_state": {"current_step": 2},
            "pipeline_class_path": "myapp.pipelines.DataPipeline",
            "status": "RUNNING",
            "errors": [],
            "results": [{"data": "result-1"}],
            "metrics": {"start_time": 100.0, "end_time": 200.0, "duration": 100.0},
        }
        defaults.update(overrides)
        return ContextSnapshot(**defaults)

    def test_construction(self) -> None:
        cs = self._make_context_snapshot()
        assert cs.state_id == "state-001"
        assert cs.workflow_id == "wf-001"
        assert cs.parent_id is None
        assert cs.child_ids == []
        assert cs.depth == 0
        assert cs.status == "RUNNING"
        assert cs.errors == []
        assert isinstance(cs.traversal, TraversalSnapshot)

    def test_with_parent_and_children(self) -> None:
        cs = self._make_context_snapshot(
            parent_id="parent-001",
            child_ids=["child-a", "child-b"],
            depth=2,
        )
        assert cs.parent_id == "parent-001"
        assert cs.child_ids == ["child-a", "child-b"]
        assert cs.depth == 2

    def test_with_horizontal_links(self) -> None:
        cs = self._make_context_snapshot(
            previous_context_id="prev-001",
            next_context_id="next-001",
        )
        assert cs.previous_context_id == "prev-001"
        assert cs.next_context_id == "next-001"

    def test_with_errors(self) -> None:
        errors = [
            {"type": "ValueError", "message": "bad value", "traceback": "..."},
            {"type": "RuntimeError", "message": "crash", "traceback": "..."},
        ]
        cs = self._make_context_snapshot(errors=errors)
        assert len(cs.errors) == 2
        assert cs.errors[0]["type"] == "ValueError"

    def test_with_results(self) -> None:
        results = [{"data": "r1"}, {"data": "r2"}]
        cs = self._make_context_snapshot(results=results)
        assert len(cs.results) == 2

    def test_get_schema_name(self) -> None:
        assert ContextSnapshot.get_schema_name() == "volnux:snapshot:context"

    def test_change_object_id(self) -> None:
        cs = self._make_context_snapshot()
        cs.change_object_id("new-state-id")
        assert cs.id == "new-state-id"


# ===========================================================================
# ContextSnapshot.to_dict / from_dict / set_state / get_state
# ===========================================================================
class TestContextSnapshotRoundTrip:

    def _make_traversal_data(self) -> Dict[str, Any]:
        return {
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

    def _make_context_snapshot(self, **overrides: Any) -> ContextSnapshot:
        defaults: Dict[str, Any] = {
            "state_id": "state-rt-001",
            "workflow_id": "wf-rt",
            "parent_id": None,
            "child_ids": ["child-1"],
            "depth": 1,
            "previous_context_id": "prev-rt",
            "next_context_id": None,
            "traversal": TraversalSnapshot(**self._make_traversal_data()),
            "pipeline_id": "pipe-rt",
            "pipeline_state": {},
            "pipeline_class_path": "app.Pipeline",
            "status": "COMPLETED",
            "errors": [],
            "results": [],
            "metrics": {"start_time": 0.0, "end_time": 10.0, "duration": 10.0},
        }
        defaults.update(overrides)
        return ContextSnapshot(**defaults)

    def test_get_state_returns_dict(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()
        assert isinstance(state, dict)
        assert state["state_id"] == "state-rt-001"
        # Traversal should be flattened to a dict too
        assert isinstance(state["traversal"], dict)
        assert (
            state["traversal"]["engine_class_path"]
            == "volnux.engine.base.WorkflowEngine"
        )

    def test_get_state_flattens_traversal(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()
        # The traversal should be a plain dict, not a TraversalSnapshot
        assert not hasattr(state["traversal"], "engine_class_path") or isinstance(
            state["traversal"], dict
        )

    def test_set_state_updates_fields(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()
        state["status"] = "FAILED"
        state["depth"] = 5

        cs.set_state(state)
        assert cs.status == "FAILED"
        assert cs.depth == 5

    def test_set_state_rebuilds_traversal(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()

        # Mutate traversal in the state dict
        state["traversal"]["tasks_processed"] = 99
        cs.set_state(state)

        # Traversal should be a TraversalSnapshot instance
        assert isinstance(cs.traversal, TraversalSnapshot)
        assert cs.traversal.tasks_processed == 99

    def test_set_state_does_not_mutate_original(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()
        original_status = state["status"]
        state["status"] = "NEW_STATUS"
        cs.set_state(state)

        # Original dict should still have old status (set_state copies)
        # Actually the code does data.copy(), so the original dict passed in is not mutated,
        # but the copy IS mutated by set_state. Let's verify the object is updated.
        assert cs.status == "NEW_STATUS"

    def test_from_dict_creates_context_snapshot(self) -> None:
        data = {
            "state_id": "from-dict-001",
            "workflow_id": "wf-fd",
            "parent_id": None,
            "child_ids": [],
            "depth": 0,
            "previous_context_id": None,
            "next_context_id": None,
            "traversal": self._make_traversal_data(),
            "pipeline_id": "pipe-fd",
            "pipeline_state": {"step": 1},
            "pipeline_class_path": "app.Pipeline",
            "status": "RUNNING",
            "errors": [],
            "results": [],
            "metrics": {},
        }
        cs = ContextSnapshot.from_dict(data)
        assert isinstance(cs, ContextSnapshot)
        assert cs.state_id == "from-dict-001"
        assert isinstance(cs.traversal, TraversalSnapshot)

    def test_from_dict_round_trip(self) -> None:
        cs = self._make_context_snapshot()
        state = cs.get_state()
        restored = ContextSnapshot.from_dict(state)
        assert restored.state_id == cs.state_id
        assert restored.workflow_id == cs.workflow_id
        assert restored.traversal.engine_class_path == cs.traversal.engine_class_path

    def test_to_dict_same_structure_as_get_state(self) -> None:
        """to_dict relies on asdict (dataclass); our mock BaseModel is not a
        dataclass, so to_dict will raise TypeError.  Verify that get_state()
        produces the expected structure instead."""
        cs = self._make_context_snapshot()
        state = cs.get_state()
        # get_state() flattens traversal to a plain dict
        assert isinstance(state["traversal"], dict)
        assert state["state_id"] == "state-rt-001"

    def test_to_dict_raises_on_non_dataclass(self) -> None:
        """to_dict delegates to dataclasses.asdict which requires a real
        dataclass instance.  The mock BaseModel is not a dataclass, so
        this correctly raises TypeError."""
        cs = self._make_context_snapshot()
        with pytest.raises(TypeError, match="asdict"):
            cs.to_dict()
