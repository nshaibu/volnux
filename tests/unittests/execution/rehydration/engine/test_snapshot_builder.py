"""Tests for rehydrator/engine/builder — SnapshotBuilder.

Covers:
  - build_pipeline_task() creates TaskSnapshot from task profile.
  - build_engine_state() captures the engine's queue state for a context.

An ExecutionContext is persisted as itself (``await context.save()``), so the
builder has no context snapshot to build.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from volnux.execution.rehydrator.engine.builder import SnapshotBuilder
from volnux.execution.rehydrator.engine.serializer import StateSerializer
from volnux.execution.rehydrator.engine.snapshot import (
    EngineStateSnapshot,
    TaskSnapshot,
)


# ===========================================================================
# Helpers
# ===========================================================================
def _make_mock_engine(**overrides: Any) -> MagicMock:
    """Build a mock WorkflowEngine with sensible defaults."""
    engine = MagicMock()
    engine.__module__ = "volnux.engine.base"
    engine.__class__.__name__ = "WorkflowEngine"
    engine.task_queue = []
    engine.sink_queue = []
    engine.tasks_processed = 0
    engine.current_task_node = None

    for key, value in overrides.items():
        setattr(engine, key, value)

    return engine


def _make_mock_context(engine: Any = "default", **overrides: Any) -> MagicMock:
    """Build a mock ExecutionContext that has an engine attached."""
    context = MagicMock()
    context.id = "ctx-obj-001"
    context.state_id = "ctx-obj-001"
    context.get_engine.return_value = (
        _make_mock_engine() if engine == "default" else engine
    )
    for key, value in overrides.items():
        setattr(context, key, value)
    return context


def _make_mock_task_profile(**overrides: Any) -> MagicMock:
    """Build a mock PipelineTask for build_pipeline_task tests."""
    task = MagicMock()
    task.is_grouping = False  # Explicit: getattr checks truthiness

    mock_event_class = MagicMock()
    mock_event_class.__module__ = "myapp.events"
    mock_event_class.__name__ = "ProcessDataTask"
    task.get_event_class.return_value = mock_event_class

    task.get_id.return_value = "task-proto-001"
    task.get_event_name.return_value = "process_data"
    task.sink_node = None
    task.sink_pipe = None
    task.sequence_number = 0
    task.descriptor = None
    task.descriptor_pipe = None

    mock_options = MagicMock()
    mock_options.as_dict.return_value = {"batch_size": 10}
    task.options = mock_options

    mock_condition = MagicMock()
    mock_condition.as_dict.return_value = {"active": True}
    task.condition_node = mock_condition

    for key, value in overrides.items():
        setattr(task, key, value)

    return task


@pytest.fixture()
def builder() -> SnapshotBuilder:
    return SnapshotBuilder(serializer=StateSerializer)


# ===========================================================================
# build_pipeline_task()
# ===========================================================================
class TestSnapshotBuilderBuildPipelineTask:

    def test_build_pipeline_task_returns_task_snapshot(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        task_profile = _make_mock_task_profile()
        snapshot = builder.build_pipeline_task(task_profile)

        assert isinstance(snapshot, TaskSnapshot)
        assert snapshot.task_id == "task-proto-001"
        assert snapshot.event_name == "process_data"
        assert snapshot.event_class_import_path == "myapp.events.ProcessDataTask"
        assert snapshot.task_type == "normal"

    def test_build_pipeline_task_with_group(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        task_profile = _make_mock_task_profile()
        # is_grouping needs to be accessible via getattr
        task_profile.is_grouping = True

        snapshot = builder.build_pipeline_task(task_profile)
        assert snapshot.task_type == "group"

    def test_build_pipeline_task_context_and_checkpoint(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        snapshot = builder.build_pipeline_task(
            _make_mock_task_profile(),
            context_id="ctx-1",
            task_checkpoint={"step": 3},
        )
        assert snapshot.context_id == "ctx-1"
        assert snapshot.task_checkpoint == {"step": 3}

    def test_build_pipeline_task_defaults(self, builder: SnapshotBuilder) -> None:
        snapshot = builder.build_pipeline_task(_make_mock_task_profile())
        assert snapshot.context_id is None
        assert snapshot.task_checkpoint is None

    def test_build_pipeline_task_real_task_keeps_namespace_and_descriptors(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        from volnux import EventBase
        from volnux.task import PipelineTask

        class SnapshottedEvent(EventBase):
            def process(self, *args, **kwargs):
                return True, None

        task = PipelineTask(event=SnapshottedEvent)
        task.namespace = "billing"
        task.descriptors = {"out_of_balance": 2}

        snapshot = builder.build_pipeline_task(task, context_id="ctx-2")
        assert snapshot.namespace == "billing"
        assert snapshot.descriptors == {"out_of_balance": 2}
        assert snapshot.id == task.get_id()

    def test_build_pipeline_task_change_object_id(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        task_profile = _make_mock_task_profile()
        snapshot = builder.build_pipeline_task(task_profile)
        assert snapshot.id == "task-proto-001"


# ===========================================================================
# build_engine_state()
# ===========================================================================
class TestBuildEngineState:

    def build(self, builder: SnapshotBuilder, **engine_overrides: Any):
        engine = _make_mock_engine(**engine_overrides)
        return builder.build_engine_state(_make_mock_context(engine=engine))

    def test_returns_engine_state_keyed_by_context(
        self, builder: SnapshotBuilder
    ) -> None:
        state = self.build(builder)

        assert isinstance(state, EngineStateSnapshot)
        assert state.context_id == "ctx-obj-001"
        assert state.id == "ctx-obj-001"

    def test_empty_queues(self, builder: SnapshotBuilder) -> None:
        state = self.build(builder)

        assert state.task_queue_snapshot == []
        assert state.sink_queue_snapshot == []
        assert state.current_task is None
        assert state.tasks_processed == 0
        assert state.current_task_queue_size == 0
        assert state.current_sink_queue_size == 0

    def test_with_task_queue(self, builder: SnapshotBuilder) -> None:
        node_a = MagicMock()
        node_a.task.get_id.return_value = "ta"
        node_a.previous_context.state_id = "ctx-a"
        node_b = MagicMock()
        node_b.task.get_id.return_value = "tb"
        node_b.previous_context.state_id = "ctx-b"

        state = self.build(builder, task_queue=[node_a, node_b])

        assert state.current_task_queue_size == 2
        assert [t["task_id"] for t in state.task_queue_snapshot] == ["ta", "tb"]
        assert [t["position_in_queue"] for t in state.task_queue_snapshot] == [0, 1]
        assert state.task_queue_snapshot[1]["previous_context_id"] == "ctx-b"

    def test_with_sink_queue(self, builder: SnapshotBuilder) -> None:
        sink_node = MagicMock()
        sink_node.task.get_id.return_value = "sink-1"
        sink_node.previous_context.state_id = "ctx-sink"

        state = self.build(builder, sink_queue=[sink_node])

        assert state.current_sink_queue_size == 1
        assert state.sink_queue_snapshot[0]["task_id"] == "sink-1"

    def test_with_current_task_node(self, builder: SnapshotBuilder) -> None:
        current = MagicMock()
        current.task.get_id.return_value = "current-t"
        current.previous_context.state_id = "ctx-current"

        state = self.build(builder, current_task_node=current)

        assert state.current_task["task_id"] == "current-t"
        assert state.current_task["position_in_queue"] == 0

    def test_tasks_processed(self, builder: SnapshotBuilder) -> None:
        assert self.build(builder, tasks_processed=12).tasks_processed == 12

    def test_queue_index_always_zero(self, builder: SnapshotBuilder) -> None:
        """Builder always sets queue_index=0 (snapshot-time convention)."""
        assert self.build(builder).queue_index == 0

    def test_without_engine_raises(self, builder: SnapshotBuilder) -> None:
        with pytest.raises(ValueError, match="no engine attached"):
            builder.build_engine_state(_make_mock_context(engine=None))
