"""Tests for rehydrator/engine/builder — SnapshotBuilder.

Covers:
  - build() creates ContextSnapshot with all expected fields.
  - build_pipeline_task() creates TaskSnapshot from task profile.
  - _build_traversal() correctly captures engine queue state.
  - _build_pipeline_state() delegates to pipeline.__getstate__.
  - _build_metrics() captures start_time, end_time, duration.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from volnux.execution.rehydrator.engine.builder import SnapshotBuilder
from volnux.execution.rehydrator.engine.serializer import StateSerializer
from volnux.execution.rehydrator.engine.snapshot import (
    ContextSnapshot,
    TraversalSnapshot,
    TaskSnapshot,
)


# ===========================================================================
# Helpers
# ===========================================================================
def _make_traversal_data() -> Dict[str, Any]:
    """Standard TraversalSnapshot dict used across builder tests."""
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


def _make_mock_context(**overrides: Any) -> MagicMock:
    """Build a mock ExecutionContext with sensible defaults."""
    context = MagicMock()
    context.id = "ctx-obj-001"
    context.state_id = "state-001"
    context.workflow_id = "wf-001"
    context.parent_context = None
    context.child_contexts = []
    context.previous_context = None
    context.next_context = None

    # Engine
    context.get_engine.return_value = _make_mock_engine()

    # Pipeline
    mock_pipeline = MagicMock()
    mock_pipeline.id = "pipe-001"
    mock_pipeline.__class__.__module__ = "myapp.pipelines"
    mock_pipeline.__class__.__name__ = "DataPipeline"
    mock_pipeline.__getstate__ = MagicMock(return_value={"step": 3})
    context.pipeline = mock_pipeline

    # Depth
    context.get_depth.return_value = 1

    # Metrics
    mock_metrics = MagicMock()
    mock_metrics.start_time = 100.0
    mock_metrics.end_time = 250.0
    mock_metrics.duration = 150.0
    context.metrics = mock_metrics

    # State (async) — builder does ``await context.state_async``,
    # so state_async must be an awaitable (coroutine), not an AsyncMock.
    mock_state = MagicMock()
    mock_state.status.value = "RUNNING"
    mock_state.errors = []
    mock_state.results = []

    async def _mock_state_async():
        return mock_state

    context.state_async = _mock_state_async()

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
# build()
# ===========================================================================
class TestSnapshotBuilderBuild:

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_returns_context_snapshot(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"

        context = _make_mock_context()
        snapshot = await builder.build(context)

        assert isinstance(snapshot, ContextSnapshot)
        assert snapshot.state_id == "state-001"
        assert snapshot.workflow_id == "wf-001"
        assert snapshot.depth == 1
        assert snapshot.pipeline_id == "pipe-001"
        assert snapshot.pipeline_class_path == "myapp.pipelines.DataPipeline"
        assert snapshot.status == "RUNNING"
        assert snapshot.errors == []
        assert snapshot.results == []

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_captures_pipeline_state(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"
        context = _make_mock_context()
        snapshot = await builder.build(context)

        assert snapshot.pipeline_state == {"step": 3}

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_captures_metrics(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"
        context = _make_mock_context()
        snapshot = await builder.build(context)

        assert snapshot.metrics["start_time"] == 100.0
        assert snapshot.metrics["end_time"] == 250.0
        assert snapshot.metrics["duration"] == 150.0

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_with_parent_and_children(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"

        mock_parent = MagicMock()
        mock_parent.state_id = "parent-001"
        mock_child_a = MagicMock()
        mock_child_a.state_id = "child-a"
        mock_child_b = MagicMock()
        mock_child_b.state_id = "child-b"

        context = _make_mock_context(
            parent_context=mock_parent,
            child_contexts=[mock_child_a, mock_child_b],
        )
        snapshot = await builder.build(context)

        assert snapshot.parent_id == "parent-001"
        assert snapshot.child_ids == ["child-a", "child-b"]

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_with_horizontal_links(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"

        mock_prev = MagicMock()
        mock_prev.state_id = "prev-001"
        mock_next = MagicMock()
        mock_next.state_id = "next-001"

        context = _make_mock_context(
            previous_context=mock_prev,
            next_context=mock_next,
        )
        snapshot = await builder.build(context)

        assert snapshot.previous_context_id == "prev-001"
        assert snapshot.next_context_id == "next-001"

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_with_errors(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"

        mock_state = MagicMock()
        mock_state.status.value = "FAILED"
        mock_state.errors = [ValueError("bad"), RuntimeError("crash")]
        mock_state.results = []

        context = _make_mock_context()

        async def _err_state():
            return mock_state

        context.state_async = _err_state()

        snapshot = await builder.build(context)
        assert len(snapshot.errors) == 2
        assert snapshot.errors[0]["type"] == "ValueError"
        assert snapshot.errors[1]["type"] == "RuntimeError"

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_change_object_id_called(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"
        context = _make_mock_context()

        snapshot = await builder.build(context)
        assert snapshot.id == "ctx-obj-001"

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_none_pipeline_raises(
        self,
        mock_import_str: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        """When context.pipeline is None, serialize_pipeline_ref will raise
        AttributeError. This is a known limitation — pipeline should not be None."""
        mock_import_str.return_value = "volnux.engine.base.WorkflowEngine"

        context = _make_mock_context()
        context.pipeline = None
        with pytest.raises(AttributeError):
            await builder.build(context)


# ===========================================================================
# build_pipeline_task()
# ===========================================================================
class TestSnapshotBuilderBuildPipelineTask:

    @patch("rehydrator.engine.builder.get_obj_klass_import_str")
    async def test_build_pipeline_task_returns_task_snapshot(
        self,
        mock_import_str: MagicMock,
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

    def test_build_pipeline_task_change_object_id(
        self,
        builder: SnapshotBuilder,
    ) -> None:
        task_profile = _make_mock_task_profile()
        snapshot = builder.build_pipeline_task(task_profile)
        assert snapshot.id == "task-proto-001"


# ===========================================================================
# _build_traversal()
# ===========================================================================
class TestBuildTraversal:

    def test_empty_queues(self, builder: SnapshotBuilder) -> None:
        engine = _make_mock_engine()
        traversal = builder._build_traversal(engine)

        assert traversal.task_queue_snapshot == []
        assert traversal.sink_queue_snapshot == []
        assert traversal.current_task is None
        assert traversal.tasks_processed == 0
        assert traversal.current_task_queue_size == 0
        assert traversal.current_sink_queue_size == 0

    def test_with_task_queue(self, builder: SnapshotBuilder) -> None:
        node_a = MagicMock()
        node_a.task.get_id.return_value = "ta"
        node_a.previous_context.state_id = "ctx-a"

        node_b = MagicMock()
        node_b.task.get_id.return_value = "tb"
        node_b.previous_context.state_id = "ctx-b"

        engine = _make_mock_engine(task_queue=[node_a, node_b])
        traversal = builder._build_traversal(engine)

        assert traversal.current_task_queue_size == 2
        assert len(traversal.task_queue_snapshot) == 2
        assert traversal.task_queue_snapshot[0]["task_id"] == "ta"
        assert traversal.task_queue_snapshot[1]["task_id"] == "tb"
        assert traversal.task_queue_snapshot[0]["position_in_queue"] == 0
        assert traversal.task_queue_snapshot[1]["position_in_queue"] == 1

    def test_with_sink_queue(self, builder: SnapshotBuilder) -> None:
        sink_node = MagicMock()
        sink_node.task.get_id.return_value = "sink-1"
        sink_node.previous_context.state_id = "ctx-sink"

        engine = _make_mock_engine(sink_queue=[sink_node])
        traversal = builder._build_traversal(engine)

        assert traversal.current_sink_queue_size == 1
        assert len(traversal.sink_queue_snapshot) == 1
        assert traversal.sink_queue_snapshot[0]["task_id"] == "sink-1"

    def test_with_current_task_node(self, builder: SnapshotBuilder) -> None:
        current = MagicMock()
        current.task.get_id.return_value = "current-t"
        current.previous_context.state_id = "ctx-current"

        engine = _make_mock_engine(current_task_node=current)
        traversal = builder._build_traversal(engine)

        assert traversal.current_task is not None
        assert traversal.current_task["task_id"] == "current-t"
        assert traversal.current_task["position_in_queue"] == 0

    def test_tasks_processed(self, builder: SnapshotBuilder) -> None:
        engine = _make_mock_engine(tasks_processed=12)
        traversal = builder._build_traversal(engine)
        assert traversal.tasks_processed == 12

    def test_queue_index_always_zero(self, builder: SnapshotBuilder) -> None:
        """Builder always sets queue_index=0 (snapshot-time convention)."""
        engine = _make_mock_engine()
        traversal = builder._build_traversal(engine)
        assert traversal.queue_index == 0


# ===========================================================================
# _build_pipeline_state()
# ===========================================================================
class TestBuildPipelineState:

    def test_with_pipeline(self, builder: SnapshotBuilder) -> None:
        mock_pipeline = MagicMock()
        mock_pipeline.__getstate__ = MagicMock(return_value={"step": 5, "total": 10})

        context = _make_mock_context()
        context.pipeline = mock_pipeline

        state = builder._build_pipeline_state(context)
        assert state == {"step": 5, "total": 10}

    def test_none_pipeline_returns_empty(self, builder: SnapshotBuilder) -> None:
        context = _make_mock_context()
        context.pipeline = None

        state = builder._build_pipeline_state(context)
        assert state == {}


# ===========================================================================
# _build_metrics()
# ===========================================================================
class TestBuildMetrics:

    def test_captures_all_fields(self, builder: SnapshotBuilder) -> None:
        context = _make_mock_context()
        metrics = builder._build_metrics(context)

        assert metrics == {
            "start_time": 100.0,
            "end_time": 250.0,
            "duration": 150.0,
        }

    def test_none_times(self, builder: SnapshotBuilder) -> None:
        context = _make_mock_context()
        context.metrics.start_time = None
        context.metrics.end_time = None
        context.metrics.duration = None

        metrics = builder._build_metrics(context)
        assert metrics["start_time"] is None
        assert metrics["end_time"] is None
