from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.engine.base import (
    EngineExecutionResult,
    EngineResult,
    TaskNode,
    WorkflowEngine, SubgraphErrorStrategy,
)
from volnux.engine.checkpoint_config import CheckPointConfig, CheckPointFrequency



class _ConcreteEngine(WorkflowEngine):
    """Trivial concrete implementation so we can instantiate WorkflowEngine."""

    @property
    def task_queue(self):
        return []

    @property
    def sink_queue(self):
        return []

    def spawn_sub_engine(
        self,
        root_task: "TaskType",
        pipeline: "Pipeline",
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
    ) -> "WorkflowEngine":
        return _ConcreteEngine()

    async def execute(self, root_task, pipeline):
        return EngineResult(status=EngineExecutionResult.COMPLETED)


class TestEngineExecutionResult:
    """Tests for the EngineExecutionResult enum."""

    def test_all_values_exist(self):
        assert EngineExecutionResult.COMPLETED is not None
        assert EngineExecutionResult.TERMINATED_EARLY is not None
        assert EngineExecutionResult.FAILED is not None

    def test_string_values(self):
        assert EngineExecutionResult.COMPLETED.value == "completed"
        assert EngineExecutionResult.TERMINATED_EARLY.value == "terminated_early"
        assert EngineExecutionResult.FAILED.value == "failed"

    def test_membership(self):
        assert EngineExecutionResult.COMPLETED in EngineExecutionResult
        assert EngineExecutionResult.TERMINATED_EARLY in EngineExecutionResult
        assert EngineExecutionResult.FAILED in EngineExecutionResult

    def test_enum_iteration(self):
        values = list(EngineExecutionResult)
        assert len(values) == 4
        assert EngineExecutionResult.COMPLETED in values
        assert EngineExecutionResult.TERMINATED_EARLY in values
        assert EngineExecutionResult.FAILED in values



class TestTaskNode:
    """Tests for the TaskNode named-tuple."""

    def test_construction_with_task_only(self):
        mock_task = MagicMock()
        node = TaskNode(task=mock_task)
        assert node.task is mock_task
        assert node.previous_context is None

    def test_construction_with_task_and_previous_context(self):
        mock_task = MagicMock()
        mock_ctx = MagicMock()
        node = TaskNode(task=mock_task, previous_context=mock_ctx)
        assert node.task is mock_task
        assert node.previous_context is mock_ctx

    def test_default_previous_context_is_none(self):
        node = TaskNode(task=MagicMock())
        assert node.previous_context is None

    def test_is_named_tuple(self):
        # typing.NamedTuple creates a tuple subclass with _fields and _make
        assert issubclass(TaskNode, tuple)
        assert hasattr(TaskNode, "_fields")
        assert hasattr(TaskNode, "_make")

    def test_immutable(self):
        mock_task = MagicMock()
        node = TaskNode(task=mock_task)
        with pytest.raises(AttributeError):
            node.task = MagicMock()

    def test_indexed_access(self):
        mock_task = MagicMock()
        mock_ctx = MagicMock()
        node = TaskNode(task=mock_task, previous_context=mock_ctx)
        assert node[0] is mock_task
        assert node[1] is mock_ctx

    def test_len(self):
        node = TaskNode(task=MagicMock())
        assert len(node) == 2

    def test_fields(self):
        assert TaskNode._fields == ("task", "previous_context")


class TestEngineResult:
    """Tests for the EngineResult dataclass."""

    def test_construction_status_only(self):
        result = EngineResult(status=EngineExecutionResult.COMPLETED)
        assert result.status == EngineExecutionResult.COMPLETED
        assert result.final_context is None
        assert result.error is None
        assert result.tasks_processed == 0

    def test_construction_with_all_fields(self):
        mock_ctx = MagicMock()
        err = RuntimeError("boom")
        result = EngineResult(
            status=EngineExecutionResult.FAILED,
            final_context=mock_ctx,
            error=err,
            tasks_processed=5,
        )
        assert result.status == EngineExecutionResult.FAILED
        assert result.final_context is mock_ctx
        assert result.error is err
        assert result.tasks_processed == 5

    def test_is_dataclass(self):
        assert dataclasses.is_dataclass(EngineResult)

    def test_mutable(self):
        """EngineResult is a plain dataclass — fields are mutable."""
        result = EngineResult(status=EngineExecutionResult.COMPLETED)
        result.tasks_processed = 10
        assert result.tasks_processed == 10


class TestWorkflowEngine:
    """Tests for WorkflowEngine abstract base class behaviour."""

    def test_cannot_instantiate_abstract_class_directly(self):
        with pytest.raises(TypeError):
            WorkflowEngine()  # type: ignore[abstract]

    def test_init_defaults(self):
        engine = _ConcreteEngine()
        assert engine.tasks_processed == 0
        assert engine.current_task_node is None
        assert engine.final_context is None
        assert engine._checkpointer is None
        assert engine.checkpoint_config is None

    def test_init_with_checkpointing_creates_default_config(self):
        engine = _ConcreteEngine(enable_checkpointing=True)
        assert engine.checkpoint_config is not None
        assert isinstance(engine.checkpoint_config, CheckPointConfig)
        # The default policy frequency should be PER_TASK
        assert engine.checkpoint_config.policy.frequency == CheckPointFrequency.PER_TASK

    def test_init_with_checkpointing_and_explicit_config(self):
        explicit_config = MagicMock(spec=CheckPointConfig)
        engine = _ConcreteEngine(
            enable_checkpointing=True,
            checkpoint_config=explicit_config,
        )
        assert engine.checkpoint_config is explicit_config

    def test_init_without_checkpointing_ignores_explicit_config(self):
        explicit_config = MagicMock(spec=CheckPointConfig)
        engine = _ConcreteEngine(
            enable_checkpointing=False,
            checkpoint_config=explicit_config,
        )
        assert engine.checkpoint_config is None

    # -- get_name ------------------------------------------------------------

    def test_get_name_returns_class_name(self):
        engine = _ConcreteEngine()
        assert engine.get_name() == "_ConcreteEngine"

    def test_get_name_custom_subclass(self):
        class MyEngine(_ConcreteEngine):
            pass

        engine = MyEngine()
        assert engine.get_name() == "MyEngine"

    # -- enable_checkpointing ------------------------------------------------

    def test_enable_checkpointing_sets_checkpointer(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PERIODIC,
        )
        assert engine._checkpointer is mock_checkpointer
        assert engine._checkpoint_frequency == CheckPointFrequency.PERIODIC

    def test_enable_checkpointing_default_frequency(self):
        engine = _ConcreteEngine()
        engine.enable_checkpointing(checkpointer=MagicMock())
        assert engine._checkpoint_frequency == CheckPointFrequency.PER_TASK

    @pytest.mark.asyncio
    async def test_checkpoint_before_task_noop_when_no_checkpointer(self):
        engine = _ConcreteEngine()
        mock_ctx = MagicMock()
        mock_task_node = TaskNode(task=MagicMock())
        # Should return without error and NOT call persist
        await engine._checkpoint_before_task(mock_ctx, mock_task_node)
        mock_ctx.persist.assert_not_called()
        assert engine.current_task_node is None

    @pytest.mark.asyncio
    async def test_checkpoint_before_task_sets_current_task_node(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = AsyncMock()
        mock_task_node = TaskNode(task=MagicMock())
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.ON_STATE_CHANGE,
        )
        await engine._checkpoint_before_task(mock_ctx, mock_task_node)
        assert engine.current_task_node is mock_task_node
        # ON_STATE_CHANGE frequency should NOT call persist before task
        mock_ctx.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_checkpoint_before_task_persists_for_per_task_frequency(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = AsyncMock()
        mock_task = MagicMock()
        mock_task.event = "test_event"
        mock_task_node = TaskNode(task=mock_task)
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        await engine._checkpoint_before_task(mock_ctx, mock_task_node)
        mock_ctx.persist.assert_awaited_once()
        assert engine.current_task_node is mock_task_node

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_noop_when_no_checkpointer(self):
        engine = _ConcreteEngine()
        mock_ctx = MagicMock()
        # Should return without error and NOT increment or persist
        await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.tasks_processed == 0
        mock_ctx.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_increments_tasks_processed(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PERIODIC,
        )
        await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.tasks_processed == 1
        # PERIODIC should NOT call persist
        mock_ctx.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_clears_current_task_node(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PERIODIC,
        )
        engine.current_task_node = TaskNode(task=MagicMock())
        await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.current_task_node is None

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_persists_for_per_task(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = AsyncMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        await engine._checkpoint_after_task(mock_ctx, success=True)
        mock_ctx.persist.assert_awaited_once()
        assert engine.tasks_processed == 1

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_persists_for_on_state_change(self):
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = AsyncMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.ON_STATE_CHANGE,
        )
        await engine._checkpoint_after_task(mock_ctx, success=False)
        mock_ctx.persist.assert_awaited_once()
        assert engine.tasks_processed == 1

    @pytest.mark.asyncio
    async def test_checkpoint_after_task_cumulative_count(self):
        """Multiple _checkpoint_after_task calls accumulate tasks_processed."""
        engine = _ConcreteEngine()
        mock_checkpointer = MagicMock()
        mock_ctx = AsyncMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        for _ in range(3):
            await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.tasks_processed == 3
        assert mock_ctx.persist.await_count == 3

    @pytest.mark.asyncio
    async def test_execute_returns_result(self):
        engine = _ConcreteEngine()
        result = await engine.execute(root_task=MagicMock(), pipeline=MagicMock())
        assert isinstance(result, EngineResult)
        assert result.status == EngineExecutionResult.COMPLETED


    def test_task_queue_is_accessible(self):
        engine = _ConcreteEngine()
        assert engine.task_queue == []

    def test_sink_queue_is_accessible(self):
        engine = _ConcreteEngine()
        assert engine.sink_queue == []
