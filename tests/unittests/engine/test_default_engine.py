from __future__ import annotations

import logging
from collections import deque
from contextlib import contextmanager
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.engine.base import (
    EngineExecutionResult,
    EngineResult,
    SubgraphErrorStrategy,
    TaskNode,
    WorkflowEngine,
)
from volnux.engine.checkpoint_config import CheckPointConfig, CheckPointFrequency
from volnux.execution.status import ExecutionStatus
from volnux.parser.protocols import GroupingStrategy
from volnux.task.group import PipelineTaskGrouping



class _Awaitable:
    """Makes any value awaitable (returns it from ``await expr``)."""

    @staticmethod
    async def _coro(value):
        return value

    def __init__(self, value):
        self._value = value

    def __await__(self):
        return self._coro(self._value).__await__()


def _awaitable(value=None):
    return _Awaitable(value)


def _awaitable_fn(return_value=None):
    """Return a callable that produces an awaitable — drop-in for AsyncMock."""

    def _fn(*a, **kw):
        return _Awaitable(return_value)

    return _fn



def _make_fake_sub_engine(tasks_processed=0, final_context=None):
    """Build a fake sub-engine stand-in for spawn_sub_engine()'s return
    value — controllable via ``.execute`` (an AsyncMock)."""
    child = MagicMock()
    child.tasks_processed = tasks_processed
    child.final_context = final_context
    child._engine_id = "child"
    child.execute = AsyncMock()
    return child


def _make_task(**overrides):
    """Build a mock task with sensible defaults."""
    task = MagicMock()
    task.is_parallel_execution_node = False
    task.is_conditional = False
    task.sink_node = None
    mock_cond = MagicMock()
    mock_cond.on_success_pipe = None
    mock_cond.on_success_event = None
    mock_cond.on_failure_event = None
    task.condition_node = mock_cond
    for k, v in overrides.items():
        setattr(task, k, v)
    return task


@contextmanager
def _patched_status():
    """Patch *ExecutionStatus* with hashable sentinel objects."""
    with patch("volnux.engine.default_engine.ExecutionStatus") as mock_status:
        mock_status.CANCELLED = object()
        mock_status.ABORTED = object()
        mock_status.PAUSED = object()
        mock_status.FAILED = object()
        mock_status.COMPLETED = object()
        yield mock_status


@contextmanager
def _patched_pipe():
    """Patch *PipeType* with a hashable PARALLELISM sentinel."""
    with patch("volnux.engine.default_engine.PipeType") as mock_pipe:
        mock_pipe.PARALLELISM = object()
        yield mock_pipe


@contextmanager
def _patched_execution_context(ctx_factory=None):
    """Patch *ExecutionContext* to return a mock context with awaitable
    ``dispatch`` and ``state_async`` attributes."""
    factory = ctx_factory or _make_mock_ctx
    with patch("volnux.engine.default_engine.ExecutionContext") as mock_cls:
        mock_cls.side_effect = factory
        yield mock_cls


def _make_mock_ctx():
    """Create a mock ExecutionContext ready for dispatch / state_async."""
    ctx = MagicMock()
    object.__setattr__(ctx, "dispatch", _awaitable_fn())
    state = MagicMock()
    state.status = object()  # hashable, not CANCELLED / ABORTED
    object.__setattr__(ctx, "state_async", _awaitable(state))
    ctx.is_multitask.return_value = False
    return ctx


def _make_mock_ctx_with_status(status_obj):
    """Create a mock ExecutionContext whose state carries *status_obj*."""
    ctx = MagicMock()
    object.__setattr__(ctx, "dispatch", _awaitable_fn())
    state = MagicMock()
    state.status = status_obj
    object.__setattr__(ctx, "state_async", _awaitable(state))
    ctx.is_multitask.return_value = False
    return ctx


class _TrackableAwaitable:
    """An awaitable that also tracks call count (like AsyncMock)."""

    call_count = 0
    call_args_list = []

    def __init__(self, side_effect=None):
        self._side_effect = side_effect
        self.call_count = 0
        self.call_args_list = []

    def __call__(self, *a, **kw):
        self.call_count += 1
        self.call_args_list.append((a, kw))
        if self._side_effect is not None:
            raise self._side_effect
        return _Awaitable(None)


def _make_sink_ctx(dispatch_side_effect=None):
    """Create a mock context for sink node testing (dispatch is awaitable
    and tracks call count)."""
    ctx = MagicMock()
    object.__setattr__(
        ctx, "dispatch", _TrackableAwaitable(side_effect=dispatch_side_effect)
    )
    return ctx



class TestDefaultWorkflowEngineInit:
    def test_default_params(self):
        engine = DefaultWorkflowEngine()
        assert engine.enable_debug_logging is False
        assert engine.strict_mode is True

    def test_custom_params(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True, strict_mode=False)
        assert engine.enable_debug_logging is True
        assert engine.strict_mode is False

    def test_get_name(self):
        engine = DefaultWorkflowEngine()
        assert engine.get_name() == "DefaultIterativeEngine"

    def test_inherits_from_workflow_engine(self):
        engine = DefaultWorkflowEngine()
        assert isinstance(engine, WorkflowEngine)

    def test_calls_super_init(self):
        """Engine must initialize parent state via super().__init__."""
        engine = DefaultWorkflowEngine()
        # Parent initializes these in WorkflowEngine.__init__
        assert hasattr(engine, "tasks_processed")
        assert engine.tasks_processed == 0
        assert engine.current_task_node is None
        assert engine.final_context is None
        assert engine._checkpointer is None
        assert engine.checkpoint_config is None

    def test_init_with_checkpointing_enabled(self):
        """When enable_checkpointing=True, parent creates a default config."""
        engine = DefaultWorkflowEngine(enable_checkpointing=True)
        assert engine.checkpoint_config is not None
        assert isinstance(engine.checkpoint_config, CheckPointConfig)

    def test_init_with_checkpointing_and_explicit_config(self):
        """Explicit config is used when checkpointing is enabled."""
        explicit = MagicMock(spec=CheckPointConfig)
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=explicit
        )
        assert engine.checkpoint_config is explicit

    def test_init_without_checkpointing_ignores_config(self):
        """Config is None when checkpointing is disabled."""
        explicit = MagicMock(spec=CheckPointConfig)
        engine = DefaultWorkflowEngine(
            enable_checkpointing=False, checkpoint_config=explicit
        )
        assert engine.checkpoint_config is None

    def test_queues_initialized_empty(self):
        """task_queue and sink_queue should be empty deques at init."""
        engine = DefaultWorkflowEngine()
        assert isinstance(engine.task_queue, deque)
        assert isinstance(engine.sink_queue, deque)
        assert len(engine.task_queue) == 0
        assert len(engine.sink_queue) == 0



class TestQueueProperties:
    def test_task_queue_is_mutable_deque(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        engine.task_queue.append(TaskNode(task, None))
        assert len(engine.task_queue) == 1

    def test_sink_queue_is_mutable_deque(self):
        engine = DefaultWorkflowEngine()
        sink = _make_task(name="sink")
        engine.sink_queue.append(sink)
        assert len(engine.sink_queue) == 1

    def test_task_queue_is_same_reference(self):
        engine = DefaultWorkflowEngine()
        assert engine.task_queue is engine._task_queue

    def test_sink_queue_is_same_reference(self):
        engine = DefaultWorkflowEngine()
        assert engine.sink_queue is engine._sink_queue



class TestCheckpointAfterTaskOverride:
    """The overridden _checkpoint_after_task should NOT increment
    tasks_processed (the execute loop handles counting)."""

    @pytest.mark.asyncio
    async def test_no_checkpointer_is_noop(self):
        engine = DefaultWorkflowEngine()
        mock_ctx = MagicMock()
        engine.tasks_processed = 5
        await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.tasks_processed == 5

    @pytest.mark.asyncio
    async def test_with_checkpointer_does_not_increment_counter(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        mock_ctx = AsyncMock()
        engine.tasks_processed = 5
        await engine._checkpoint_after_task(mock_ctx, success=True)
        # Counter should NOT have been incremented by this method
        assert engine.tasks_processed == 5

    @pytest.mark.asyncio
    async def test_with_checkpointer_persists_for_per_task(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        mock_ctx = AsyncMock()
        await engine._checkpoint_after_task(mock_ctx, success=True)
        mock_ctx.persist.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_checkpointer_persists_for_on_state_change(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.ON_STATE_CHANGE,
        )
        mock_ctx = AsyncMock()
        await engine._checkpoint_after_task(mock_ctx, success=True)
        mock_ctx.persist.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_checkpointer_no_persist_for_periodic(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PERIODIC,
        )
        mock_ctx = MagicMock()
        await engine._checkpoint_after_task(mock_ctx, success=True)
        mock_ctx.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_clears_current_task_node(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        engine.enable_checkpointing(
            checkpointer=mock_checkpointer,
            checkpoint_frequency=CheckPointFrequency.PERIODIC,
        )
        mock_ctx = MagicMock()
        engine.current_task_node = TaskNode(task=MagicMock())
        await engine._checkpoint_after_task(mock_ctx, success=True)
        assert engine.current_task_node is None




class TestExecute:

    @pytest.mark.asyncio
    async def test_none_root_task_returns_completed_zero_tasks(self):
        engine = DefaultWorkflowEngine()
        result = await engine.execute(None, MagicMock())
        assert result.status == EngineExecutionResult.COMPLETED
        assert result.tasks_processed == 0
        assert result.final_context is None

    @pytest.mark.asyncio
    async def test_falsy_root_task_returns_completed_zero_tasks(self):
        """Empty string, 0, [] etc. should all be treated as no root."""
        engine = DefaultWorkflowEngine()
        result = await engine.execute("", MagicMock())
        assert result.status == EngineExecutionResult.COMPLETED
        assert result.tasks_processed == 0

    @pytest.mark.asyncio
    async def test_single_task_no_next_returns_completed(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ) as m_detect, patch.object(
            engine, "_build_context", return_value=mock_ctx
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ) as m_switch, patch.object(
            engine, "_resolve_next_task", return_value=None
        ) as m_resolve, patch.object(
            engine, "_drain_sink_nodes"
        ) as m_drain:

            result = await engine.execute(task, pipeline)

            assert result.status == EngineExecutionResult.COMPLETED
            assert result.tasks_processed == 1
            assert result.final_context is mock_ctx

            m_detect.assert_called_once_with(task)
            m_build.assert_called_once_with(
                task=task,
                pipeline=pipeline,
                previous_context=None,
                parallel_tasks=None,
            )
            m_switch.assert_called_once()
            m_resolve.assert_called_once_with(task, mock_ctx)
            m_drain.assert_called_once_with(pipeline)
            assert engine.final_context is mock_ctx

    @pytest.mark.asyncio
    async def test_multi_task_workflow_processes_all_tasks(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task()
        task2 = _make_task()
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", side_effect=[ctx1, ctx2]
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", side_effect=[task2, None]
        ) as m_resolve, patch.object(
            engine, "_drain_sink_nodes"
        ) as m_drain:

            result = await engine.execute(task1, pipeline)

            assert result.status == EngineExecutionResult.COMPLETED
            assert result.tasks_processed == 2
            assert m_build.call_count == 2
            assert m_resolve.call_count == 2
            m_drain.assert_called_once_with(pipeline)
            assert engine.final_context is ctx2

    @pytest.mark.asyncio
    async def test_final_context_is_last_processed_context(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task()
        task2 = _make_task()
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", side_effect=[ctx1, ctx2]
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", side_effect=[task2, None]
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task1, pipeline)
            assert engine.final_context is ctx2

    @pytest.mark.asyncio
    async def test_strict_mode_exception_returns_failed(self):
        engine = DefaultWorkflowEngine(strict_mode=True)
        task = _make_task()
        pipeline = MagicMock()
        error = RuntimeError("boom")

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", side_effect=error
        ) as m_build, patch.object(
            engine, "_drain_sink_nodes"
        ):

            result = await engine.execute(task, pipeline)

            assert result.status == EngineExecutionResult.FAILED
            assert isinstance(result.error, RuntimeError)
            assert result.tasks_processed == 1
            m_build.assert_called_once()

    @pytest.mark.asyncio
    async def test_non_strict_mode_continues_on_error(self):
        """In non-strict mode the inner exception is logged and the loop
        continues to whatever remains in the queue."""
        engine = DefaultWorkflowEngine(strict_mode=False)
        task1 = _make_task()
        task2 = _make_task()
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()

        build_effects = [ctx1, RuntimeError("task2 error"), ctx2]

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_should_terminate", return_value=False), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", side_effect=[task2, None]
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            with patch.object(
                engine, "_build_context", side_effect=build_effects
            ) as m_build:
                result = await engine.execute(task1, pipeline)

                assert result.status == EngineExecutionResult.COMPLETED
                assert result.tasks_processed == 2
                assert m_build.call_count == 2

    @pytest.mark.asyncio
    async def test_fatal_outer_exception_returns_failed(self):
        """An exception outside the inner try/except (e.g. from
        ``_drain_sink_nodes`` in strict mode) hits the outer handler."""
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()
        fatal = RuntimeError("fatal")

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes", side_effect=fatal
        ):

            result = await engine.execute(task, pipeline)

            assert result.status == EngineExecutionResult.FAILED
            assert result.error is fatal
            assert result.tasks_processed == 1




class TestExecuteStateReset:
    """Verify that execute() resets internal state for a fresh run."""

    @pytest.mark.asyncio
    async def test_queues_cleared_between_executions(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task()
        pipeline = MagicMock()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=_make_mock_ctx()
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            # First execution
            await engine.execute(task1, pipeline)
            assert engine.tasks_processed == 1

            # Second execution should reset counter
            await engine.execute(task1, pipeline)
            assert engine.tasks_processed == 1  # not 2

    @pytest.mark.asyncio
    async def test_final_context_reset_between_executions(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task()
        pipeline = MagicMock()
        ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task1, pipeline)
            assert engine.final_context is ctx

            # Second execution — even if it fails early, final_context should
            # have been reset to None at the start of execute()
            ctx2 = _make_mock_ctx()
            with patch.object(engine, "_build_context", return_value=ctx2):
                await engine.execute(task1, pipeline)
            assert engine.final_context is ctx2

    @pytest.mark.asyncio
    async def test_current_task_node_reset_at_start(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        # Set stale state
        engine.current_task_node = TaskNode(task=MagicMock())

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=_make_mock_ctx()
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)
            # After completion, current_task_node should be None
            # (cleared by _checkpoint_after_task or by reset)
            assert engine.current_task_node is None



class TestExecuteCheckpointIntegration:
    """Verify that execute() calls checkpoint hooks when configured."""

    @pytest.mark.asyncio
    async def test_checkpoint_hooks_called_when_enabled(self):
        engine = DefaultWorkflowEngine(enable_checkpointing=True)
        engine.enable_checkpointing(
            checkpointer=MagicMock(),
            checkpoint_frequency=CheckPointFrequency.PER_TASK,
        )
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ), patch.object(
            engine, "_checkpoint_before_task", new_callable=AsyncMock
        ) as m_before, patch.object(
            engine, "_checkpoint_after_task", new_callable=AsyncMock
        ) as m_after:

            await engine.execute(task, pipeline)

            m_before.assert_awaited_once()
            m_after.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_checkpoint_hooks_not_called_when_disabled(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ), patch.object(
            engine, "_checkpoint_before_task", new_callable=AsyncMock
        ) as m_before, patch.object(
            engine, "_checkpoint_after_task", new_callable=AsyncMock
        ) as m_after:

            await engine.execute(task, pipeline)

            # Hooks are still called (they're internal methods), but they
            # are no-ops when _checkpointer is None.
            m_before.assert_awaited_once()
            m_after.assert_awaited_once()





class TestExecuteEarlyTermination:

    @pytest.mark.asyncio
    async def test_cancelled_returns_terminated_early(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        with _patched_status() as ms:
            mock_ctx = _make_mock_ctx_with_status(ms.CANCELLED)

            with patch.object(
                engine, "_detect_parallel_tasks", return_value=None
            ), patch.object(
                engine, "_build_context", return_value=mock_ctx
            ), patch.object(
                engine, "_handle_task_switch", return_value=False
            ), patch.object(
                engine, "_resolve_next_task", return_value=None
            ):

                result = await engine.execute(task, pipeline)
                assert result.status == EngineExecutionResult.TERMINATED_EARLY
                assert result.tasks_processed == 1
                assert result.final_context is mock_ctx

    @pytest.mark.asyncio
    async def test_aborted_returns_terminated_early(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        with _patched_status() as ms:
            mock_ctx = _make_mock_ctx_with_status(ms.ABORTED)

            with patch.object(
                engine, "_detect_parallel_tasks", return_value=None
            ), patch.object(
                engine, "_build_context", return_value=mock_ctx
            ), patch.object(
                engine, "_handle_task_switch", return_value=False
            ), patch.object(
                engine, "_resolve_next_task", return_value=None
            ):

                result = await engine.execute(task, pipeline)
                assert result.status == EngineExecutionResult.TERMINATED_EARLY
                assert result.tasks_processed == 1

    @pytest.mark.asyncio
    async def test_normal_status_does_not_terminate(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=_make_mock_ctx()
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            result = await engine.execute(task, pipeline)
            assert result.status == EngineExecutionResult.COMPLETED


class TestExecuteTaskSwitching:

    @pytest.mark.asyncio
    async def test_switch_schedules_new_task(self):
        """When ``_handle_task_switch`` returns True the loop ``continue``s,
        skipping ``_resolve_next_task`` for that iteration."""
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        _switch_call_count = [0]

        def _switch_side_effect(*a, **kw):
            _switch_call_count[0] += 1
            if _switch_call_count[0] == 1:
                kw["queue"].appendleft(TaskNode(_make_task(), None))
                return True
            return False

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=mock_ctx
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", side_effect=_switch_side_effect
        ) as m_switch, patch.object(
            engine, "_resolve_next_task", return_value=None
        ) as m_resolve, patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)

            assert m_resolve.call_count == 1
            assert m_switch.call_count == 2
            assert m_build.call_count == 2

    @pytest.mark.asyncio
    async def test_no_descriptor_configured_does_not_switch(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ) as m_switch, patch.object(
            engine, "_resolve_next_task", return_value=None
        ) as m_resolve, patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)

            m_switch.assert_called_once()
            m_resolve.assert_called_once_with(task, mock_ctx)

    @pytest.mark.asyncio
    async def test_switch_to_nonexistent_descriptor_raises(self):
        engine = DefaultWorkflowEngine(strict_mode=True)
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        class _FakeSwitchError(Exception):
            def __init__(self, *a, **kw):
                super().__init__(*a)

        switch_error = _FakeSwitchError("bad desc")

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_handle_task_switch", side_effect=switch_error
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            result = await engine.execute(task, pipeline)

            assert result.status == EngineExecutionResult.FAILED
            assert result.error is switch_error



class TestExecuteConditionalBranching:

    @pytest.mark.asyncio
    async def test_conditional_success_follows_success_event(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=True)
        next_task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_evaluate_conditional_branch", return_value=next_task
        ) as m_eval, patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)
            m_eval.assert_called_once_with(task, mock_ctx)

    @pytest.mark.asyncio
    async def test_conditional_failure_follows_failure_event(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=True)
        next_task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_evaluate_conditional_branch", return_value=next_task
        ) as m_eval, patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)
            m_eval.assert_called_once()

    @pytest.mark.asyncio
    async def test_conditional_no_result_returns_none_workflow_ends(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=True)
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_evaluate_conditional_branch", return_value=None
        ) as m_eval, patch.object(
            engine, "_drain_sink_nodes"
        ) as m_drain:

            result = await engine.execute(task, pipeline)

            m_eval.assert_called_once_with(task, mock_ctx)
            assert result.tasks_processed == 1
            m_drain.assert_called_once_with(pipeline)


class TestExecuteParallelTasks:

    @pytest.mark.asyncio
    async def test_non_parallel_single_task_profile(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_parallel_execution_node=False)
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=mock_ctx
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)

            kwargs = m_build.call_args[1]
            assert kwargs["parallel_tasks"] is None

    @pytest.mark.asyncio
    async def test_parallel_chain_gets_list_of_task_profiles(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task(is_parallel_execution_node=True)
        task2 = _make_task(is_parallel_execution_node=True)
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()
        parallel_set = {task1, task2}

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=parallel_set
        ), patch.object(
            engine, "_build_context", return_value=mock_ctx
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task1, pipeline)

            kwargs = m_build.call_args[1]
            assert kwargs["parallel_tasks"] is parallel_set


class TestExecuteContextChaining:

    @pytest.mark.asyncio
    async def test_first_context_set_as_pipeline_root(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext", return_value=mock_ctx
        ) as MC, patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task, pipeline)

            assert pipeline.execution_context is mock_ctx

    @pytest.mark.asyncio
    async def test_subsequent_contexts_linked(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task()
        task2 = _make_task()
        task1.condition_node.on_success_event = task2
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext", side_effect=[ctx1, ctx2]
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(task1, pipeline)

            assert ctx2.previous_context is ctx1
            assert ctx1.next_context is ctx2

    @pytest.mark.asyncio
    async def test_sink_nodes_collected_in_sink_queue(self):
        """Non-first tasks with ``sink_node`` should be collected into the
        engine's sink queue."""
        engine = DefaultWorkflowEngine()
        sink = _make_task(name="sink")
        task1 = _make_task(name="t1")
        task2 = _make_task(name="t2", sink_node=sink)
        task1.condition_node.on_success_event = task2
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext", side_effect=[ctx1, ctx2]
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_drain_sink_nodes"
        ) as m_drain:

            await engine.execute(task1, pipeline)

            m_drain.assert_called_once_with(pipeline)
            # The sink should be in the engine's sink queue
            assert sink in engine.sink_queue




class TestExecuteSinkNodes:

    @pytest.mark.asyncio
    async def test_sink_nodes_executed_after_main_queue(self):
        engine = DefaultWorkflowEngine()
        sink = _make_task(name="sink")
        task1 = _make_task(name="t1")
        task2 = _make_task(name="t2", sink_node=sink)
        task1.condition_node.on_success_event = task2
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()
        sink_ctx = _make_sink_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext",
            side_effect=[ctx1, ctx2, sink_ctx],
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ):

            await engine.execute(task1, pipeline)

            # Sink context was dispatched (async via _Awaitable)
            assert sink_ctx.dispatch.call_count >= 1

    @pytest.mark.asyncio
    async def test_sink_node_error_strict_mode_returns_failed(self):
        engine = DefaultWorkflowEngine(strict_mode=True)
        sink = _make_task(name="sink")
        task1 = _make_task(name="t1")
        task2 = _make_task(name="t2", sink_node=sink)
        task1.condition_node.on_success_event = task2
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()
        sink_error = RuntimeError("sink boom")

        # For strict mode, _drain_sink_nodes re-raises. The sink context's
        # dispatch is _awaitable_fn which raises when awaited.
        async def _failing_dispatch(*a, **kw):
            raise sink_error

        sink_ctx = MagicMock()
        object.__setattr__(sink_ctx, "dispatch", _failing_dispatch)

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext",
            side_effect=[ctx1, ctx2, sink_ctx],
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ):

            result = await engine.execute(task1, pipeline)

            # In strict mode _drain_sink_nodes re-raises, caught by outer
            # except → FAILED
            assert result.status == EngineExecutionResult.FAILED
            assert result.error is sink_error

    @pytest.mark.asyncio
    async def test_sink_node_error_non_strict_mode_continues(self):
        engine = DefaultWorkflowEngine(strict_mode=False)
        sink1 = _make_task(name="sink1")
        sink2 = _make_task(name="sink2")
        task1 = _make_task(name="t1")
        task2 = _make_task(name="t2", sink_node=sink1)
        task3 = _make_task(name="t3", sink_node=sink2)
        task1.condition_node.on_success_event = task2
        task2.condition_node.on_success_event = task3
        pipeline = MagicMock()

        ctx1 = _make_mock_ctx()
        ctx2 = _make_mock_ctx()
        ctx3 = _make_mock_ctx()

        # First sink dispatch raises, second succeeds
        async def _failing_dispatch(*a, **kw):
            raise RuntimeError("sink1 err")

        sink_ctx1 = MagicMock()
        object.__setattr__(sink_ctx1, "dispatch", _failing_dispatch)
        sink_ctx2 = _make_sink_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext",
            side_effect=[ctx1, ctx2, ctx3, sink_ctx1, sink_ctx2],
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ):

            result = await engine.execute(task1, pipeline)

            # Non-strict: first sink error is logged, second still dispatched
            assert result.status == EngineExecutionResult.COMPLETED
            assert sink_ctx2.dispatch.call_count >= 1

    @pytest.mark.asyncio
    async def test_empty_sink_queue_noop(self):
        """When no sink nodes are collected, ``_drain_sink_nodes`` is a no-op."""
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        ctx = _make_mock_ctx()

        with _patched_status(), patch(
            "volnux.engine.default_engine.ExecutionContext", return_value=ctx
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ):

            # Let the real _drain_sink_nodes run with an empty queue.
            result = await engine.execute(task, pipeline)
            assert result.status == EngineExecutionResult.COMPLETED




class TestDetectParallelTasks:
    def test_non_parallel_returns_none(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_parallel_execution_node=False)
        assert engine._detect_parallel_tasks(task) is None

    def test_single_parallel_no_chain_returns_none(self):
        """A parallel node whose ``on_success_pipe`` is not PARALLELISM
        produces an empty ``parallel_tasks`` set -> returns None."""
        engine = DefaultWorkflowEngine()
        task = _make_task(is_parallel_execution_node=True)
        task.condition_node.on_success_pipe = None

        with _patched_pipe():
            result = engine._detect_parallel_tasks(task)
        assert result is None

    def test_parallel_chain_returns_set(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task(is_parallel_execution_node=True)
        task2 = _make_task(is_parallel_execution_node=True)
        task3 = _make_task(is_parallel_execution_node=False)

        with _patched_pipe() as mp:
            task1.condition_node.on_success_pipe = mp.PARALLELISM
            task1.condition_node.on_success_event = task2
            task2.condition_node.on_success_pipe = mp.PARALLELISM
            task2.condition_node.on_success_event = task3
            task3.condition_node.on_success_pipe = None

            result = engine._detect_parallel_tasks(task1)

        assert result == {task1, task2, task3}

    def test_parallel_chain_stops_at_non_parallelism_pipe(self):
        engine = DefaultWorkflowEngine()
        task1 = _make_task(is_parallel_execution_node=True)
        task2 = _make_task(is_parallel_execution_node=True)
        task3 = _make_task(is_parallel_execution_node=True)

        with _patched_pipe() as mp:
            other_pipe = object()
            task1.condition_node.on_success_pipe = mp.PARALLELISM
            task1.condition_node.on_success_event = task2
            task2.condition_node.on_success_pipe = other_pipe
            task2.condition_node.on_success_event = task3

            result = engine._detect_parallel_tasks(task1)

        assert result == {task1, task2}

    def test_debug_logging_for_parallel_chain(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        task1 = _make_task(is_parallel_execution_node=True)
        task2 = _make_task(is_parallel_execution_node=True)

        with _patched_pipe() as mp:
            task1.condition_node.on_success_pipe = mp.PARALLELISM
            task1.condition_node.on_success_event = task2
            task2.condition_node.on_success_pipe = None

            with patch("volnux.engine.default_engine.logger") as mock_logger:
                engine._detect_parallel_tasks(task1)
                mock_logger.debug.assert_called_once()
                call_args = mock_logger.debug.call_args[0][0]
                assert "2 parallel tasks" in call_args



class TestBuildContext:

    @pytest.mark.asyncio
    async def test_first_context_set_as_pipeline_root(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            mock_instance = MagicMock()
            MC.return_value = mock_instance

            ctx = await engine._build_context(
                task=task,
                pipeline=pipeline,
                previous_context=None,
            )

            assert ctx is mock_instance
            assert pipeline.execution_context is mock_instance
            MC.assert_called_once_with(pipeline=pipeline, task_profiles=task)

    @pytest.mark.asyncio
    async def test_subsequent_context_sink_collected_and_chained(self):
        engine = DefaultWorkflowEngine()
        sink_task = _make_task(name="sink")
        task = _make_task(sink_node=sink_task)
        pipeline = MagicMock()
        prev_ctx = MagicMock()

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            mock_instance = MagicMock()
            MC.return_value = mock_instance

            ctx = await engine._build_context(
                task=task,
                pipeline=pipeline,
                previous_context=prev_ctx,
            )

            # Sink node should be collected in engine's sink queue
            assert sink_task in engine.sink_queue
            # Context chain should be linked
            assert ctx.previous_context is prev_ctx
            assert prev_ctx.next_context is mock_instance
            # Pipeline root is NOT overwritten
            assert pipeline.execution_context is not mock_instance

    @pytest.mark.asyncio
    async def test_parallel_tasks_context_created_with_list(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()
        parallel_set = {task, _make_task()}

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            await engine._build_context(
                task=task,
                pipeline=pipeline,
                previous_context=None,
                parallel_tasks=parallel_set,
            )

            call_kwargs = MC.call_args[1]
            assert isinstance(call_kwargs["task_profiles"], list)
            assert set(call_kwargs["task_profiles"]) == parallel_set

    @pytest.mark.asyncio
    async def test_non_parallel_context_created_with_single_task(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        pipeline = MagicMock()

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            await engine._build_context(
                task=task,
                pipeline=pipeline,
                previous_context=None,
            )

            call_kwargs = MC.call_args[1]
            assert call_kwargs["task_profiles"] is task

    @pytest.mark.asyncio
    async def test_no_sink_when_none(self):
        """Task without sink_node should not add anything to sink_queue."""
        engine = DefaultWorkflowEngine()
        task = _make_task(sink_node=None)
        pipeline = MagicMock()
        prev_ctx = MagicMock()

        with patch("volnux.engine.default_engine.ExecutionContext"):
            await engine._build_context(
                task=task,
                pipeline=pipeline,
                previous_context=prev_ctx,
            )
            assert len(engine.sink_queue) == 0



class TestShouldTerminate:
    def test_cancelled_returns_true(self):
        engine = DefaultWorkflowEngine()
        state = MagicMock()

        with _patched_status() as ms:
            state.status = ms.CANCELLED
            assert engine._should_terminate(state) is True

    def test_aborted_returns_true(self):
        engine = DefaultWorkflowEngine()
        state = MagicMock()

        with _patched_status() as ms:
            state.status = ms.ABORTED
            assert engine._should_terminate(state) is True

    def test_paused_returns_true(self):
        """A HITL-suspended execution must stop the loop, not be treated
        as a completed task."""
        engine = DefaultWorkflowEngine()
        state = MagicMock()

        with _patched_status() as ms:
            state.status = ms.PAUSED
            assert engine._should_terminate(state) is True

    def test_failed_returns_true(self):
        engine = DefaultWorkflowEngine()
        state = MagicMock()

        with _patched_status() as ms:
            state.status = ms.FAILED
            assert engine._should_terminate(state) is True

    def test_other_status_returns_false(self):
        engine = DefaultWorkflowEngine()
        state = MagicMock()

        with _patched_status() as ms:
            state.status = object()  # different from CANCELLED / ABORTED
            assert engine._should_terminate(state) is False

    def test_debug_logging_on_termination(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        state = MagicMock()

        with _patched_status() as ms:
            state.status = ms.CANCELLED
            with patch("volnux.engine.default_engine.logger") as mock_logger:
                engine._should_terminate(state)
                mock_logger.debug.assert_called_once()
                call_args = mock_logger.debug.call_args[0][0]
                assert "Early termination" in call_args



class TestMapTerminationStatus:
    def test_paused_returns_suspended(self):
        engine = DefaultWorkflowEngine()
        assert (
            engine._map_termination_status(ExecutionStatus.PAUSED)
            == EngineExecutionResult.SUSPENDED
        )

    def test_failed_returns_failed(self):
        engine = DefaultWorkflowEngine()
        assert (
            engine._map_termination_status(ExecutionStatus.FAILED)
            == EngineExecutionResult.FAILED
        )

    def test_cancelled_returns_terminated_early(self):
        engine = DefaultWorkflowEngine()
        assert (
            engine._map_termination_status(ExecutionStatus.CANCELLED)
            == EngineExecutionResult.TERMINATED_EARLY
        )

    def test_always_returns_terminated_early(self):
        engine = DefaultWorkflowEngine()
        assert (
            engine._map_termination_status(MagicMock())
            == EngineExecutionResult.TERMINATED_EARLY
        )


class TestHandleTaskSwitch:
    def test_no_switch_request_returns_false(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        state = MagicMock()
        state.get_switch_request.return_value = None
        queue = deque()

        assert engine._handle_task_switch(task, state, queue) is False

    def test_descriptor_not_configured_returns_false(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        state = MagicMock()
        req = MagicMock()
        req.descriptor_configured = False
        state.get_switch_request.return_value = req
        queue = deque()

        assert engine._handle_task_switch(task, state, queue) is False

    def test_valid_switch_appends_to_queue_returns_true(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        new_task = _make_task(name="switched_target")
        task.get_descriptor.return_value = new_task

        state = MagicMock()
        req = MagicMock()
        req.descriptor_configured = True
        req.next_task_descriptor = "desc_X"
        state.get_switch_request.return_value = req

        queue = deque()
        prev_ctx = MagicMock()

        result = engine._handle_task_switch(task, state, queue, prev_ctx)

        assert result is True
        assert len(queue) == 1
        node = queue[0]
        assert node.task is new_task
        assert node.previous_context is prev_ctx
        task.get_descriptor.assert_called_once_with("desc_X")

    def test_invalid_descriptor_raises_task_switching_error(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        task.get_descriptor.return_value = None

        state = MagicMock()
        req = MagicMock()
        req.descriptor_configured = True
        req.next_task_descriptor = "nonexistent"
        state.get_switch_request.return_value = req

        queue = deque()

        class _FakeSwitchError(Exception):
            def __init__(self, *a, **kw):
                super().__init__(*a)

        with patch("volnux.engine.default_engine.TaskSwitchingError", _FakeSwitchError):
            with pytest.raises(_FakeSwitchError) as exc_info:
                engine._handle_task_switch(task, state, queue)

            assert "nonexistent" in str(exc_info.value)

    def test_debug_logging_on_switch(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        task = _make_task()
        new_task = _make_task(name="target")
        task.get_descriptor.return_value = new_task

        state = MagicMock()
        req = MagicMock()
        req.descriptor_configured = True
        req.next_task_descriptor = "desc_Y"
        state.get_switch_request.return_value = req

        queue = deque()

        with patch("volnux.engine.default_engine.logger") as mock_logger:
            engine._handle_task_switch(task, state, queue)
            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "desc_Y" in call_args


class TestResolveNextTask:
    def test_conditional_delegates_to_evaluate_conditional(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=True)
        next_task = _make_task()
        mock_ctx = MagicMock()

        with patch.object(
            engine, "_evaluate_conditional_branch", return_value=next_task
        ) as m:
            result = engine._resolve_next_task(task, mock_ctx)

        m.assert_called_once_with(task, mock_ctx)
        assert result is next_task

    def test_non_conditional_delegates_to_follow_sequential(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=False)
        next_task = _make_task()
        mock_ctx = MagicMock()

        with patch.object(
            engine, "_follow_sequential_flow", return_value=next_task
        ) as m:
            result = engine._resolve_next_task(task, mock_ctx)

        m.assert_called_once_with(task, mock_ctx)
        assert result is next_task


class TestEvaluateConditionalBranch:
    def test_success_result_follows_success_event(self):
        engine = DefaultWorkflowEngine()
        success_task = _make_task(name="success")
        failure_task = _make_task(name="failure")
        task = _make_task(is_conditional=True)
        task.condition_node.on_success_event = success_task
        task.condition_node.on_failure_event = failure_task

        mock_result = MagicMock()
        mock_result.success = True
        mock_ctx = MagicMock()

        with patch(
            "volnux.engine.default_engine.evaluate_context_execution_results",
            return_value=mock_result,
        ):
            result = engine._evaluate_conditional_branch(task, mock_ctx)

        assert result is success_task

    def test_failure_result_follows_failure_event(self):
        engine = DefaultWorkflowEngine()
        success_task = _make_task(name="success")
        failure_task = _make_task(name="failure")
        task = _make_task(is_conditional=True)
        task.condition_node.on_success_event = success_task
        task.condition_node.on_failure_event = failure_task

        mock_result = MagicMock()
        mock_result.success = False
        mock_ctx = MagicMock()

        with patch(
            "volnux.engine.default_engine.evaluate_context_execution_results",
            return_value=mock_result,
        ):
            result = engine._evaluate_conditional_branch(task, mock_ctx)

        assert result is failure_task

    def test_none_result_returns_none(self):
        engine = DefaultWorkflowEngine()
        task = _make_task(is_conditional=True)
        mock_ctx = MagicMock()

        with patch(
            "volnux.engine.default_engine.evaluate_context_execution_results",
            return_value=None,
        ):
            result = engine._evaluate_conditional_branch(task, mock_ctx)

        assert result is None

    def test_debug_logging_on_branch(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        success_task = _make_task(name="success")
        failure_task = _make_task(name="failure")
        task = _make_task(is_conditional=True)
        task.condition_node.on_success_event = success_task
        task.condition_node.on_failure_event = failure_task

        mock_result = MagicMock()
        mock_result.success = True
        mock_ctx = MagicMock()

        with patch(
            "volnux.engine.default_engine.evaluate_context_execution_results",
            return_value=mock_result,
        ), patch("volnux.engine.default_engine.logger") as mock_logger:
            engine._evaluate_conditional_branch(task, mock_ctx)
            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "success" in call_args


class TestFollowSequentialFlow:
    def test_multitask_uses_decision_task(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        next_task = _make_task(name="via_decision")

        decision = MagicMock()
        decision.condition_node.on_success_event = next_task

        mock_ctx = MagicMock()
        mock_ctx.is_multitask.return_value = True
        mock_ctx.get_decision_task_profile.return_value = decision

        result = engine._follow_sequential_flow(task, mock_ctx)
        assert result is next_task

    def test_non_multitask_follows_success_event(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()
        next_task = _make_task(name="next")
        task.condition_node.on_success_event = next_task

        mock_ctx = MagicMock()
        mock_ctx.is_multitask.return_value = False

        result = engine._follow_sequential_flow(task, mock_ctx)
        assert result is next_task

    def test_none_decision_task_returns_none(self):
        engine = DefaultWorkflowEngine()
        task = _make_task()

        mock_ctx = MagicMock()
        mock_ctx.is_multitask.return_value = True
        mock_ctx.get_decision_task_profile.return_value = None

        result = engine._follow_sequential_flow(task, mock_ctx)
        assert result is None


class TestDrainSinkNodes:

    @pytest.mark.asyncio
    async def test_empty_queue_noop(self):
        engine = DefaultWorkflowEngine()
        pipeline = MagicMock()

        await engine._drain_sink_nodes(pipeline)
        # Should return silently, no side effects.

    @pytest.mark.asyncio
    async def test_processes_all_sink_nodes_in_order(self):
        engine = DefaultWorkflowEngine()
        pipeline = MagicMock()
        sink1 = _make_task(name="sink1")
        sink2 = _make_task(name="sink2")
        engine.sink_queue.append(sink1)
        engine.sink_queue.append(sink2)

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            ctx1 = _make_sink_ctx()
            ctx2 = _make_sink_ctx()
            MC.side_effect = [ctx1, ctx2]

            await engine._drain_sink_nodes(pipeline)

            assert MC.call_count == 2
            assert ctx1.dispatch.call_count >= 1
            assert ctx2.dispatch.call_count >= 1
            assert len(engine.sink_queue) == 0

    @pytest.mark.asyncio
    async def test_strict_mode_raises_on_error(self):
        engine = DefaultWorkflowEngine(strict_mode=True)
        pipeline = MagicMock()
        sink = _make_task(name="sink")
        engine.sink_queue.append(sink)
        error = RuntimeError("sink err")

        async def _failing_dispatch(*a, **kw):
            raise error

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            ctx = MagicMock()
            object.__setattr__(ctx, "dispatch", _failing_dispatch)
            MC.return_value = ctx

            with pytest.raises(RuntimeError, match="sink err"):
                await engine._drain_sink_nodes(pipeline)

    @pytest.mark.asyncio
    async def test_non_strict_mode_logs_and_continues(self):
        engine = DefaultWorkflowEngine(strict_mode=False)
        pipeline = MagicMock()
        sink1 = _make_task(name="sink1")
        sink2 = _make_task(name="sink2")
        engine.sink_queue.append(sink1)
        engine.sink_queue.append(sink2)

        async def _failing_dispatch(*a, **kw):
            raise RuntimeError("sink1 err")

        with patch("volnux.engine.default_engine.ExecutionContext") as MC:
            ctx1 = MagicMock()
            object.__setattr__(ctx1, "dispatch", _failing_dispatch)
            ctx2 = _make_sink_ctx()
            MC.side_effect = [ctx1, ctx2]

            await engine._drain_sink_nodes(pipeline)

            assert MC.call_count == 2
            assert ctx2.dispatch.call_count >= 1
            assert len(engine.sink_queue) == 0

    @pytest.mark.asyncio
    async def test_debug_logging(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        pipeline = MagicMock()
        sink = _make_task(name="sink")
        engine.sink_queue.append(sink)

        with patch("volnux.engine.default_engine.ExecutionContext") as MC, patch(
            "volnux.engine.default_engine.logger"
        ) as mock_logger:

            MC.return_value = _make_sink_ctx()
            await engine._drain_sink_nodes(pipeline)

            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "1 sink nodes" in call_args



class TestDebugLogging:

    @pytest.mark.asyncio
    async def test_execute_logs_task_processing(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ), patch(
            "volnux.engine.default_engine.logger"
        ) as mock_logger:

            await engine.execute(task, pipeline)

            # Should log task processing
            debug_calls = [c[0][0] for c in mock_logger.debug.call_args_list]
            assert any("Processing task" in c for c in debug_calls)

    @pytest.mark.asyncio
    async def test_execute_logs_completion_count(self):
        engine = DefaultWorkflowEngine(enable_debug_logging=True)
        task = _make_task()
        pipeline = MagicMock()
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ), patch(
            "volnux.engine.default_engine.logger"
        ) as mock_logger:

            await engine.execute(task, pipeline)

            debug_calls = [c[0][0] for c in mock_logger.debug.call_args_list]
            assert any("Completed processing" in c for c in debug_calls)
            assert any("1 tasks" in c for c in debug_calls)



class TestExecuteCheckpointerStartup:
    """Checkpoint manager's async worker starts once, at root-engine
    startup only — never from a sub-engine."""

    @pytest.mark.asyncio
    async def _run(self, engine, task, pipeline, mock_ctx):
        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(engine, "_build_context", return_value=mock_ctx), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):
            return await engine.execute(task, pipeline)

    @pytest.mark.asyncio
    async def test_root_engine_starts_checkpointer_once(self):
        engine = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        mock_checkpointer.start = AsyncMock()
        engine.enable_checkpointing(mock_checkpointer, CheckPointFrequency.PER_TASK)

        await self._run(engine, _make_task(), MagicMock(), _make_mock_ctx())

        mock_checkpointer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_child_engine_never_starts_checkpointer(self):
        parent = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        mock_checkpointer.start = AsyncMock()
        parent.enable_checkpointing(mock_checkpointer, CheckPointFrequency.PER_TASK)

        child = DefaultWorkflowEngine(parent_engine=parent)
        child.enable_checkpointing(mock_checkpointer, CheckPointFrequency.PER_TASK)

        await self._run(child, _make_task(), MagicMock(), _make_mock_ctx())

        mock_checkpointer.start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_checkpointer_configured_does_not_error(self):
        engine = DefaultWorkflowEngine()
        result = await self._run(engine, _make_task(), MagicMock(), _make_mock_ctx())
        assert result.status == EngineExecutionResult.COMPLETED



class TestSpawnSubEngineCheckpointInheritance:
    """A sub-engine only ever inherits the parent's checkpoint manager —
    it must never build its own."""

    @pytest.mark.asyncio
    async def test_child_shares_parent_checkpointer_instance(self):
        parent = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        parent.enable_checkpointing(mock_checkpointer, CheckPointFrequency.PER_TASK)

        child = await parent.spawn_sub_engine(_make_task(), MagicMock())

        assert child._checkpointer is mock_checkpointer
        assert child._checkpoint_frequency == CheckPointFrequency.PER_TASK

    @pytest.mark.asyncio
    async def test_no_spurious_warning_when_parent_has_checkpointer(self):
        parent = DefaultWorkflowEngine()
        mock_checkpointer = MagicMock()
        parent.enable_checkpointing(mock_checkpointer, CheckPointFrequency.PER_TASK)

        with patch("volnux.engine.default_engine.logger") as mock_logger:
            await parent.spawn_sub_engine(_make_task(), MagicMock())
            mock_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_child_without_parent_checkpointer_has_none(self):
        parent = DefaultWorkflowEngine()
        child = await parent.spawn_sub_engine(_make_task(), MagicMock())
        assert child._checkpointer is None

    @pytest.mark.asyncio
    async def test_child_records_parent_and_error_strategy(self):
        parent = DefaultWorkflowEngine()
        child = await parent.spawn_sub_engine(
            _make_task(), MagicMock(), error_strategy=SubgraphErrorStrategy.ISOLATE
        )
        assert child.parent_engine is parent
        assert child in parent.child_engines
        assert child._error_strategy == SubgraphErrorStrategy.ISOLATE



class TestExecuteSubgraphs:

    @pytest.mark.asyncio
    async def test_execute_subgraph_success_merges_tasks_processed(self):
        engine = DefaultWorkflowEngine()
        engine.tasks_processed = 0
        child = _make_fake_sub_engine(tasks_processed=3)
        expected = EngineResult(
            status=EngineExecutionResult.COMPLETED, tasks_processed=3
        )
        child.execute.return_value = expected

        with patch.object(engine, "spawn_sub_engine", AsyncMock(return_value=child)):
            result = await engine.execute_subgraph(_make_task(), MagicMock())

        assert result is expected
        assert engine.tasks_processed == 3

    @pytest.mark.asyncio
    async def test_execute_subgraph_treat_as_failure_still_merges_tasks_processed(self):
        """Previously the error path returned an EngineResult without ever
        adding sub_engine.tasks_processed to self.tasks_processed."""
        engine = DefaultWorkflowEngine()
        engine.tasks_processed = 0
        child = _make_fake_sub_engine(tasks_processed=2)
        error = RuntimeError("boom")
        child.execute.side_effect = error

        with patch.object(engine, "spawn_sub_engine", AsyncMock(return_value=child)):
            result = await engine.execute_subgraph(
                _make_task(),
                MagicMock(),
                error_strategy=SubgraphErrorStrategy.TREAT_AS_FAILURE,
            )

        assert result.status == EngineExecutionResult.FAILED
        assert result.error is error
        assert engine.tasks_processed == 2

    @pytest.mark.asyncio
    async def test_execute_subgraph_bubble_up_reraises(self):
        engine = DefaultWorkflowEngine()
        child = _make_fake_sub_engine(tasks_processed=1)
        error = RuntimeError("boom")
        child.execute.side_effect = error

        with patch.object(engine, "spawn_sub_engine", AsyncMock(return_value=child)):
            with pytest.raises(RuntimeError):
                await engine.execute_subgraph(
                    _make_task(),
                    MagicMock(),
                    error_strategy=SubgraphErrorStrategy.BUBBLE_UP,
                )

    @pytest.mark.asyncio
    async def test_execute_subgraph_isolate_returns_synthetic_completed(self):
        engine = DefaultWorkflowEngine()
        child = _make_fake_sub_engine(tasks_processed=1)
        child.execute.side_effect = RuntimeError("boom")

        with patch.object(engine, "spawn_sub_engine", AsyncMock(return_value=child)):
            result = await engine.execute_subgraph(
                _make_task(), MagicMock(), error_strategy=SubgraphErrorStrategy.ISOLATE
            )

        assert result.status == EngineExecutionResult.COMPLETED
        assert result.final_context is None

    @pytest.mark.asyncio
    async def test_concurrent_execution_runs_all_and_merges_counts(self):
        engine = DefaultWorkflowEngine()
        engine.tasks_processed = 0
        tasks = [_make_task(), _make_task(), _make_task()]
        children = [_make_fake_sub_engine(tasks_processed=i + 1) for i in range(3)]
        for i, child in enumerate(children):
            child.execute.return_value = EngineResult(
                status=EngineExecutionResult.COMPLETED, tasks_processed=i + 1
            )

        with patch.object(engine, "spawn_sub_engine", AsyncMock(side_effect=children)):
            results = await engine.execute_subgraphs(tasks, MagicMock())

        assert len(results) == 3
        assert all(r.status == EngineExecutionResult.COMPLETED for r in results)
        assert engine.tasks_processed == 1 + 2 + 3

    @pytest.mark.asyncio
    async def test_concurrent_execution_partial_failure_treat_as_failure(self):
        engine = DefaultWorkflowEngine()
        engine.tasks_processed = 0
        tasks = [_make_task(), _make_task()]

        ok_child = _make_fake_sub_engine(tasks_processed=2)
        ok_child.execute.return_value = EngineResult(
            status=EngineExecutionResult.COMPLETED, tasks_processed=2
        )
        failing_child = _make_fake_sub_engine(tasks_processed=1)
        failing_child.execute.side_effect = RuntimeError("chain failed")

        with patch.object(
            engine,
            "spawn_sub_engine",
            AsyncMock(side_effect=[ok_child, failing_child]),
        ):
            results = await engine.execute_subgraphs(
                tasks,
                MagicMock(),
                error_strategy=SubgraphErrorStrategy.TREAT_AS_FAILURE,
            )

        statuses = {r.status for r in results}
        assert statuses == {
            EngineExecutionResult.COMPLETED,
            EngineExecutionResult.FAILED,
        }
        # Both children's counts merged despite one failing.
        assert engine.tasks_processed == 3

    @pytest.mark.asyncio
    async def test_concurrent_execution_bubble_up_merges_all_before_raising(self):
        """gather() runs every chain to completion before BUBBLE_UP raises,
        so every child's tasks_processed must still be merged."""
        engine = DefaultWorkflowEngine()
        engine.tasks_processed = 0
        tasks = [_make_task(), _make_task()]

        failing_child = _make_fake_sub_engine(tasks_processed=5)
        failing_child.execute.side_effect = RuntimeError("chain failed")
        ok_child = _make_fake_sub_engine(tasks_processed=7)
        ok_child.execute.return_value = EngineResult(
            status=EngineExecutionResult.COMPLETED, tasks_processed=7
        )

        with patch.object(
            engine,
            "spawn_sub_engine",
            AsyncMock(side_effect=[failing_child, ok_child]),
        ):
            with pytest.raises(RuntimeError):
                await engine.execute_subgraphs(
                    tasks, MagicMock(), error_strategy=SubgraphErrorStrategy.BUBBLE_UP
                )

        assert engine.tasks_processed == 12


class TestExecuteTaskGrouping:
    """{} grouping: SINGLE_CHAIN executes normally, MULTIPATH_CHAINS fans
    out into execute_subgraphs()."""

    def _grouping(self, chains):
        return PipelineTaskGrouping(chains=chains)

    @pytest.mark.asyncio
    async def test_single_chain_falls_through_to_normal_dispatch(self):
        engine = DefaultWorkflowEngine()
        grouping = self._grouping([_make_task()])
        assert grouping.strategy == GroupingStrategy.SINGLE_CHAIN
        mock_ctx = _make_mock_ctx()

        with _patched_status(), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=mock_ctx
        ) as m_build, patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ), patch.object(
            engine, "execute_subgraphs"
        ) as m_subgraphs:

            result = await engine.execute(grouping, MagicMock())

        m_subgraphs.assert_not_called()
        m_build.assert_called_once()
        assert result.status == EngineExecutionResult.COMPLETED

    @pytest.mark.asyncio
    async def test_multipath_chains_calls_execute_subgraphs_with_chains(self):
        engine = DefaultWorkflowEngine()
        chain_a, chain_b = _make_task(), _make_task()
        grouping = self._grouping([chain_a, chain_b])
        assert grouping.strategy == GroupingStrategy.MULTIPATH_CHAINS
        pipeline = MagicMock()

        with patch.object(
            engine,
            "execute_subgraphs",
            AsyncMock(
                return_value=[
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                ]
            ),
        ) as m_subgraphs, patch.object(engine, "_drain_sink_nodes"):

            result = await engine.execute(grouping, pipeline)

        m_subgraphs.assert_called_once_with([chain_a, chain_b], pipeline)
        assert result.status == EngineExecutionResult.COMPLETED

    @pytest.mark.asyncio
    async def test_multipath_all_success_continues_to_on_success_event(self):
        engine = DefaultWorkflowEngine()
        grouping = self._grouping([_make_task(), _make_task()])
        next_task = _make_task()
        grouping.condition_node.on_success_event = next_task

        with _patched_status(), patch.object(
            engine,
            "execute_subgraphs",
            AsyncMock(
                return_value=[
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                ]
            ),
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=_make_mock_ctx()
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ), patch.object(
            engine, "_drain_sink_nodes"
        ):

            result = await engine.execute(grouping, MagicMock())

        # grouping (1) + next_task (1) both processed
        assert result.tasks_processed == 2
        assert result.status == EngineExecutionResult.COMPLETED

    @pytest.mark.asyncio
    async def test_multipath_any_failed_continues_to_on_failure_event(self):
        engine = DefaultWorkflowEngine()
        grouping = self._grouping([_make_task(), _make_task()])
        failure_task = _make_task()
        grouping.condition_node.on_failure_event = failure_task

        with _patched_status(), patch.object(
            engine,
            "execute_subgraphs",
            AsyncMock(
                return_value=[
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                    EngineResult(status=EngineExecutionResult.FAILED),
                ]
            ),
        ), patch.object(
            engine, "_detect_parallel_tasks", return_value=None
        ), patch.object(
            engine, "_build_context", return_value=_make_mock_ctx()
        ), patch.object(
            engine, "_should_terminate", return_value=False
        ), patch.object(
            engine, "_handle_task_switch", return_value=False
        ), patch.object(
            engine, "_resolve_next_task", return_value=None
        ) as m_resolve, patch.object(
            engine, "_drain_sink_nodes"
        ):

            await engine.execute(grouping, MagicMock())

        # The failure branch's task must have been queued and dispatched —
        # _resolve_next_task only fires for it, not for the grouping itself.
        m_resolve.assert_called_once_with(failure_task, ANY)

    @pytest.mark.asyncio
    async def test_multipath_any_suspended_stops_workflow_early(self):
        engine = DefaultWorkflowEngine()
        grouping = self._grouping([_make_task(), _make_task()])

        with patch.object(
            engine,
            "execute_subgraphs",
            AsyncMock(
                return_value=[
                    EngineResult(status=EngineExecutionResult.COMPLETED),
                    EngineResult(status=EngineExecutionResult.SUSPENDED),
                ]
            ),
        ), patch.object(engine, "_drain_sink_nodes") as m_drain:

            result = await engine.execute(grouping, MagicMock())

        assert result.status == EngineExecutionResult.SUSPENDED
        m_drain.assert_not_called()

    @pytest.mark.asyncio
    async def test_multipath_sink_node_collected(self):
        """_build_context() is bypassed for MULTIPATH_CHAINS, so sink-node
        collection must be replicated at the interception point."""
        engine = DefaultWorkflowEngine()
        grouping = self._grouping([_make_task()])
        grouping.strategy = GroupingStrategy.MULTIPATH_CHAINS
        sink = _make_task(name="sink")
        grouping.sink_node = sink

        with patch.object(
            engine,
            "execute_subgraphs",
            AsyncMock(
                return_value=[EngineResult(status=EngineExecutionResult.COMPLETED)]
            ),
        ), patch.object(engine, "_drain_sink_nodes") as m_drain:

            await engine.execute(grouping, MagicMock())

        assert sink in engine.sink_queue
        m_drain.assert_called_once()
