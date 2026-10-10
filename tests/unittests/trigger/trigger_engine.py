import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from volnux.engine.workflows.trigger.engine import (
    TriggerEngine,
    WorkflowConfigExecutor,
    BaseWorkflowConfigExecutor,
)
from volnux.engine.workflows.workflow import WorkflowExecutionError  # WorkflowNotfound
from volnux.engine.workflows.trigger.triggers.base import (
    TriggerBase,
    TriggerActivation,
    TriggerType,
)


def make_trigger(trigger_id: str = "t1", workflow_name: str = "wf1") -> MagicMock:
    trigger = MagicMock(spec=TriggerBase)
    trigger.trigger_id = trigger_id
    trigger.workflow_name = workflow_name
    trigger.lifecycle = None
    trigger.error_count = 0
    trigger.start = AsyncMock()
    trigger.stop = AsyncMock()
    return trigger


def make_executor(registry: MagicMock = None) -> MagicMock:
    executor = AsyncMock(spec=BaseWorkflowConfigExecutor)
    executor._workflow_registry = registry or make_registry()
    executor.get_workflow_registry = MagicMock(return_value=executor._workflow_registry)
    return executor


def make_registry(workflow_names: list = None) -> MagicMock:
    registry = MagicMock()
    registry.is_ready.return_value = True

    def get_config(name):
        if workflow_names is None or name in workflow_names:
            cfg = MagicMock()
            cfg.triggers = MagicMock()
            cfg.triggers.register = MagicMock()
            cfg.run_workflow = MagicMock(return_value="result")
            return cfg
        return None

    registry.get_workflow_config.side_effect = get_config
    return registry


@pytest.fixture
def engine():
    executor = make_executor()
    return TriggerEngine(workflow_executor=executor, consumer_concurrency=2)


@pytest.fixture
def engine_no_executor():
    return TriggerEngine(workflow_executor=None, consumer_concurrency=2)


class TestInit:

    def test_defaults(self):
        engine = TriggerEngine()
        assert engine.workflow_executor is None
        assert engine.consumer_concurrency == 5
        assert engine.drain_timeout == 30.0
        assert engine.triggers == {}
        assert not engine.is_running()

    def test_invalid_concurrency_raises(self):
        with pytest.raises(ValueError, match="consumer_concurrency"):
            TriggerEngine(consumer_concurrency=0)

    def test_invalid_drain_timeout_raises(self):
        with pytest.raises(ValueError, match="drain_timeout"):
            TriggerEngine(drain_timeout=0)

    def test_negative_drain_timeout_raises(self):
        with pytest.raises(ValueError, match="drain_timeout"):
            TriggerEngine(drain_timeout=-5.0)

    def test_bounded_queue(self):
        engine = TriggerEngine()
        assert engine.task_queue.maxsize == 1000


class TestRegister:

    def test_register_stores_trigger(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        assert "t1" in engine.triggers
        assert engine.triggers["t1"] is trigger

    def test_register_calls_workflow_triggers_register(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        registry = engine.workflow_executor.get_workflow_registry()
        config = registry.get_workflow_config("wf1")
        config.triggers.register.assert_called_once_with(
            trigger, engine._handle_activation
        )

    def test_register_duplicate_raises(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        with pytest.raises(ValueError, match="already registered"):
            engine.register(trigger)

    def test_register_unknown_workflow_raises(self):
        registry = make_registry(workflow_names=["wf_exists"])
        executor = make_executor(registry)
        engine = TriggerEngine(workflow_executor=executor)
        trigger = make_trigger("t1", "wf_missing")
        with pytest.raises(WorkflowNotfound):
            engine.register(trigger)

    def test_register_without_executor_raises(self, engine_no_executor):
        trigger = make_trigger("t1", "wf1")
        with pytest.raises(WorkflowExecutionError):
            engine_no_executor.register(trigger)


class TestFireTrigger:

    def test_fire_trigger_enqueues_item(self, engine):
        engine.fire_trigger("wf1", {"key": "val"}, TriggerType.MANUAL)
        assert engine.task_queue.qsize() == 1

    def test_fire_trigger_without_executor_raises(self, engine_no_executor):
        with pytest.raises(RuntimeError, match="executor"):
            engine_no_executor.fire_trigger("wf1", {}, TriggerType.MANUAL)

    def test_fire_trigger_unknown_workflow_raises(self, engine):
        engine.workflow_executor._workflow_registry.get_workflow_config.return_value = (
            None
        )
        with pytest.raises(RuntimeError, match="not found"):
            engine.fire_trigger("wf_missing", {}, TriggerType.MANUAL)

    def test_fire_trigger_full_queue_logs_warning(self, engine, caplog):
        import logging

        engine.task_queue = asyncio.Queue(maxsize=1)
        engine.task_queue.put_nowait(("wf1", {}))  # fill it
        with caplog.at_level(logging.WARNING):
            engine.fire_trigger("wf1", {}, TriggerType.MANUAL)
        assert "full" in caplog.text.lower() or "dropping" in caplog.text.lower()


class TestStart:

    @pytest.mark.asyncio
    async def test_start_without_executor_raises(self, engine_no_executor):
        with pytest.raises(RuntimeError, match="workflow_executor"):
            await engine_no_executor.start()

    @pytest.mark.asyncio
    async def test_start_sets_running(self, engine):
        await engine.start()
        assert engine.is_running()
        await engine.force_stop()

    @pytest.mark.asyncio
    async def test_start_spawns_workers(self, engine):
        await engine.start()
        assert len(engine._consumer_tasks) == engine.consumer_concurrency
        await engine.force_stop()

    @pytest.mark.asyncio
    async def test_start_calls_trigger_start(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        await engine.start()
        trigger.start.assert_called_once()
        await engine.force_stop()

    @pytest.mark.asyncio
    async def test_start_marks_trigger_error_on_failure(self, engine):
        from volnux.engine.workflows.trigger.triggers.base import TriggerLifecycle

        trigger = make_trigger("t1", "wf1")
        trigger.start.side_effect = Exception("boom")
        engine.register(trigger)
        await engine.start()  # should not raise
        assert trigger.lifecycle == TriggerLifecycle.ERROR
        await engine.force_stop()


class TestStop:

    @pytest.mark.asyncio
    async def test_stop_sets_not_running(self, engine):
        await engine.start()
        await engine.stop()
        assert not engine.is_running()

    @pytest.mark.asyncio
    async def test_stop_drains_queue(self, engine):
        await engine.start()
        # enqueue a couple of items; workers will consume them
        engine.task_queue.put_nowait(("wf1", {}))
        engine.task_queue.put_nowait(("wf1", {}))
        await engine.stop()
        assert engine.task_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_stop_clears_consumer_tasks(self, engine):
        await engine.start()
        await engine.stop()
        assert engine._consumer_tasks == []

    @pytest.mark.asyncio
    async def test_stop_calls_trigger_stop(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        await engine.start()
        await engine.stop()
        trigger.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_timeout_logs_warning(self, caplog):
        import logging

        executor = make_executor()

        # Make executor hang forever so drain times out
        async def hang(*_):
            await asyncio.sleep(9999)

        executor.side_effect = hang

        engine = TriggerEngine(
            workflow_executor=executor,
            consumer_concurrency=1,
            drain_timeout=0.1,
        )
        await engine.start()
        engine.task_queue.put_nowait(("wf1", {}))

        with caplog.at_level(logging.WARNING):
            await engine.stop()

        assert "timed out" in caplog.text.lower() or "timeout" in caplog.text.lower()


class TestForceStop:

    @pytest.mark.asyncio
    async def test_force_stop_sets_not_running(self, engine):
        await engine.start()
        await engine.force_stop()
        assert not engine.is_running()

    @pytest.mark.asyncio
    async def test_force_stop_returns_dropped_count(self, engine):
        await engine.start()
        # Pause workers so items stay in queue
        for task in engine._consumer_tasks:
            task.cancel()
        await asyncio.gather(*engine._consumer_tasks, return_exceptions=True)
        engine._consumer_tasks.clear()

        engine.task_queue.put_nowait(("wf1", {}))
        engine.task_queue.put_nowait(("wf1", {}))

        dropped = await engine.force_stop()
        assert dropped == 2

    @pytest.mark.asyncio
    async def test_force_stop_clears_consumer_tasks(self, engine):
        await engine.start()
        await engine.force_stop()
        assert engine._consumer_tasks == []

    @pytest.mark.asyncio
    async def test_force_stop_calls_trigger_stop(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        await engine.start()
        await engine.force_stop()
        trigger.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_force_stop_continues_on_trigger_error(self, engine):
        trigger = make_trigger("t1", "wf1")
        trigger.stop.side_effect = Exception("trigger stop failed")
        engine.register(trigger)
        await engine.start()
        dropped = await engine.force_stop()  # must not raise
        assert isinstance(dropped, int)

    @pytest.mark.asyncio
    async def test_force_stop_empty_queue_returns_zero(self, engine):
        await engine.start()
        dropped = await engine.force_stop()
        assert dropped == 0


class TestHandleActivation:

    @pytest.mark.asyncio
    async def test_enqueues_workflow(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)

        activation = MagicMock(spec=TriggerActivation)
        activation.trigger_id = "t1"
        activation.workflow_params = {"x": 1}

        await engine._handle_activation(activation)
        assert engine.task_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_unknown_trigger_logs_error(self, engine, caplog):
        import logging

        activation = MagicMock(spec=TriggerActivation)
        activation.trigger_id = "unknown"

        with caplog.at_level(logging.ERROR):
            await engine._handle_activation(activation)

        assert "unknown" in caplog.text.lower()
        assert engine.task_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_full_queue_increments_error_count(self, engine):
        trigger = make_trigger("t1", "wf1")
        engine.register(trigger)
        engine.task_queue = asyncio.Queue(maxsize=1)
        engine.task_queue.put_nowait(("wf1", {}))  # fill it

        activation = MagicMock(spec=TriggerActivation)
        activation.trigger_id = "t1"
        activation.workflow_params = {}

        await engine._handle_activation(activation)
        assert trigger.error_count == 1


class TestWorkflowConfigExecutor:

    @pytest.mark.asyncio
    async def test_execute_success(self):
        registry = make_registry(["wf1"])
        executor = WorkflowConfigExecutor(registry)
        result = await executor.execute("wf1", {"run_type": "single", "key": "val"})
        assert result == "result"

    @pytest.mark.asyncio
    async def test_execute_does_not_mutate_params(self):
        registry = make_registry(["wf1"])
        executor = WorkflowConfigExecutor(registry)
        params = {"run_type": "batch", "key": "val"}
        original = dict(params)
        await executor.execute("wf1", params)
        assert params == original  # fix-5: caller's dict must be unchanged

    @pytest.mark.asyncio
    async def test_execute_registry_not_ready_raises(self):
        registry = make_registry(["wf1"])
        registry.is_ready.return_value = False
        executor = WorkflowConfigExecutor(registry)
        with pytest.raises(WorkflowExecutionError, match="not ready"):
            await executor.execute("wf1", {})

    @pytest.mark.asyncio
    async def test_execute_missing_config_raises(self):
        registry = make_registry([])  # no workflows
        executor = WorkflowConfigExecutor(registry)
        with pytest.raises(WorkflowExecutionError, match="not found"):
            await executor.execute("wf_missing", {})

    @pytest.mark.asyncio
    async def test_execute_awaits_async_run_workflow(self):
        registry = make_registry(["wf1"])
        config = registry.get_workflow_config("wf1")
        config.run_workflow = AsyncMock(return_value="async_result")
        executor = WorkflowConfigExecutor(registry)
        result = await executor.execute("wf1", {})
        assert result == "async_result"

    @pytest.mark.asyncio
    async def test_callable_interface(self):
        registry = make_registry(["wf1"])
        executor = WorkflowConfigExecutor(registry)
        result = await executor("wf1", {})
        assert result == "result"
