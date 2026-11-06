"""TriggerEngine: the caller's execution id, unregister, and starting late triggers."""

import asyncio

import pytest
import pytest_asyncio

from tests.unittests.trigger.test_trigger_engine import (
    FakeRegistry,
    RecordingExecutor,
    until,
)
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.triggers import ManualTrigger, TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType

pytestmark = pytest.mark.usefixtures("store")


class IdExecutor(RecordingExecutor):
    """An executor that takes the caller's execution id."""

    def __init__(self, registry):
        super().__init__(registry)
        self.ids = []

    async def execute(self, workflow_name, params, *, execution_id=None):
        self.ids.append(execution_id)
        return await super().execute(workflow_name, params)


@pytest.fixture
def registry():
    return FakeRegistry("wf")


@pytest.fixture
def executor(registry):
    return IdExecutor(registry)


@pytest.fixture
def engine(executor):
    return TriggerEngine(
        executor, consumer_concurrency=1, drain_timeout=1, state_sync_interval=0.05
    )


@pytest_asyncio.fixture
async def running(engine):
    await engine.start()
    yield engine
    await engine.force_stop()


class TestExecutionId:
    @pytest.mark.asyncio
    async def test_without_an_id_the_queue_item_is_unchanged(self, engine):
        engine.register(ManualTrigger("wf"))
        await engine.fire_trigger("wf", {"a": 1}, TriggerType.MANUAL)
        assert engine.task_queue.get_nowait() == ("wf", {"a": 1})

    @pytest.mark.asyncio
    async def test_with_an_id_it_travels_with_the_run(self, engine):
        engine.register(ManualTrigger("wf"))
        await engine.fire_trigger("wf", {"a": 1}, TriggerType.MANUAL, execution_id="e1")
        assert engine.task_queue.get_nowait() == ("wf", {"a": 1}, "e1")

    @pytest.mark.asyncio
    async def test_the_executor_receives_it(self, running, executor):
        running.register(ManualTrigger("wf"))
        await running.fire_trigger("wf", {}, TriggerType.MANUAL, execution_id="e1")
        await running.fire_trigger("wf", {}, TriggerType.MANUAL)
        await until(lambda: len(executor.runs) == 2)
        assert executor.ids == ["e1", None]

    @pytest.mark.asyncio
    async def test_an_executor_that_knows_no_ids_still_serves_plain_runs(
        self, registry
    ):
        from tests.unittests.trigger.test_trigger_engine import (
            RecordingExecutor as Plain,
        )

        plain = Plain(registry)
        engine = TriggerEngine(plain, consumer_concurrency=1, state_sync_interval=0.05)
        engine.register(ManualTrigger("wf"))
        await engine.start()
        try:
            await engine.fire_trigger("wf", {"n": 1}, TriggerType.MANUAL)
            await until(lambda: plain.runs)
            assert plain.runs == [("wf", {"n": 1})]
        finally:
            await engine.force_stop()


class TestUnregister:
    @pytest.mark.asyncio
    async def test_the_trigger_is_removed_from_engine_and_workflow(
        self, engine, registry
    ):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.unregister(trigger.trigger_id)
        assert trigger.trigger_id not in engine.triggers
        assert registry.configs["wf"].triggers.get_trigger(trigger.trigger_id) is None

    @pytest.mark.asyncio
    async def test_a_started_trigger_is_stopped(self, running):
        trigger = ManualTrigger("wf")
        running.register(trigger)
        await running.start_trigger(trigger.trigger_id)
        assert trigger.lifecycle is TriggerLifecycle.ACTIVE
        await running.unregister(trigger.trigger_id)
        assert trigger.lifecycle is TriggerLifecycle.STOPPED

    @pytest.mark.asyncio
    async def test_the_slot_can_be_used_again(self, engine):
        first = ManualTrigger("wf")
        engine.register(first)
        await engine.unregister(first.trigger_id)
        engine.register(ManualTrigger("wf"))  # (one manual trigger per workflow)

    @pytest.mark.asyncio
    async def test_unknown_id(self, engine):
        with pytest.raises(ValueError, match="not registered"):
            await engine.unregister("nope")


class TestStartTrigger:
    @pytest.mark.asyncio
    async def test_a_late_trigger_is_started_once(self, running):
        trigger = ManualTrigger("wf")
        running.register(trigger)
        assert await running.start_trigger(trigger.trigger_id) is True
        assert trigger.lifecycle is TriggerLifecycle.ACTIVE
        assert await running.start_trigger(trigger.trigger_id) is False

    @pytest.mark.asyncio
    async def test_nothing_starts_while_the_engine_is_stopped(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        assert await engine.start_trigger(trigger.trigger_id) is False
        assert trigger.lifecycle is not TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    async def test_unknown_id(self, running):
        with pytest.raises(ValueError):
            await running.start_trigger("nope")
