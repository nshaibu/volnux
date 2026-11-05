"""TriggerEngine: registration, the queue, the consumer pool, shutdown, state sync."""

import asyncio
from typing import Any, Dict, List, Tuple

import pytest
import pytest_asyncio
from datetime import datetime, timezone

from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import (
    ManualTrigger,
    TriggerLifecycle,
)
from volnux.engine.workflows.trigger.triggers.base import TriggerType
from volnux.engine.workflows.workflow import (
    TriggerRegistry,
    WorkflowExecutionError,
    WorkflowNotFound,
)

pytestmark = pytest.mark.usefixtures("store")


# --------------------------------------------------------------------- doubles
class FakeConfig:
    def __init__(self):
        self.triggers = TriggerRegistry()


class FakeRegistry:
    def __init__(self, *names):
        self.configs = {name: FakeConfig() for name in names}

    def get_workflow_config(self, name):
        return self.configs.get(name)


class RecordingExecutor(WorkflowExecutor):
    """Records the runs; ``gate`` holds them, ``fail`` makes them raise."""

    def __init__(self, registry):
        super().__init__(registry)
        self.runs: List[Tuple[str, Dict[str, Any]]] = []
        self.started = 0
        self.gate = None
        self.fail = None

    async def execute(self, workflow_name, params):
        self.started += 1
        if self.gate is not None:
            await self.gate.wait()
        if self.fail is not None:
            raise self.fail
        self.runs.append((workflow_name, params))
        return "ok"


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


@pytest.fixture
def registry():
    return FakeRegistry("wf", "other")


@pytest.fixture
def executor(registry):
    return RecordingExecutor(registry)


@pytest.fixture
def engine(executor):
    return TriggerEngine(
        executor, consumer_concurrency=2, drain_timeout=1, state_sync_interval=0.05
    )


@pytest_asyncio.fixture
async def running(engine):
    await engine.start()
    yield engine
    await engine.force_stop()


# ------------------------------------------------------------------ __init__
class TestInit:
    def test_defaults_come_from_the_settings(self):
        engine = TriggerEngine()
        assert engine.workflow_executor is None
        assert engine.consumer_concurrency == 5
        assert engine.drain_timeout == 30.0
        assert engine._state_sync_interval == 10.0
        assert engine.task_queue.maxsize == 1000
        assert engine.triggers == {} and not engine.is_running()

    def test_the_settings_are_used(self):
        from volnux.conf import override_settings

        with override_settings(
            TRIGGER_ENGINE_CONSUMER_CONCURRENCY=3,
            TRIGGER_ENGINE_MAX_QUEUE_SIZE=7,
            TRIGGER_ENGINE_DRAIN_TIMEOUT=2.5,
            TRIGGER_ENGINE_STATE_SYNC_INTERVAL=0.5,
        ):
            engine = TriggerEngine()
        assert engine.consumer_concurrency == 3
        assert engine.task_queue.maxsize == 7
        assert engine.drain_timeout == 2.5
        assert engine._state_sync_interval == 0.5

    def test_arguments_win_over_the_settings(self):
        engine = TriggerEngine(consumer_concurrency=9, max_queue_size=3)
        assert engine.consumer_concurrency == 9 and engine.task_queue.maxsize == 3

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"consumer_concurrency": 0}, "consumer_concurrency"),
            ({"drain_timeout": 0}, "drain_timeout"),
            ({"drain_timeout": -1}, "drain_timeout"),
            ({"state_sync_interval": 0}, "state_sync_interval"),
            ({"state_sync_interval": -2}, "state_sync_interval"),
            ({"max_queue_size": -1}, "max_queue_size"),
        ],
    )
    def test_invalid_values_are_refused(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            TriggerEngine(**kwargs)

    def test_a_queue_size_of_zero_is_unbounded(self):
        assert TriggerEngine(max_queue_size=0).task_queue.maxsize == 0


# ------------------------------------------------------------------ executor
class TestExecutorAccess:
    def test_has_workflow_executor(self, engine):
        assert engine.has_workflow_executor()
        assert not TriggerEngine().has_workflow_executor()

    def test_set_workflow_executor(self, registry):
        engine = TriggerEngine()
        executor = RecordingExecutor(registry)
        engine.set_workflow_executor(executor)
        assert engine.workflow_executor is executor
        assert engine.get_workflow_registry() is registry

    def test_the_registry_needs_an_executor(self):
        with pytest.raises(WorkflowExecutionError, match="not provided"):
            TriggerEngine().get_workflow_registry()


# ------------------------------------------------------------------ register
class TestRegister:
    def test_a_trigger_is_registered_with_its_workflow_and_the_engine(
        self, engine, registry
    ):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        assert engine.triggers[trigger.trigger_id] is trigger
        assert (
            registry.configs["wf"].triggers.get_trigger(trigger.trigger_id) is trigger
        )
        assert trigger.lifecycle is TriggerLifecycle.INITIALIZED

    def test_activations_reach_the_engine(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        assert trigger._on_activate == engine._handle_activation

    def test_an_unknown_workflow(self, engine):
        with pytest.raises(WorkflowNotFound, match="nope"):
            engine.register(ManualTrigger("nope"))

    def test_the_same_trigger_twice(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        with pytest.raises(ValueError, match="already registered"):
            engine.register(trigger)

    def test_one_trigger_of_a_type_per_workflow(self, engine):
        engine.register(ManualTrigger("wf"))
        second = ManualTrigger("wf")
        with pytest.raises(ValueError, match="already registered"):
            engine.register(second)
        assert second.trigger_id not in engine.triggers

    def test_the_same_type_on_another_workflow(self, engine):
        engine.register(ManualTrigger("wf"))
        engine.register(ManualTrigger("other"))
        assert len(engine.triggers) == 2

    def test_without_an_executor(self):
        with pytest.raises(WorkflowExecutionError):
            TriggerEngine().register(ManualTrigger("wf"))


class TestRegisterManager:
    class Manager(ManualTrigger):
        trigger_type = TriggerType.MANAGER

    def test_a_manager_needs_no_workflow(self, engine):
        manager = self.Manager("no-such-workflow")
        engine.register_manager(manager)
        assert engine.triggers[manager.trigger_id] is manager

    def test_only_managers(self, engine):
        with pytest.raises(ValueError, match="MANAGER"):
            engine.register_manager(ManualTrigger("wf"))

    def test_twice(self, engine):
        manager = self.Manager("x")
        engine.register_manager(manager)
        with pytest.raises(ValueError, match="already registered"):
            engine.register_manager(manager)


# ---------------------------------------------------------------- fire_trigger
class TestFireTrigger:
    @pytest.fixture
    def with_manual(self, engine):
        engine.register(ManualTrigger("wf"))
        return engine

    @pytest.mark.asyncio
    async def test_it_queues_the_run(self, with_manual):
        await with_manual.fire_trigger("wf", {"a": 1}, TriggerType.MANUAL)
        assert with_manual.task_queue.get_nowait() == ("wf", {"a": 1})

    @pytest.mark.asyncio
    async def test_without_an_executor(self):
        with pytest.raises(RuntimeError, match="executor"):
            await TriggerEngine().fire_trigger("wf", {}, TriggerType.MANUAL)

    @pytest.mark.asyncio
    async def test_unknown_workflow(self, engine):
        with pytest.raises(RuntimeError, match="config not found"):
            await engine.fire_trigger("nope", {}, TriggerType.MANUAL)

    @pytest.mark.asyncio
    async def test_a_trigger_type_the_workflow_does_not_have(self, with_manual):
        with pytest.raises(ValueError, match="not enabled"):
            await with_manual.fire_trigger("wf", {}, TriggerType.EVENT)
        assert with_manual.task_queue.empty()

    @pytest.mark.asyncio
    async def test_a_full_queue_drops_the_run(self, registry, executor, caplog):
        engine = TriggerEngine(executor, max_queue_size=1)
        engine.register(ManualTrigger("wf"))
        await engine.fire_trigger("wf", {"n": 1}, TriggerType.MANUAL)
        await engine.fire_trigger("wf", {"n": 2}, TriggerType.MANUAL)
        assert engine.task_queue.qsize() == 1
        assert engine.task_queue.get_nowait() == ("wf", {"n": 1})
        assert "queue is full" in caplog.text


# ------------------------------------------------------------ _handle_activation
class TestHandleActivation:
    def activation(self, trigger, params=None):
        from volnux.engine.workflows.trigger.triggers import TriggerActivation

        return TriggerActivation(
            trigger_id=trigger.trigger_id,
            activated_at=datetime.now(timezone.utc),
            activation_source=trigger.trigger_type,
            workflow_params=params or {},
        )

    @pytest.mark.asyncio
    async def test_the_run_is_queued(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine._handle_activation(self.activation(trigger, {"k": "v"}))
        assert engine.task_queue.get_nowait() == ("wf", {"k": "v"})
        assert trigger.error_count == 0

    @pytest.mark.asyncio
    async def test_an_unknown_trigger_is_ignored(self, engine):
        trigger = ManualTrigger("wf")  # not registered
        await engine._handle_activation(self.activation(trigger))
        assert engine.task_queue.empty()

    @pytest.mark.asyncio
    async def test_a_full_queue_counts_an_error_on_the_trigger(self, executor):
        engine = TriggerEngine(executor, max_queue_size=1)
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine._handle_activation(self.activation(trigger))
        await engine._handle_activation(self.activation(trigger))
        assert engine.task_queue.qsize() == 1
        assert trigger.error_count == 1
        # and the count is persisted
        saved = await TriggerStateRecord.get_or_none(record_id=trigger.trigger_id)
        assert saved.error_count == 1

    @pytest.mark.asyncio
    async def test_it_never_waits_on_a_full_queue(self, executor):
        engine = TriggerEngine(executor, max_queue_size=1)
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine._handle_activation(self.activation(trigger))
        await asyncio.wait_for(engine._handle_activation(self.activation(trigger)), 1)

    @pytest.mark.asyncio
    async def test_an_unexpected_failure_counts_an_error(self, engine, monkeypatch):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        monkeypatch.setattr(
            engine,
            "_enqueue",
            lambda *a: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        await engine._handle_activation(self.activation(trigger))
        assert trigger.error_count == 1

    @pytest.mark.asyncio
    async def test_a_trigger_activation_end_to_end(self, running, executor):
        trigger = ManualTrigger("wf")
        running.register(trigger)
        await trigger.run()
        await trigger.activate(x=1)
        await until(lambda: executor.runs)
        assert executor.runs[0][0] == "wf"


# ---------------------------------------------------------------------- start
class TestStart:
    @pytest.mark.asyncio
    async def test_it_needs_an_executor(self):
        with pytest.raises(RuntimeError, match="workflow_executor"):
            await TriggerEngine().start()

    @pytest.mark.asyncio
    async def test_it_starts_the_pool_the_sync_loop_and_the_triggers(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        try:
            assert engine.is_running()
            assert len(engine._consumer_tasks) == 2
            assert engine._state_sync_task is not None
            assert trigger.lifecycle is TriggerLifecycle.ACTIVE
        finally:
            await engine.force_stop()

    @pytest.mark.asyncio
    async def test_a_second_start_changes_nothing(self, engine, caplog):
        await engine.start()
        try:
            consumers = list(engine._consumer_tasks)
            sync_task = engine._state_sync_task
            await engine.start()
            assert engine._consumer_tasks == consumers
            assert engine._state_sync_task is sync_task
            assert "already running" in caplog.text
        finally:
            await engine.force_stop()

    @pytest.mark.asyncio
    async def test_a_trigger_that_fails_to_start_is_marked_and_the_rest_start(
        self, engine
    ):
        class Broken(ManualTrigger):
            async def start(self):
                raise RuntimeError("cannot start")

        broken, fine = Broken("wf"), ManualTrigger("other")
        engine.register(broken)
        engine.register(fine)
        await engine.start()
        try:
            assert broken.lifecycle is TriggerLifecycle.ERROR
            assert fine.lifecycle is TriggerLifecycle.ACTIVE
            saved = await TriggerStateRecord.get_or_none(record_id=broken.trigger_id)
            assert saved.lifecycle is TriggerLifecycle.ERROR
        finally:
            await engine.force_stop()

    @pytest.mark.asyncio
    async def test_it_can_be_started_again_after_stopping(self, engine, executor):
        await engine.start()
        await engine.stop()
        await engine.start()
        try:
            await engine.task_queue.put(("wf", {"again": True}))
            await until(lambda: executor.runs)
        finally:
            await engine.force_stop()


# ------------------------------------------------------------- consumer pool
class TestConsumers:
    @pytest.mark.asyncio
    async def test_queued_runs_are_executed(self, running, executor):
        for n in range(5):
            running.task_queue.put_nowait(("wf", {"n": n}))
        await until(lambda: len(executor.runs) == 5)
        assert sorted(p["n"] for _, p in executor.runs) == list(range(5))

    @pytest.mark.asyncio
    async def test_runs_are_concurrent_up_to_the_pool_size(self, running, executor):
        executor.gate = asyncio.Event()
        for _ in range(5):
            running.task_queue.put_nowait(("wf", {}))
        await until(lambda: executor.started == 2)
        await asyncio.sleep(0.05)
        assert executor.started == 2  # two consumers, the others wait
        executor.gate.set()
        await until(lambda: len(executor.runs) == 5)

    @pytest.mark.asyncio
    async def test_a_failing_run_does_not_stop_the_worker(
        self, running, executor, caplog
    ):
        executor.fail = RuntimeError("workflow broke")
        for _ in range(4):
            running.task_queue.put_nowait(("wf", {}))
        await until(lambda: executor.started == 4)
        assert "workflow broke" in caplog.text
        executor.fail = None
        running.task_queue.put_nowait(("wf", {"after": True}))
        await until(lambda: executor.runs)
        assert executor.runs[0][1] == {"after": True}

    @pytest.mark.asyncio
    async def test_the_queue_is_accounted_for_after_failures(self, running, executor):
        executor.fail = RuntimeError("x")
        for _ in range(3):
            running.task_queue.put_nowait(("wf", {}))
        await asyncio.wait_for(running.task_queue.join(), 2)

    @pytest.mark.asyncio
    async def test_a_malformed_item_does_not_kill_the_worker(self, running, executor):
        running.task_queue.put_nowait("not a tuple")
        running.task_queue.put_nowait(("wf", {"ok": 1}))
        await until(lambda: executor.runs)


# ------------------------------------------------------------------------ stop
class TestStop:
    @pytest.mark.asyncio
    async def test_it_stops_triggers_and_ends_the_pool(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        await engine.stop()
        assert not engine.is_running()
        assert trigger.lifecycle is TriggerLifecycle.STOPPED
        assert engine._consumer_tasks == [] and engine._state_sync_task is None

    @pytest.mark.asyncio
    async def test_the_stopped_state_is_persisted(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        await engine.stop()
        saved = await TriggerStateRecord.get_or_none(record_id=trigger.trigger_id)
        assert saved.lifecycle is TriggerLifecycle.STOPPED

    @pytest.mark.asyncio
    async def test_queued_runs_are_drained_first(self, engine, executor):
        await engine.start()
        for n in range(6):
            engine.task_queue.put_nowait(("wf", {"n": n}))
        await engine.stop()
        assert len(executor.runs) == 6
        assert engine.task_queue.empty()

    @pytest.mark.asyncio
    async def test_a_drain_that_times_out_drops_what_is_left(self, executor, caplog):
        engine = TriggerEngine(executor, consumer_concurrency=1, drain_timeout=0.1)
        executor.gate = asyncio.Event()  # the first run never finishes
        await engine.start()
        for _ in range(4):
            engine.task_queue.put_nowait(("wf", {}))
        await until(lambda: executor.started == 1)
        await engine.stop()
        assert "Drain timed out" in caplog.text
        assert engine.task_queue.empty()  # not left for the next start
        assert engine._consumer_tasks == []

    @pytest.mark.asyncio
    async def test_stopping_an_engine_that_never_started(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.stop()
        assert trigger.lifecycle is TriggerLifecycle.INITIALIZED  # nothing to stop

    @pytest.mark.asyncio
    async def test_a_trigger_that_fails_to_stop_does_not_stop_the_others(self, engine):
        class Stubborn(ManualTrigger):
            async def stop(self):
                raise RuntimeError("will not stop")

        stubborn, fine = Stubborn("wf"), ManualTrigger("other")
        engine.register(stubborn)
        engine.register(fine)
        await engine.start()
        await engine.stop()
        assert fine.lifecycle is TriggerLifecycle.STOPPED
        assert stubborn.lifecycle is TriggerLifecycle.STOPPED  # the record says so
        assert engine._consumer_tasks == []

    @pytest.mark.asyncio
    async def test_stop_twice(self, engine):
        await engine.start()
        await engine.stop()
        await engine.stop()


class TestForceStop:
    @pytest.mark.asyncio
    async def test_it_drops_the_queue_and_reports_how_many(self, executor):
        engine = TriggerEngine(executor, consumer_concurrency=1)
        executor.gate = asyncio.Event()
        await engine.start()
        for _ in range(4):
            engine.task_queue.put_nowait(("wf", {}))
        await until(lambda: executor.started == 1)
        assert await engine.force_stop() == 3
        assert engine.task_queue.empty()
        assert not engine.is_running()

    @pytest.mark.asyncio
    async def test_it_cancels_the_state_sync_loop_too(self, engine):
        await engine.start()
        sync_task = engine._state_sync_task
        await engine.force_stop()
        assert sync_task.done() and engine._state_sync_task is None

    @pytest.mark.asyncio
    async def test_it_stops_the_triggers(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        await engine.force_stop()
        assert trigger.lifecycle is TriggerLifecycle.STOPPED

    @pytest.mark.asyncio
    async def test_a_failing_trigger_is_reported_and_the_rest_stopped(
        self, engine, caplog
    ):
        class Stubborn(ManualTrigger):
            async def stop(self):
                raise RuntimeError("will not stop")

        stubborn, fine = Stubborn("wf"), ManualTrigger("other")
        engine.register(stubborn)
        engine.register(fine)
        await engine.start()
        await engine.force_stop()
        assert fine.lifecycle is TriggerLifecycle.STOPPED
        assert engine._consumer_tasks == []

    @pytest.mark.asyncio
    async def test_on_an_empty_queue(self, engine):
        await engine.start()
        assert await engine.force_stop() == 0


# ------------------------------------------------------------------ state sync
class TestStateSync:
    async def request(self, trigger, lifecycle):
        """What the CLI does: write the wanted lifecycle and mark the record dirty."""
        record = await TriggerStateRecord.get_or_none(record_id=trigger.trigger_id)
        record.lifecycle = lifecycle
        record.dirty = True
        await record.save()
        return record

    @pytest_asyncio.fixture
    async def active(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        yield engine, trigger
        await engine.force_stop()

    @pytest.mark.asyncio
    async def test_a_pause_request_is_applied(self, active):
        engine, trigger = active
        await self.request(trigger, TriggerLifecycle.PAUSED)
        await until(lambda: trigger.lifecycle is TriggerLifecycle.PAUSED)

    @pytest.mark.asyncio
    async def test_the_dirty_flag_is_cleared(self, active):
        engine, trigger = active
        await self.request(trigger, TriggerLifecycle.PAUSED)

        async def clean():
            record = await TriggerStateRecord.get_or_none(record_id=trigger.trigger_id)
            return not record.dirty

        deadline = asyncio.get_running_loop().time() + 3
        while not await clean():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)

    @pytest.mark.asyncio
    async def test_a_resume_request_is_applied(self, active):
        engine, trigger = active
        await trigger.pause()
        record = await self.request(trigger, TriggerLifecycle.ACTIVE)
        await until(lambda: trigger.lifecycle is TriggerLifecycle.ACTIVE)

    @pytest.mark.asyncio
    async def test_a_stop_request_is_applied(self, active):
        engine, trigger = active
        await self.request(trigger, TriggerLifecycle.STOPPED)
        await until(lambda: trigger.lifecycle is TriggerLifecycle.STOPPED)

    @pytest.mark.asyncio
    async def test_a_request_that_cannot_apply_is_dropped_not_retried(
        self, engine, caplog
    ):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await trigger.state.save()
        # a pause request for a trigger that is not active
        record = await self.request(trigger, TriggerLifecycle.PAUSED)
        await engine._apply_state_change(record)
        assert "cannot apply" in caplog.text
        assert record.dirty is False

    @pytest.mark.asyncio
    async def test_a_record_of_an_unknown_trigger_is_cleaned(self, engine, caplog):
        stranger = ManualTrigger("wf")  # never registered with this engine
        record = await self.request_unregistered(stranger)
        await engine._apply_state_change(record)
        assert "not found in engine registry" in caplog.text
        assert record.dirty is False

    async def request_unregistered(self, trigger):
        await trigger.state.save()
        trigger.state.dirty = True
        await trigger.state.save()
        return trigger.state

    @pytest.mark.asyncio
    async def test_a_record_that_already_matches_is_left_alone(self, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        await engine.start()
        try:
            record = await self.request(trigger, TriggerLifecycle.ACTIVE)
            await engine._apply_state_change(record)
            assert trigger.lifecycle is TriggerLifecycle.ACTIVE
            assert record.dirty is False
        finally:
            await engine.force_stop()

    @pytest.mark.asyncio
    async def test_an_error_in_one_poll_does_not_end_the_loop(
        self, engine, monkeypatch
    ):
        calls = []
        real = TriggerStateRecord.get_dirty

        async def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("store down")
            return await real()

        monkeypatch.setattr(TriggerStateRecord, "get_dirty", flaky)
        await engine.start()
        try:
            await until(lambda: len(calls) >= 3)
            assert not engine._state_sync_task.done()
        finally:
            await engine.force_stop()

    @pytest.mark.asyncio
    async def test_a_failure_applying_one_record_does_not_stop_the_others(
        self, engine, monkeypatch
    ):
        first, second = ManualTrigger("wf"), ManualTrigger("other")
        engine.register(first)
        engine.register(second)
        await engine.start()
        try:
            real = engine._apply_state_change
            applied = []

            async def apply(record):
                if record.id == first.trigger_id:
                    raise ValueError("bad record")
                applied.append(record.id)
                await real(record)

            monkeypatch.setattr(engine, "_apply_state_change", apply)
            await self.request(first, TriggerLifecycle.PAUSED)
            await self.request(second, TriggerLifecycle.PAUSED)
            await until(lambda: second.trigger_id in applied)
        finally:
            await engine.force_stop()


# --------------------------------------------------------------- TriggerRegistry
class TestTriggerRegistry:
    """The registry of a workflow, which the engine registers its triggers with."""

    @pytest.fixture
    def registry_and_trigger(self):
        registry = TriggerRegistry()
        trigger = ManualTrigger("wf")

        async def callback(activation):
            pass

        registry.register(trigger, callback)
        return registry, trigger

    def test_get_trigger_finds_it_by_id(self, registry_and_trigger):
        registry, trigger = registry_and_trigger
        assert registry.get_trigger(trigger.trigger_id) is trigger

    def test_get_trigger_of_an_unknown_id(self, registry_and_trigger):
        registry, _ = registry_and_trigger
        assert registry.get_trigger("nope") is None

    def test_unregister_removes_it(self, registry_and_trigger):
        registry, trigger = registry_and_trigger
        registry.unregister(trigger.trigger_id)
        assert registry.get_trigger(trigger.trigger_id) is None
        assert not registry.has_type(TriggerType.MANUAL)

    def test_unregister_of_an_unknown_id(self, registry_and_trigger):
        registry, _ = registry_and_trigger
        with pytest.raises(ValueError, match="not found"):
            registry.unregister("nope")

    def test_has_type(self, registry_and_trigger):
        registry, _ = registry_and_trigger
        assert registry.has_type(TriggerType.MANUAL)
        assert not registry.has_type(TriggerType.EVENT)
        assert not TriggerRegistry().has_type(TriggerType.MANUAL)
