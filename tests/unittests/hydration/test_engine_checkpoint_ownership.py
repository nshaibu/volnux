"""The engine, not the caller, creates and stops the checkpoint manager.

The manager is an asyncio component (two tasks on the running loop): it needs no
process, only a running event loop.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.engine.checkpoint_config import (
    CheckPointConfig,
    CheckPointFrequency,
    CheckPointPolicyConfig,
    CheckPointRuntimeConfig,
)
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.engine.base import EngineExecutionResult
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager


def fast_config(frequency=CheckPointFrequency.PER_TASK):
    return CheckPointConfig(
        policy=CheckPointPolicyConfig(frequency=frequency),
        runtime=CheckPointRuntimeConfig(retry_attempts=1, retry_delay_seconds=0.0),
    )


async def ok(self):
    return True, "ok"


async def boom(self):
    raise RuntimeError("step failed")


class TestFromConfig:
    def test_config_names_are_mapped_onto_the_constructor(self):
        config = CheckPointConfig(
            policy=CheckPointPolicyConfig(checkpoint_interval_seconds=2.5),
            runtime=CheckPointRuntimeConfig(
                max_concurrent_checkpoints=7,
                retry_attempts=4,
                retry_delay_seconds=0.5,
                checkpoint_ttl_seconds=120.0,
            ),
        )
        manager = VolnuxCheckPointManager.from_config(config)
        assert manager.checkpoint_interval == 2.5
        assert manager.retry_attempts == 4 and manager.retry_delay == 0.5
        assert manager.snapshot_ttl == 120
        assert manager._semaphore._value == 7

    def test_default_config(self):
        manager = VolnuxCheckPointManager.from_config(CheckPointConfig())
        assert manager.checkpoint_interval == 5.0 and manager.snapshot_ttl == 300


class TestManagerAdditions:
    @pytest.mark.asyncio
    async def test_drain_waits_for_queued_snapshots(self):
        manager = VolnuxCheckPointManager()
        saved = []

        class Snap:
            id = "s1"

            async def save(self, ttl=None):
                await asyncio.sleep(0.01)
                saved.append(self.id)

        await manager.start()
        try:
            assert manager.enqueue(Snap()) is True
            assert manager.queue_depth >= 0
            await manager.drain()
            assert saved == ["s1"] and manager.queue_depth == 0
        finally:
            await manager.stop()

    def test_a_full_queue_drops_instead_of_raising(self, caplog):
        manager = VolnuxCheckPointManager(max_queue_size=1)
        snap = MagicMock(id="s")
        assert manager.enqueue(snap) is True
        assert manager.enqueue(snap) is False
        assert "queue is full" in caplog.text
        assert manager.queue_depth == 1

    @pytest.mark.asyncio
    async def test_no_sleep_after_the_last_failed_attempt(self, monkeypatch):
        sleeps = []

        async def fake_sleep(delay, *a, **k):
            sleeps.append(delay)

        monkeypatch.setattr(
            "volnux.execution.rehydrator.checkpoint_manager.asyncio.sleep", fake_sleep
        )
        manager = VolnuxCheckPointManager(retry_attempts=3, retry_delay=1.0)
        snap = MagicMock(id="s")
        snap.save = AsyncMock(side_effect=RuntimeError("down"))
        await manager._persist_with_retry(snap)
        assert snap.save.await_count == 3
        assert len(sleeps) == 2  # between attempts only

    @pytest.mark.asyncio
    async def test_running_flag(self):
        manager = VolnuxCheckPointManager()
        assert manager.running is False
        await manager.start()
        assert manager.running is True and manager.is_healthy()
        await manager.stop()
        assert manager.running is False


class TestEngineOwnership:
    def test_frequency_is_initialised_from_the_config(self):
        assert (
            DefaultWorkflowEngine()._checkpoint_frequency
            == CheckPointFrequency.PER_TASK
        )
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True,
            checkpoint_config=fast_config(CheckPointFrequency.PERIODIC),
        )
        assert engine._checkpoint_frequency == CheckPointFrequency.PERIODIC

    def test_nothing_is_created_until_the_engine_runs(self):
        engine = DefaultWorkflowEngine(enable_checkpointing=True)
        assert engine.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_without_checkpointing_there_is_no_manager(
        self, memory_store, make_event, make_pipeline
    ):
        seen = {}

        async def process(self):
            seen["manager"] = self.checkpoint_manager
            return True, "ok"

        event = make_event(process, "Plain")
        pipeline = make_pipeline(event.__name__)()
        engine = DefaultWorkflowEngine()
        await engine.execute(pipeline.get_pipeline_state().start, pipeline)
        assert seen["manager"] is None and engine.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_the_engine_builds_starts_and_stops_its_manager(
        self, memory_store, make_event, make_pipeline
    ):
        seen = {}

        async def process(self):
            manager = self.checkpoint_manager
            seen["manager"] = manager
            seen["running"] = manager.running if manager else None
            return True, "ok"

        event = make_event(process, "Owned")
        pipeline = make_pipeline(event.__name__)()
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        result = await engine.execute(pipeline.get_pipeline_state().start, pipeline)

        assert result.status == EngineExecutionResult.COMPLETED
        assert isinstance(seen["manager"], VolnuxCheckPointManager)
        assert seen["running"] is True  # events saw a live manager
        assert seen["manager"].running is False  # stopped on the way out
        assert engine.checkpoint_manager is None  # and released

    @pytest.mark.asyncio
    async def test_a_failing_run_still_stops_the_manager(
        self, memory_store, make_event, make_pipeline
    ):
        seen = {}

        async def process(self):
            seen["manager"] = self.checkpoint_manager
            raise RuntimeError("step failed")

        event = make_event(process, "Failing")
        pipeline = make_pipeline(event.__name__)()
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        await engine.execute(pipeline.get_pipeline_state().start, pipeline)
        assert seen["manager"].running is False and engine.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_an_engine_can_run_twice(
        self, memory_store, make_event, make_pipeline
    ):
        managers = []

        async def process(self):
            managers.append(self.checkpoint_manager)
            return True, "ok"

        event = make_event(process, "Twice")
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        for _ in range(2):
            pipeline = make_pipeline(event.__name__)()
            await engine.execute(pipeline.get_pipeline_state().start, pipeline)
        assert managers[0] is not managers[1]
        assert all(m.running is False for m in managers)

    @pytest.mark.asyncio
    async def test_a_manager_supplied_by_the_caller_is_flushed_but_not_stopped(
        self, memory_store, make_event, make_pipeline
    ):
        async def process(self):
            return True, "ok"

        event = make_event(process, "Supplied")
        pipeline = make_pipeline(event.__name__)()
        manager = VolnuxCheckPointManager(retry_attempts=1, retry_delay=0.0)
        engine = DefaultWorkflowEngine()
        engine.enable_checkpointing(manager)
        try:
            await engine.execute(pipeline.get_pipeline_state().start, pipeline)
            assert manager.running is True  # the caller still owns it
            assert engine.checkpoint_manager is manager
            assert manager.queue_depth == 0  # but its queue was flushed
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_every_event_gets_the_manager_not_only_streamed_ones(
        self, memory_store, make_event, make_pipeline
    ):
        got = []

        async def process(self):
            got.append(self.checkpoint_manager)
            return True, "ok"

        first = make_event(process, "First")
        second = make_event(process, "Second")
        pipeline = make_pipeline("%s -> %s" % (first.__name__, second.__name__))()
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        await engine.execute(pipeline.get_pipeline_state().start, pipeline)
        assert len(got) == 2 and got[0] is got[1] and got[0] is not None

    @pytest.mark.asyncio
    async def test_checkpoints_of_a_run_are_persisted(
        self, memory_store, make_event, make_pipeline
    ):
        async def process(self):
            return True, "ok"

        event = make_event(process, "Persisted")
        pipeline = make_pipeline(event.__name__)()
        engine = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        await engine.execute(pipeline.get_pipeline_state().start, pipeline)

        from volnux.execution.rehydrator.event.snapshot import EventCheckpointSnapshot

        stored = await (await EventCheckpointSnapshot.all()).to_list()
        assert stored and stored[-1].class_path.endswith(event.__name__)

    @pytest.mark.asyncio
    async def test_sub_engines_share_the_roots_manager_and_never_own_it(self):
        root = DefaultWorkflowEngine(
            enable_checkpointing=True, checkpoint_config=fast_config()
        )
        await root._start_checkpointing()
        try:
            child = await root.spawn_sub_engine(MagicMock(), MagicMock())
            assert child.checkpoint_manager is root.checkpoint_manager
            await child._start_checkpointing()  # must not build another
            await child._stop_checkpointing()  # must not stop the root's
            assert root.checkpoint_manager.running is True
            assert child.get_root_engine() is root
        finally:
            await root._stop_checkpointing()
        assert root.checkpoint_manager is None


class TestContextAccessor:
    def test_no_engine_means_no_manager(self):
        from volnux.execution.context import ExecutionContext

        context = ExecutionContext.__new__(ExecutionContext)
        context._engine_ref = None
        assert context.get_checkpoint_manager() is None
