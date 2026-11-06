"""TriggerEngine: stop() ends triggers, the state sync is idempotent, a full queue
drops the activation instead of blocking."""

import asyncio
import datetime

import pytest

from tests.unittests.trigger.test_trigger_base import DummyTrigger, stored, trigger
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerActivation, TriggerType


def engine_with(*triggers, **kwargs):
    engine = TriggerEngine(**kwargs)
    for t in triggers:
        engine.triggers[t.trigger_id] = t
    return engine


@pytest.mark.asyncio
class TestStopEndsTriggers:
    async def test_stop_marks_running_trigger_stopped(self, store):
        t = trigger()
        await t.run()
        await engine_with(t).stop()
        assert t.lifecycle is TriggerLifecycle.STOPPED
        assert (await stored(t)).lifecycle is TriggerLifecycle.STOPPED
        assert t.stopped == 1  # the subclass hook ran

    async def test_stop_ends_paused_trigger(self, store):
        t = trigger()
        await t.run()
        await t.pause()
        await engine_with(t).stop()
        assert (await stored(t)).lifecycle is TriggerLifecycle.STOPPED

    async def test_stop_skips_never_started_trigger(self, store):
        t = trigger()
        await engine_with(t).stop()
        assert t.lifecycle is TriggerLifecycle.CREATED
        assert t.stopped == 0

    async def test_stop_skips_already_stopped_trigger(self, store):
        t = trigger()
        await t.run()
        await t.end()
        await engine_with(t).stop()
        assert t.stopped == 1

    async def test_one_failing_trigger_does_not_block_the_rest(self, store):
        bad, good = trigger(fail_stop=True), trigger()
        await bad.run()
        await good.run()
        await engine_with(bad, good).stop()
        assert (await stored(good)).lifecycle is TriggerLifecycle.STOPPED
        # end() records the stop even though the subclass hook raised
        assert (await stored(bad)).lifecycle is TriggerLifecycle.STOPPED

    async def test_force_stop_ends_triggers(self, store):
        t = trigger()
        await t.run()
        await engine_with(t).force_stop()
        assert (await stored(t)).lifecycle is TriggerLifecycle.STOPPED


@pytest.mark.asyncio
class TestStateSync:
    async def _request(self, t, lifecycle):
        record = await stored(t)
        record.lifecycle = lifecycle
        record.dirty = True
        await record.save()
        return await stored(t)

    async def test_pause_request_is_applied_and_cleared(self, store):
        t = trigger()
        await t.run()
        engine = engine_with(t)
        await engine._apply_state_change(
            await self._request(t, TriggerLifecycle.PAUSED)
        )
        assert t.lifecycle is TriggerLifecycle.PAUSED
        record = await stored(t)
        assert record.dirty is False and record.lifecycle is TriggerLifecycle.PAUSED

    async def test_resume_request_is_applied(self, store):
        t = trigger()
        await t.run()
        await t.pause()
        await engine_with(t)._apply_state_change(
            await self._request(t, TriggerLifecycle.ACTIVE)
        )
        assert t.lifecycle is TriggerLifecycle.ACTIVE

    async def test_stop_request_ends_the_trigger(self, store):
        t = trigger()
        await t.run()
        await engine_with(t)._apply_state_change(
            await self._request(t, TriggerLifecycle.STOPPED)
        )
        assert t.lifecycle is TriggerLifecycle.STOPPED and t.stopped == 1

    async def test_request_matching_current_state_is_a_noop(self, store):
        t = trigger()
        await t.run()
        await engine_with(t)._apply_state_change(
            await self._request(t, TriggerLifecycle.ACTIVE)
        )
        assert t.lifecycle is TriggerLifecycle.ACTIVE
        assert (await stored(t)).dirty is False

    async def test_inapplicable_request_is_dropped_not_retried(self, store):
        t = trigger()
        await t.run()
        await t.end()
        await engine_with(t)._apply_state_change(
            await self._request(t, TriggerLifecycle.PAUSED)
        )
        assert (await stored(t)).dirty is False

    async def test_unknown_trigger_is_cleaned(self, store):
        t = trigger()
        await t.run()
        record = await self._request(t, TriggerLifecycle.PAUSED)
        await engine_with()._apply_state_change(record)
        assert (await stored(t)).dirty is False

    async def test_loop_isolates_failing_records(self, store, monkeypatch):
        bad, good = trigger(), trigger()
        for t in (bad, good):
            await t.run()
            await self._request(t, TriggerLifecycle.PAUSED)
        engine = engine_with(bad, good, state_sync_interval=0.01)
        real = engine._apply_state_change

        async def flaky(record):
            if record.id == bad.trigger_id:
                raise ValueError("boom")
            await real(record)

        monkeypatch.setattr(engine, "_apply_state_change", flaky)
        task = asyncio.create_task(engine._state_sync_loop())
        for _ in range(100):
            await asyncio.sleep(0.01)
            if good.lifecycle is TriggerLifecycle.PAUSED:
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert good.lifecycle is TriggerLifecycle.PAUSED


@pytest.mark.asyncio
class TestHandleActivation:
    def _activation(self, t):
        return TriggerActivation(
            trigger_id=t.trigger_id,
            activated_at=datetime.datetime.now(datetime.timezone.utc),
            activation_source=TriggerType.EVENT,
            workflow_params={"a": 1},
        )

    async def test_enqueues(self, store):
        t = trigger()
        engine = engine_with(t)
        await engine._handle_activation(self._activation(t))
        assert engine.task_queue.get_nowait() == ("wf", {"a": 1})

    async def test_full_queue_does_not_block_and_counts_error(self, store):
        t = trigger()
        await t.run()
        engine = engine_with(t, max_queue_size=1)
        engine.task_queue.put_nowait(("other", {}))
        await asyncio.wait_for(engine._handle_activation(self._activation(t)), 1)
        assert engine.task_queue.qsize() == 1
        assert t.state.error_count == 1
        assert (await stored(t)).error_count == 1

    async def test_unknown_trigger_is_ignored(self, store):
        t = trigger()
        engine = engine_with()
        await engine._handle_activation(self._activation(t))
        assert engine.task_queue.empty()
