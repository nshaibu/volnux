import asyncio

import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType
from volnux.engine.workflows.trigger.triggers.condition import ConditionalTrigger

TICK = 0.01


class Flag:
    """A condition whose value the test controls; counts evaluations."""

    def __init__(self, *values):
        self.values = list(values)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        v = self.values[min(self.calls - 1, len(self.values) - 1)]
        if isinstance(v, Exception):
            raise v
        return v


def make(fn, **kw):
    kw.setdefault("poll_interval", TICK)
    t = ConditionalTrigger("wf", fn, **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


async def settle(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.005)


class TestConstruction:
    def test_type_and_defaults(self):
        t = ConditionalTrigger("wf", lambda: True)
        assert t.trigger_type is TriggerType.CONDITION
        assert t.poll_interval == 60.0 and t.edge_trigger is True

    @pytest.mark.parametrize("bad", [0, -1, -0.5])
    def test_rejects_non_positive_interval(self, bad):
        with pytest.raises(ValueError):
            ConditionalTrigger("wf", lambda: True, poll_interval=bad)

    def test_rejects_non_callable(self):
        with pytest.raises(TypeError):
            ConditionalTrigger("wf", True)


@pytest.mark.asyncio
class TestEvaluate:
    async def test_sync_truthiness(self, store):
        for value, expected in [(1, True), (0, False), ("", False), ([1], True)]:
            t, _ = make(lambda v=value: v)
            assert await t._evaluate_condition() is expected

    async def test_async_callable(self, store):
        async def cond():
            return True

        t, _ = make(cond)
        assert await t._evaluate_condition() is True

    async def test_future_is_awaited(self, store):
        async def go():
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(True)
            t, _ = make(lambda: fut)
            return await t._evaluate_condition()

        assert await go() is True

    async def test_exception_returns_none_and_counts_error(self, store):
        t, _ = make(Flag(RuntimeError("x")))
        assert await t._evaluate_condition() is None
        assert t.error_count == 1


@pytest.mark.asyncio
class TestEdgeTrigger:
    async def test_fires_once_while_condition_stays_true(self, store):
        flag = Flag(True)
        t, got = make(flag)
        await t.run()
        await settle(lambda: flag.calls >= 5)
        await t.end()
        assert len(got) == 1

    async def test_fires_again_after_false_then_true(self, store):
        flag = Flag(True, False, True)
        t, got = make(flag)
        await t.run()
        await settle(lambda: len(got) == 2)
        await t.end()
        assert t.fire_count == 2

    async def test_never_fires_while_false(self, store):
        flag = Flag(False)
        t, got = make(flag)
        await t.run()
        await settle(lambda: flag.calls >= 4)
        await t.end()
        assert got == []

    async def test_first_evaluation_is_immediate(self, store):
        t, got = make(Flag(True), poll_interval=3600)
        await t.run()
        await settle(lambda: len(got) == 1)
        await t.end()

    async def test_error_does_not_look_like_a_new_edge(self, store):
        flag = Flag(True, RuntimeError("blip"), True, True)
        t, got = make(flag)
        await t.run()
        await settle(lambda: flag.calls >= 5)
        await t.end()
        assert len(got) == 1
        assert t.error_count == 1


@pytest.mark.asyncio
class TestLevelTrigger:
    async def test_fires_on_every_true_tick(self, store):
        t, got = make(Flag(True), edge_trigger=False)
        await t.run()
        await settle(lambda: len(got) >= 3)
        await t.end()


@pytest.mark.asyncio
class TestActivationPayload:
    async def test_params_and_metadata(self, store):
        t, got = make(
            Flag(True), workflow_params={"report": "monthly"}, metadata={"k": "v"}
        )
        await t.run()
        await settle(lambda: got)
        await t.end()
        params = got[0].workflow_params
        assert params["report"] == "monthly" and params["condition_met"] is True
        assert "checked_at" in params
        assert got[0].activation_source is TriggerType.CONDITION


@pytest.mark.asyncio
class TestDisabledAndErrors:
    async def test_disabled_trigger_does_not_evaluate_or_fire(self, store):
        flag = Flag(True)
        t, got = make(flag, enabled=False)
        await t.run()
        await asyncio.sleep(TICK * 5)
        await t.end()
        assert got == [] and flag.calls == 0

    async def test_failed_activation_is_retried_next_tick(self, store):
        flag = Flag(True)
        t, _ = make(flag)
        attempts = []

        async def flaky(activation):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("engine busy")

        t._on_activate = flaky
        await t.run()
        await settle(lambda: len(attempts) == 2)
        await t.end()
        # counted once by activate(), not a second time by the poll loop
        assert t.error_count == 1

    async def test_no_callback_counts_each_failure_once(self, store):
        t = ConditionalTrigger("wf", Flag(True), poll_interval=TICK)
        await t.run()
        await settle(lambda: t.error_count >= 2)
        await t.end()
        assert t.error_count == t.state.error_count


@pytest.mark.asyncio
class TestLifecycle:
    async def test_start_is_idempotent(self, store):
        t, _ = make(Flag(False))
        await t.start()
        first = t._task
        await t.start()
        assert t._task is first
        await t.stop()

    async def test_stop_cancels_polling_and_is_safe_twice(self, store):
        flag = Flag(False)
        t, _ = make(flag)
        await t.run()
        await settle(lambda: flag.calls >= 2)
        await t.end()
        calls = flag.calls
        await asyncio.sleep(TICK * 4)
        assert flag.calls == calls and t._task is None
        await t.stop()

    async def test_run_and_end_update_the_record(self, store):
        t, _ = make(Flag(False))
        await t.run()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )

    async def test_restart_fires_again_for_still_true_condition(self, store):
        t, got = make(Flag(True))
        await t.start()
        await settle(lambda: len(got) == 1)
        await t.stop()
        await t.start()
        await settle(lambda: len(got) == 2)
        await t.stop()

    async def test_fire_count_and_last_fired_persist(self, store):
        t, got = make(Flag(True))
        await t.run()
        await settle(lambda: got)
        await t.end()
        record = await TriggerStateRecord.get(t.trigger_id)
        assert record.fire_count == 1 and record.last_fired is not None
