import asyncio

import pytest

from volnux.engine.workflows.trigger.event import TriggerEvent
from volnux.engine.workflows.trigger.event_filters import TimestampFilter
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import (
    ConditionalTrigger,
    EventTrigger,
    LinearChainTrigger,
    ManualTrigger,
    TriggerLifecycle,
    WindowedTrigger,
    WindowSink,
)
from volnux.engine.workflows.trigger.triggers.base import TriggerType


async def settle(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


async def quiet(seconds=0.08):
    await asyncio.sleep(seconds)


# ---------------------------------------------------------------- LinearChain
def linear(n=2, **kw):
    steps = [ManualTrigger(f"step{i}") for i in range(n)]
    chain = LinearChainTrigger("final", steps, **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    chain.set_activation_callback(cb)
    return chain, steps, got


class TestLinearConstruction:
    def test_type(self):
        chain, _, _ = linear()
        assert chain.trigger_type is TriggerType.WORKFLOW_CHAIN

    def test_needs_two_triggers(self):
        with pytest.raises(ValueError, match="at least two"):
            LinearChainTrigger("f", [ManualTrigger("a")])

    @pytest.mark.parametrize("bad", [["x", "y"], [object(), ManualTrigger("a")]])
    def test_steps_must_be_triggers(self, bad):
        with pytest.raises(TypeError, match="trigger"):
            LinearChainTrigger("f", bad)

    def test_same_trigger_twice_is_refused(self):
        a = ManualTrigger("a")
        with pytest.raises(ValueError, match="twice"):
            LinearChainTrigger("f", [a, a])

    @pytest.mark.parametrize("bad", [0, -3])
    def test_step_timeout_must_be_positive(self, bad):
        with pytest.raises(ValueError, match="step_timeout"):
            LinearChainTrigger(
                "f", [ManualTrigger("a"), ManualTrigger("b")], step_timeout=bad
            )

    def test_reducer_must_be_callable(self):
        with pytest.raises(TypeError, match="reducer"):
            LinearChainTrigger(
                "f", [ManualTrigger("a"), ManualTrigger("b")], reducer="nope"
            )


@pytest.mark.asyncio
class TestLinearFlow:
    async def test_only_the_first_step_is_armed_at_start(self, store):
        chain, (a, b), _ = linear()
        await chain.run()
        assert a.lifecycle is TriggerLifecycle.ACTIVE
        assert b.lifecycle is TriggerLifecycle.CREATED
        await chain.end()

    async def test_steps_fire_in_order_then_the_workflow(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        await a.invoke(user="ada")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        assert a.lifecycle is TriggerLifecycle.STOPPED  # disarmed once it fired
        assert got == []
        await b.invoke(user="bob")
        await settle(lambda: got)
        await chain.end()
        params = got[0].workflow_params
        assert params["step_0_invoked_by"] == "ada"
        assert params["step_1_invoked_by"] == "bob"
        assert "__trigger_history__" in params
        assert got[0].activation_source is TriggerType.WORKFLOW_CHAIN

    async def test_a_later_step_cannot_fire_early(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        with pytest.raises(Exception):
            await b.invoke()  # nothing listening yet: no callback installed
        assert got == []
        await chain.end()

    async def test_history_keeps_every_steps_data_apart(self, store):
        chain, (a, b, c), got = linear(3)
        await chain.run()
        for step in (a, b, c):
            await step.invoke(user=step.workflow_name)
            await quiet()
        await settle(lambda: got)
        history = got[0].workflow_params["__trigger_history__"]
        assert [history[f"step_{i}_invoked_by"] for i in range(3)] == [
            "step0",
            "step1",
            "step2",
        ]
        await chain.end()

    async def test_the_chain_rearms_for_the_next_round(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        for round_ in range(3):
            await a.invoke(user=f"a{round_}")
            await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
            await b.invoke(user=f"b{round_}")
            await settle(lambda: len(got) == round_ + 1)
            await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE)
        assert chain.fire_count == 3
        await chain.end()

    async def test_data_from_one_round_does_not_leak_into_the_next(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        await a.invoke(user="first", secret="round-1")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: len(got) == 1)
        await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE)
        await a.invoke(user="second")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: len(got) == 2)
        history = got[1].workflow_params["__trigger_history__"]
        assert history["step_0_invoked_by"] == "second"
        assert "step_0_secret" not in history
        # nor in the parameters the second step was armed with
        assert "secret" not in b.workflow_params and "step_0_secret" not in str(
            b.workflow_params.get("__trigger_history__", "")
        )
        await chain.end()

    async def test_configured_step_params_survive_the_rounds(self, store):
        chain, (a, b), got = linear()
        b.workflow_params = {"fixed": "keep"}
        chain._base_params[1] = {"fixed": "keep"}
        await chain.run()
        for _ in range(2):
            await a.invoke()
            await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
            assert b.workflow_params["fixed"] == "keep"
            await b.invoke()
            await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE)
        await chain.end()

    async def test_custom_reducer_shapes_the_activation(self, store):
        chain, (a, b), got = linear(
            reducer=lambda acc, new: {"count": acc.get("count", 0) + 1}
        )
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: got)
        assert got[0].workflow_params["count"] == 1
        await chain.end()

    async def test_async_reducer_is_awaited(self, store):
        async def reducer(acc, new):
            return {"merged": True}

        chain, (a, b), got = linear(reducer=reducer)
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: got)
        assert got[0].workflow_params["merged"] is True
        await chain.end()

    async def test_a_failing_reducer_restarts_the_chain(self, store):
        def reducer(acc, new):
            raise ValueError("bad reducer")

        chain, (a, b), got = linear(reducer=reducer)
        await chain.run()
        await a.invoke()
        await settle(lambda: chain.error_count == 1)
        await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE)
        assert got == [] and chain._current_step == 0
        await chain.end()

    async def test_a_failing_final_activation_rearms_and_does_not_crash(self, store):
        chain, (a, b), _ = linear()

        async def boom(activation):
            raise RuntimeError("engine busy")

        chain.set_activation_callback(boom)
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: chain.error_count == 1)
        await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE)
        assert chain._running
        await chain.end()

    async def test_a_step_that_cannot_start_restarts_the_chain(self, store):
        chain, (a, b), got = linear()
        real = b.start
        b.start = None  # not awaitable: arming step 1 fails
        await chain.run()
        await a.invoke()
        await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE and chain._running)
        assert chain._current_step == 0
        b.start = real
        await chain.end()

    async def test_a_chain_that_cannot_rearm_is_marked_errored(self, store):
        chain, (a, b), _ = linear()
        await chain.run()
        a.start = None
        b.start = None
        await a.invoke()
        await settle(lambda: chain.lifecycle is TriggerLifecycle.ERROR)
        assert not chain._running

    async def test_a_disabled_step_does_not_advance_the_chain(self, store):
        steps = [ManualTrigger("s0", enabled=False), ManualTrigger("s1")]
        chain = LinearChainTrigger("final", steps)
        got = []

        async def cb(activation):
            got.append(activation)

        chain.set_activation_callback(cb)
        await chain.run()
        with pytest.raises(RuntimeError, match="disabled"):
            await steps[0].invoke()
        assert steps[1].lifecycle is TriggerLifecycle.CREATED and got == []
        await chain.end()


@pytest.mark.asyncio
class TestLinearTimeouts:
    async def test_a_slow_second_step_resets_the_chain(self, store):
        chain, (a, b), got = linear(step_timeout=0.1)
        await chain.run()
        await a.invoke(user="ada")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await settle(lambda: a.lifecycle is TriggerLifecycle.ACTIVE, timeout=3)
        assert chain._current_step == 0 and chain._accumulated_data == {}
        assert got == []
        await chain.end()

    async def test_first_step_has_no_timeout(self, store):
        chain, (a, b), _ = linear(step_timeout=0.05)
        await chain.run()
        await quiet(0.2)
        assert a.lifecycle is TriggerLifecycle.ACTIVE and chain._timeout_task is None
        await chain.end()

    async def test_firing_in_time_cancels_the_timeout(self, store):
        chain, (a, b), got = linear(step_timeout=0.3)
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: got)
        await quiet(0.4)
        assert len(got) == 1
        await chain.end()


@pytest.mark.asyncio
class TestLinearStop:
    async def test_stop_disarms_and_forgets_progress(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await chain.end()
        assert b.lifecycle is TriggerLifecycle.STOPPED
        assert chain._current_step == 0 and chain._accumulated_data == {}
        assert got == []

    async def test_a_restart_begins_at_step_zero_with_nothing_accumulated(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        await a.invoke(user="stale")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await chain.end()

        await chain.run()
        assert chain._current_step == 0 and chain._accumulated_data == {}
        await a.invoke(user="fresh")
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: got)
        assert (
            got[0].workflow_params["__trigger_history__"]["step_0_invoked_by"]
            == "fresh"
        )
        await chain.end()

    async def test_an_activation_arriving_after_stop_is_discarded(self, store):
        from datetime import datetime, timezone

        from volnux.engine.workflows.trigger.triggers.base import TriggerActivation

        chain, (a, b), got = linear()
        await chain.run()
        late = a._on_activate  # the chain's callback for step 0
        await chain.end()
        await late(
            TriggerActivation(
                trigger_id=a.trigger_id,
                activated_at=datetime.now(timezone.utc),
                activation_source=TriggerType.MANUAL,
            )
        )
        await quiet()
        assert got == [] and b.lifecycle is not TriggerLifecycle.ACTIVE

    async def test_stop_before_start_is_safe(self, store):
        chain, _, _ = linear()
        await chain.stop()

    async def test_background_tasks_are_tracked_and_released(self, store):
        chain, (a, b), got = linear()
        await chain.run()
        await a.invoke()
        await settle(lambda: b.lifecycle is TriggerLifecycle.ACTIVE)
        await b.invoke()
        await settle(lambda: got)
        await settle(lambda: not chain._background)
        await chain.end()

    async def test_run_and_end_update_the_record(self, store):
        chain, _, _ = linear()
        await chain.run()
        assert (await TriggerStateRecord.get(chain.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await chain.end()
        assert (await TriggerStateRecord.get(chain.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )

    async def test_a_failed_start_leaves_the_chain_not_running(self, store):
        chain, (a, b), _ = linear()
        a.start = None
        with pytest.raises(Exception):
            await chain.run()
        assert not chain._running
        assert chain.lifecycle is TriggerLifecycle.ERROR


# ------------------------------------------------------------- WindowSink etc.
def flow_events(trades=3, quotes=1):
    async def go():
        for i in range(trades):
            await TriggerEvent.publish(
                TriggerEvent(event_type="market.trade", source="t", data={"i": i})
            )
        for i in range(quotes):
            await TriggerEvent.publish(
                TriggerEvent(event_type="market.quote", source="t", data={"i": i})
            )

    return go()


def windowed(condition=None, **kw):
    trades = EventTrigger("_w", ["market.trade"], name="trades")
    quotes = EventTrigger("_w", ["market.quote"], name="quotes")
    sink = WindowSink(
        condition
        or (lambda state: len(state["trades"]) >= 3 and len(state["quotes"]) >= 1),
        poll_interval=0.01,
    )
    w = WindowedTrigger("process", [trades, quotes], sink, **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    w.set_activation_callback(cb)
    return w, got, trades, quotes, sink


class TestWindowSink:
    def test_it_is_a_conditional_trigger(self):
        sink = WindowSink(lambda s: True)
        assert isinstance(sink, ConditionalTrigger)
        assert sink.trigger_type is TriggerType.CONDITION

    def test_condition_must_be_callable(self):
        with pytest.raises(TypeError):
            WindowSink("nope")

    @pytest.mark.parametrize("bad", [0, -1])
    def test_poll_interval_must_be_positive(self, bad):
        with pytest.raises(ValueError):
            WindowSink(lambda s: True, poll_interval=bad)

    @pytest.mark.asyncio
    async def test_false_until_a_window_is_injected(self):
        sink = WindowSink(lambda s: True)
        assert await sink._evaluate_condition() is False

    @pytest.mark.asyncio
    async def test_condition_sees_the_window_state(self):
        seen = []
        sink = WindowSink(lambda s: seen.append(s) or True)
        sink._inject_state_reader(lambda: {"a": [1]})
        assert await sink._evaluate_condition() is True
        assert seen == [{"a": [1]}]

    @pytest.mark.asyncio
    async def test_async_condition_is_awaited(self):
        async def cond(state):
            return len(state["a"]) == 2

        sink = WindowSink(cond)
        sink._inject_state_reader(lambda: {"a": [1, 2]})
        assert await sink._evaluate_condition() is True

    @pytest.mark.asyncio
    async def test_a_raising_condition_keeps_the_edge_state(self, store):
        sink = WindowSink(lambda s: 1 / 0)
        sink._inject_state_reader(lambda: {})
        assert await sink._evaluate_condition() is None
        assert sink.error_count == 1

    def test_a_new_window_resets_edge_detection(self):
        sink = WindowSink(lambda s: True)
        sink._last_condition_state = True
        sink._inject_state_reader(lambda: {})
        assert sink._last_condition_state is False


class TestWindowedConstruction:
    def test_type(self):
        w, *_ = windowed()
        assert w.trigger_type is TriggerType.WORKFLOW_CHAIN

    def test_needs_an_aggregator(self):
        with pytest.raises(ValueError, match="at least one"):
            WindowedTrigger("p", [], WindowSink(lambda s: True))

    def test_aggregators_need_names(self):
        with pytest.raises(ValueError, match="name"):
            WindowedTrigger(
                "p", [EventTrigger("_w", ["a"])], WindowSink(lambda s: True)
            )

    def test_aggregator_names_must_be_unique(self):
        with pytest.raises(ValueError, match="unique"):
            WindowedTrigger(
                "p",
                [
                    EventTrigger("_w", ["a"], name="x"),
                    EventTrigger("_w", ["b"], name="x"),
                ],
                WindowSink(lambda s: True),
            )

    def test_sink_must_be_a_window_sink(self):
        with pytest.raises(TypeError, match="WindowSink"):
            WindowedTrigger(
                "p",
                [EventTrigger("_w", ["a"], name="x")],
                ConditionalTrigger("w", lambda: True),
            )

    @pytest.mark.parametrize("bad", [0, -1])
    def test_window_timeout_must_be_positive(self, bad):
        with pytest.raises(ValueError, match="window_timeout"):
            windowed(window_timeout=bad)


@pytest.mark.asyncio
class TestWindowedFlow:
    async def test_events_are_routed_into_named_slots_and_the_window_fires(
        self, store, message_bus
    ):
        w, got, *_ = windowed()
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(trades=3, quotes=1)
        await settle(lambda: got)
        params = got[0].workflow_params
        assert [len(params["window_state"][k]) for k in ("trades", "quotes")] == [3, 1]
        assert params["window_epoch"] and params["window_closed_at"]
        assert got[0].activation_source is TriggerType.WORKFLOW_CHAIN
        await w.end()

    async def test_the_next_window_starts_empty(self, store, message_bus):
        w, got, *_ = windowed()
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: len(got) == 1)
        await asyncio.sleep(0.1)
        await flow_events(3, 1)
        await settle(lambda: len(got) == 2)
        # each window holds only its own events
        assert [
            len(got[1].workflow_params["window_state"][k]) for k in ("trades", "quotes")
        ] == [3, 1]
        assert (
            got[1].workflow_params["window_epoch"]
            > got[0].workflow_params["window_epoch"]
        )
        await w.end()

    async def test_condition_not_met_means_no_activation(self, store, message_bus):
        w, got, *_ = windowed()
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(trades=2, quotes=1)  # needs 3 trades
        await quiet(0.2)
        assert got == []
        await w.end()

    async def test_each_window_arms_the_aggregators_again(self, store, message_bus):
        w, got, trades, quotes, sink = windowed()
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: got)
        await settle(lambda: trades.subscribed and quotes.subscribed)
        await w.end()

    async def test_a_failed_workflow_activation_still_rearms(self, store, message_bus):
        w, got, trades, *_ = windowed()
        calls = []

        async def flaky(activation):
            calls.append(1)
            raise RuntimeError("engine busy")

        w.set_activation_callback(flaky)
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: calls)
        await settle(lambda: trades.subscribed)
        assert w._running and w.error_count == 1
        await w.end()

    async def test_run_and_end_update_the_record(self, store, message_bus):
        w, *_ = windowed()
        await w.run()
        assert (await TriggerStateRecord.get(w.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await w.end()
        assert (await TriggerStateRecord.get(w.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )


@pytest.mark.asyncio
class TestWindowTimeout:
    async def test_a_timed_out_window_resets_without_firing_and_reopens(
        self, store, message_bus
    ):
        w, got, trades, *_ = windowed(window_timeout=0.15)
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(trades=2, quotes=0)
        await settle(lambda: w._generation >= 2)  # the window rolled over
        assert got == []
        assert all(len(v) == 0 for v in w._window_state.values())
        # and the trigger is still alive: the new window is armed and can fire
        await settle(lambda: trades.subscribed)
        assert w._running
        await w.end()

    async def test_a_later_window_can_still_fire_after_a_timeout(
        self, store, message_bus
    ):
        w, got, *_ = windowed(window_timeout=0.3)
        await w.run()
        await asyncio.sleep(0.05)
        await settle(lambda: w._generation >= 2, timeout=5)
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: got)
        await w.end()

    async def test_firing_cancels_the_timeout(self, store, message_bus):
        w, got, *_ = windowed(window_timeout=0.4)
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: got)
        generation = w._generation
        await quiet(0.1)
        assert generation <= w._generation  # no spurious extra close for old window
        assert len(got) == 1
        await w.end()

    async def test_a_stale_close_is_ignored(self, store, message_bus):
        w, got, *_ = windowed()
        await w.run()
        stale = w._generation - 1
        await w._close_window(stale, timed_out=True)
        assert w._generation == stale + 1  # nothing closed
        await w.end()

    async def test_a_close_after_stop_is_ignored(self, store, message_bus):
        w, got, *_ = windowed()
        await w.run()
        generation = w._generation
        await w.end()
        await w._close_window(generation, timed_out=False)
        assert got == []


@pytest.mark.asyncio
class TestWindowStop:
    async def test_stop_disarms_everything(self, store, message_bus):
        w, got, trades, quotes, sink = windowed(window_timeout=5)
        await w.run()
        await w.end()
        assert not trades.subscribed and not quotes.subscribed
        assert sink._task is None and w._timeout_task is None
        assert w._running is False

    async def test_nothing_is_collected_after_stop(self, store, message_bus):
        w, got, *_ = windowed()
        await w.run()
        await asyncio.sleep(0.05)
        await w.end()
        await flow_events(3, 1)
        await quiet(0.15)
        assert got == [] and all(len(v) == 0 for v in w._window_state.values())

    async def test_stop_leaves_no_background_tasks(self, store, message_bus):
        w, got, *_ = windowed(window_timeout=5)
        await w.run()
        await asyncio.sleep(0.05)
        await flow_events(3, 1)
        await settle(lambda: got)
        await w.end()
        assert not [t for t in w._background if not t.done()]

    async def test_a_failed_start_leaves_it_not_running(
        self, store, message_bus, monkeypatch
    ):
        w, *_ = windowed()

        async def boom():
            raise RuntimeError("cannot open")

        monkeypatch.setattr(w, "_open_window", boom)
        with pytest.raises(RuntimeError):
            await w.run()
        assert not w._running and w.lifecycle is TriggerLifecycle.ERROR

    async def test_an_aggregator_that_cannot_arm_is_counted(self, store, message_bus):
        w, got, trades, *_ = windowed()
        trades.start = None
        await w.run()
        assert w.error_count >= 1
        await w.end()


@pytest.mark.asyncio
class TestWindowEpoch:
    async def test_the_epoch_is_stamped_on_timestamp_filters(self, store, message_bus):
        stamp = TimestampFilter(start=0, end=60)
        agg = EventTrigger("_w", ["a"], name="a", event_filter=stamp)
        w = WindowedTrigger("p", [agg], WindowSink(lambda s: False))
        await w.run()
        assert stamp.window_epoch == w._window_epoch
        await w.end()

    async def test_nested_composite_filters_are_stamped_too(self, store, message_bus):
        from volnux.engine.workflows.trigger.event_filters import (
            CompositeFilter,
            TypeFilter,
        )

        stamp = TimestampFilter(start=0, end=60)
        nested = CompositeFilter(
            [CompositeFilter([TypeFilter(["a"]), stamp], "AND")], "AND"
        )
        agg = EventTrigger(
            "_w", ["a"], name="a", event_filter=nested, event_pattern={"k": 1}
        )
        w = WindowedTrigger("p", [agg], WindowSink(lambda s: False))
        await w.run()
        assert stamp.window_epoch is not None
        await w.end()

    async def test_only_events_inside_the_time_slice_are_collected(
        self, store, message_bus
    ):
        from datetime import datetime, timezone

        early = EventTrigger(
            "_w", ["m"], name="early", event_filter=TimestampFilter(start=0, end=60)
        )
        late = EventTrigger(
            "_w", ["m"], name="late", event_filter=TimestampFilter(start=60, end=None)
        )
        w = WindowedTrigger("p", [early, late], WindowSink(lambda s: False))
        await w.run()
        await asyncio.sleep(0.05)
        epoch = w._window_epoch.timestamp()
        for offset in (10, 90):  # numeric event_time = Unix seconds
            await TriggerEvent.publish(
                TriggerEvent(
                    event_type="m", source="t", data={"event_time": epoch + offset}
                )
            )
        await settle(lambda: w._window_state["early"] and w._window_state["late"])
        assert len(w._window_state["early"]) == 1 and len(w._window_state["late"]) == 1
        await w.end()


class TestTimestampExtractor:
    def test_numeric_event_time_is_accepted(self):
        from volnux.engine.workflows.trigger.event_filters.timestamp import (
            _default_extractor,
        )

        event = TriggerEvent(
            event_type="x", source="s", data={"event_time": 1700000000.5}
        )
        assert _default_extractor(event) == 1700000000.5

    def test_booleans_are_not_timestamps(self):
        from volnux.engine.workflows.trigger.event_filters.timestamp import (
            _default_extractor,
        )

        event = TriggerEvent(event_type="x", source="s", data={"event_time": True})
        with pytest.raises(ValueError):
            _default_extractor(event)

    def test_iso_and_ingestion_fallbacks_still_work(self):
        from volnux.engine.workflows.trigger.event_filters.timestamp import (
            _default_extractor,
        )

        iso = TriggerEvent(
            event_type="x", source="s", data={"event_time": "2024-01-02T03:04:05+00:00"}
        )
        assert _default_extractor(iso).year == 2024
        plain = TriggerEvent(event_type="x", source="s")
        assert _default_extractor(plain) == plain.timestamp
