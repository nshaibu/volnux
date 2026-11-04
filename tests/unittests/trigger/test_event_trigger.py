import asyncio
import datetime

import pytest

from volnux.engine.workflows.trigger.event import TriggerEvent
from volnux.engine.workflows.trigger.event_filters import (
    CompositeFilter,
    TimestampFilter,
    TypeFilter,
)
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import EventTrigger, TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType

pytestmark = pytest.mark.asyncio


def make(types=("order.created",), **kw):
    t = EventTrigger("wf", list(types), **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


async def publish(event_type="order.created", **data):
    await TriggerEvent.publish(
        TriggerEvent(event_type=event_type, source="t", data=data)
    )


async def settle(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


async def quiet():
    await asyncio.sleep(0.05)


class TestConstruction:
    async def test_type_and_name(self):
        t = EventTrigger("wf", ["a"], name="agg")
        assert t.trigger_type is TriggerType.EVENT and t.name == "agg"

    async def test_requires_event_types(self):
        with pytest.raises(ValueError):
            EventTrigger("wf", [])

    async def test_no_event_bus_parameter(self):
        with pytest.raises(TypeError):
            EventTrigger("wf", ["a"], event_bus=object())

    async def test_event_types_are_copied(self):
        types = ["a"]
        t = EventTrigger("wf", types)
        types.append("b")
        assert t.event_types == ["a"]

    async def test_filter_composition(self):
        assert EventTrigger("wf", ["a"])._filter is None
        assert EventTrigger("wf", ["a"], event_pattern={"k": 1})._filter is not None
        both = EventTrigger(
            "wf", ["a"], event_pattern={"k": 1}, event_filter=TypeFilter(["a"])
        )._filter
        assert isinstance(both, CompositeFilter) and both.operator == "AND"


class TestDelivery:
    async def test_matching_type_activates(self, store, message_bus):
        t, got = make()
        await t.run()
        await publish(amount=5)
        await settle(lambda: got)
        await t.end()
        assert len(got) == 1

    async def test_other_types_are_ignored(self, store, message_bus):
        t, got = make()
        await t.run()
        await publish("order.deleted")
        await quiet()
        await t.end()
        assert got == []

    async def test_several_types(self, store, message_bus):
        t, got = make(["a", "b"])
        await t.run()
        await publish("a")
        await publish("b")
        await publish("c")
        await settle(lambda: len(got) == 2)
        await quiet()
        await t.end()
        assert len(got) == 2

    async def test_wildcard_accepts_everything(self, store, message_bus):
        t, got = make(["*"])
        await t.run()
        await publish("anything.at.all")
        await settle(lambda: got)
        await t.end()

    async def test_no_event_before_start_or_after_stop(self, store, message_bus):
        t, got = make()
        await publish()
        await t.run()
        await t.end()
        await publish()
        await quiet()
        assert got == []


class TestFilters:
    async def test_pattern_match_and_mismatch(self, store, message_bus):
        t, got = make(event_pattern={"region": "eu", "vip": True})
        await t.run()
        await publish(region="eu", vip=False)
        await publish(region="eu")
        await publish(region="eu", vip=True, extra=1)
        await settle(lambda: got)
        await quiet()
        await t.end()
        assert len(got) == 1
        assert got[0].workflow_params["event_data"]["extra"] == 1

    async def test_extra_filter_combined_with_pattern(self, store, message_bus):
        only_a = CompositeFilter([TypeFilter(["a"])], "OR")
        t, got = make(["a", "b"], event_pattern={"k": 1}, event_filter=only_a)
        await t.run()
        await publish("a", k=1)
        await publish("b", k=1)
        await publish("a", k=2)
        await settle(lambda: got)
        await quiet()
        await t.end()
        assert [g.workflow_params["event_type"] for g in got] == ["a"]

    async def test_timestamp_filter_is_usable(self, store, message_bus):
        t, got = make(event_filter=TimestampFilter(start=0, end=3600))
        t._filter.set_epoch(datetime.datetime.now(datetime.timezone.utc))
        await t.run()
        await publish()
        await settle(lambda: got)
        await t.end()


class TestActivation:
    async def test_payload(self, store, message_bus):
        t, got = make(workflow_params={"fixed": 1}, metadata={"m": "v"})
        await t.run()
        event = TriggerEvent(
            event_type="order.created",
            source="shop",
            data={"id": 7},
            metadata={"trace": "x"},
        )
        await TriggerEvent.publish(event)
        await settle(lambda: got)
        await t.end()
        params = got[0].workflow_params
        assert params["fixed"] == 1
        assert params["event_id"] == event.event_id
        assert params["event_source"] == "shop"
        assert params["event_data"] == {"id": 7}
        assert params["event_metadata"] == {"trace": "x"}
        assert params["correlation_id"].count(":") == 3
        assert got[0].activation_source is TriggerType.EVENT

    async def test_fire_count_and_last_fired_persist(self, store, message_bus):
        t, got = make()
        await t.run()
        await publish()
        await publish()
        await settle(lambda: len(got) == 2)
        await t.end()
        record = await TriggerStateRecord.get(t.trigger_id)
        assert record.fire_count == 2 and record.last_fired is not None

    async def test_disabled_trigger_does_not_fire(self, store, message_bus):
        t, got = make(enabled=False)
        await t.run()
        await publish()
        await quiet()
        await t.end()
        assert got == []

    async def test_failed_activation_keeps_listening(self, store, message_bus):
        t, _ = make()
        seen = []

        async def flaky(activation):
            seen.append(1)
            if len(seen) == 1:
                raise RuntimeError("busy")

        t._on_activate = flaky
        await t.run()
        await publish()
        await publish()
        await settle(lambda: len(seen) == 2)
        await t.end()
        assert t.error_count == 1

    async def test_no_callback_counts_error_and_keeps_listening(
        self, store, message_bus
    ):
        t = EventTrigger("wf", ["order.created"])
        await t.run()
        await publish()
        await publish()
        await settle(lambda: t.error_count == 2)
        await t.end()


class TestLifecycle:
    async def test_start_is_idempotent(self, store, message_bus):
        t, got = make()
        await t.start()
        task = t._subscription_task
        await t.start()
        assert t._subscription_task is task
        await publish()
        await settle(lambda: got)
        await quiet()
        assert len(got) == 1  # one subscription, one delivery
        await t.stop()

    async def test_stop_is_idempotent_and_clears_the_task(self, store, message_bus):
        t, _ = make()
        await t.stop()
        await t.start()
        await t.stop()
        await t.stop()
        assert t._subscription_task is None and not t.subscribed

    async def test_restart_resumes_delivery(self, store, message_bus):
        t, got = make()
        await t.start()
        await t.stop()
        await t.start()
        await publish()
        await settle(lambda: got)
        await t.stop()

    async def test_two_triggers_each_receive_the_event(self, store, message_bus):
        (a, got_a), (b, got_b) = make(), make()
        await a.run()
        await b.run()
        await publish()
        await settle(lambda: got_a and got_b)
        await a.end()
        await b.end()

    async def test_run_and_end_update_the_record(self, store, message_bus):
        t, _ = make()
        await t.run()
        assert t.subscribed
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert not t.subscribed
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )

    async def test_subscription_failure_marks_the_trigger_error(
        self, store, message_bus, monkeypatch
    ):
        async def broken(*args, **kwargs):
            raise ConnectionError("backend gone")

        monkeypatch.setattr(TriggerEvent, "subscribe", broken)
        t, _ = make()
        await t.run()
        await settle(lambda: t._subscription_task.done())
        assert t.lifecycle is TriggerLifecycle.ERROR
        assert not t.subscribed
        await t.stop()
