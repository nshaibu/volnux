import asyncio

import pytest

from volnux.engine.workflows.trigger.event import TriggerEvent
from volnux.engine.workflows.trigger.triggers import (
    EventTrigger,
    WindowedTrigger,
    WindowSink,
    WorkflowChainTrigger,
    WorkflowStatus,
)
from volnux.engine.workflows.trigger.triggers.chain.workflow import (
    WORKFLOW_COMPLETED_TOPIC,
)

pytestmark = pytest.mark.asyncio


def make(**kw):
    kw.setdefault("parent_workflow", "parent")
    t = WorkflowChainTrigger("child", **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


async def completed(workflow="parent", status="success", topic=None, **extra):
    await TriggerEvent.publish(
        TriggerEvent(
            event_type=topic or WORKFLOW_COMPLETED_TOPIC,
            source="engine",
            data={"workflow_name": workflow, "status": status, **extra},
            correlation_id="corr",
        )
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


async def test_no_event_bus_parameter():
    with pytest.raises(TypeError):
        WorkflowChainTrigger("c", parent_workflow="p", event_bus=object())


async def test_default_status_is_success():
    t, _ = make()
    assert t.on_status == {WorkflowStatus.SUCCESS.value}


async def test_fires_for_parent_success_with_payload(store, message_bus):
    t, got = make()
    await t.run()
    await completed(result={"rows": 3}, execution_id="x1")
    await settle(lambda: got)
    await t.end()
    p = got[0].workflow_params
    assert p["parent_workflow"] == "parent" and p["parent_status"] == "success"
    assert p["parent_result"] == {"rows": 3} and p["parent_execution_id"] == "x1"
    assert p["parent_correlation_id"] == "corr"


async def test_other_workflow_status_and_topic_are_ignored(store, message_bus):
    t, got = make()
    await t.run()
    await completed(workflow="other")
    await completed(status="failure")
    await completed(topic="something.else")
    await quiet()
    await t.end()
    assert got == []


async def test_custom_statuses(store, message_bus):
    t, got = make(on_status=["failure", "timed_out"])
    await t.run()
    await completed(status="success")
    await completed(status="failure")
    await completed(status="timed_out")
    await settle(lambda: len(got) == 2)
    await t.end()


async def test_stop_is_idempotent_and_halts_delivery(store, message_bus):
    t, got = make()
    await t.stop()
    await t.run()
    await t.end()
    await t.stop()
    await completed()
    await quiet()
    assert got == [] and not t.subscribed


async def test_start_is_idempotent(store, message_bus):
    t, got = make()
    await t.start()
    task = t._subscription_task
    await t.start()
    assert t._subscription_task is task
    await t.stop()


async def test_windowed_trigger_routes_events_to_named_aggregators(store, message_bus):
    trades = EventTrigger("_w", ["market.trade"], name="trades")
    quotes = EventTrigger("_w", ["market.quote"], name="quotes")
    window = WindowedTrigger(
        "process",
        aggregators=[trades, quotes],
        sink=WindowSink(condition_fn=lambda state: False, poll_interval=0.01),
    )

    async def cb(activation):
        pass

    window.set_activation_callback(cb)
    await window.run()
    await asyncio.sleep(0.05)
    for _ in range(3):
        await TriggerEvent.publish(TriggerEvent(event_type="market.trade", source="t"))
    await TriggerEvent.publish(TriggerEvent(event_type="market.quote", source="t"))
    await settle(
        lambda: len(window._window_state["trades"]) == 3
        and len(window._window_state["quotes"]) == 1
    )
    await window.end()
