"""LocalCommandChannel: usable from the engine loop AND from another thread's loop.

Thread-executor events run on a private event loop (asyncio.run in a worker
thread), so the channel they receive commands on is created on the engine loop
but consumed on another. asyncio.Queue cannot do that.
"""

import asyncio
import threading
import time

import pytest

from volnux.flows.bridge.communications.tasks import (
    CommandType,
    MessageType,
    TaskCommand,
    TaskMessage,
)
from volnux.flows.bridge.communications.tasks.channels.local import LocalCommandChannel


def command(kind=CommandType.PAUSE, task_id="t"):
    return TaskCommand(task_id=task_id, command_type=kind)


def message(state="running", task_id="t"):
    return TaskMessage(
        task_id=task_id,
        message_type=MessageType.STATUS_UPDATE,
        payload={"state": state},
    )


def run_in_thread(coro_factory):
    """Run a coroutine on a brand-new event loop in another thread."""
    box = {}

    def target():
        try:
            box["result"] = asyncio.run(coro_factory())
        except BaseException as exc:  # noqa: BLE001 - re-raised by the caller
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, box


@pytest.mark.asyncio
async def test_commands_and_messages_round_trip_in_one_loop():
    channel = LocalCommandChannel("t")

    await channel.send_command(command())
    await channel.send_message(message())

    assert (await channel.receive_command(timeout=1)).command_type == CommandType.PAUSE
    assert (await channel.receive_message(timeout=1)).payload["state"] == "running"


@pytest.mark.asyncio
async def test_receive_returns_none_on_timeout():
    channel = LocalCommandChannel("t")
    t0 = time.perf_counter()
    assert await channel.receive_command(timeout=0.1) is None
    assert await channel.receive_message(timeout=0.1) is None
    assert 0.18 <= time.perf_counter() - t0 < 1.0


@pytest.mark.asyncio
async def test_a_zero_timeout_never_blocks():
    channel = LocalCommandChannel("t")
    t0 = time.perf_counter()
    assert await channel.receive_command(timeout=0) is None
    assert time.perf_counter() - t0 < 0.05


@pytest.mark.asyncio
async def test_items_are_delivered_in_order():
    channel = LocalCommandChannel("t")
    for kind in (CommandType.PAUSE, CommandType.RESUME, CommandType.CANCEL):
        await channel.send_command(command(kind))

    got = [(await channel.receive_command(timeout=1)).command_type for _ in range(3)]

    assert got == [CommandType.PAUSE, CommandType.RESUME, CommandType.CANCEL]


@pytest.mark.asyncio
async def test_a_command_sent_from_the_engine_loop_reaches_another_threads_loop():
    channel = LocalCommandChannel("t")
    received = {}

    async def listener():
        t0 = time.perf_counter()
        received["command"] = await channel.receive_command(timeout=5)
        received["latency"] = time.perf_counter() - t0

    thread, box = run_in_thread(listener)
    await asyncio.sleep(0.2)  # the listener is now waiting on its own loop
    await channel.send_command(command(CommandType.CANCEL))
    await asyncio.to_thread(thread.join, 5)

    assert "error" not in box, box.get("error")
    assert received["command"].command_type == CommandType.CANCEL
    assert received["latency"] < 1.0  # woken immediately, not at the 5 s timeout


@pytest.mark.asyncio
async def test_a_message_sent_from_another_thread_reaches_the_engine_loop():
    channel = LocalCommandChannel("t")

    async def worker():
        await channel.send_message(message("completed"))

    thread, box = run_in_thread(worker)

    got = await channel.receive_message(timeout=5)
    thread.join(5)

    assert "error" not in box, box.get("error")
    assert got.payload["state"] == "completed"


@pytest.mark.asyncio
async def test_cancelling_a_waiting_receive_does_not_lose_the_next_item():
    channel = LocalCommandChannel("t")
    waiter = asyncio.ensure_future(channel.receive_command(timeout=5))
    await asyncio.sleep(0.05)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    await channel.send_command(command(CommandType.RESUME))

    assert (await channel.receive_command(timeout=1)).command_type == CommandType.RESUME


@pytest.mark.asyncio
async def test_one_item_wakes_exactly_one_of_several_waiters():
    channel = LocalCommandChannel("t")
    waiters = [
        asyncio.ensure_future(channel.receive_command(timeout=0.5)) for _ in range(3)
    ]
    await asyncio.sleep(0.05)
    await channel.send_command(command())

    results = await asyncio.gather(*waiters)

    assert sum(r is not None for r in results) == 1


@pytest.mark.asyncio
async def test_a_waiter_woken_then_cancelled_hands_the_item_to_the_next_waiter():
    channel = LocalCommandChannel("t")
    first = asyncio.ensure_future(channel.receive_command(timeout=5))
    second = asyncio.ensure_future(channel.receive_command(timeout=5))
    await asyncio.sleep(0.05)

    await channel.send_command(command())
    first.cancel()  # woken for the item, but cancelled before it consumed it

    got = await asyncio.wait_for(second, 2)
    assert got is not None


@pytest.mark.asyncio
async def test_the_channel_does_not_depend_on_the_loop_that_created_it():
    """Created outside any loop (as a pickled/rebuilt channel would be)."""
    channel = await asyncio.to_thread(LocalCommandChannel, "t")
    await channel.send_command(command())
    assert (await channel.receive_command(timeout=1)) is not None


def test_a_child_channel_is_the_same_kind_with_its_own_id():
    parent = LocalCommandChannel("parent")
    child = parent.create_child("tool-1")
    assert type(child) is LocalCommandChannel and child.task_id == "tool-1"
    assert child is not parent


@pytest.mark.asyncio
async def test_poll_interval_lets_the_listener_sleep_between_polls():
    assert LocalCommandChannel("t").poll_interval >= 0.1
