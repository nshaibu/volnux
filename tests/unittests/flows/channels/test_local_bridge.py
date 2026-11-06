"""TaskCommunicationBridge behaviour, exercised through the local bridge."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from volnux.flows.bridge.communications.tasks import (
    CommandType,
    LocalTaskCommunicationBridge,
    MessageType,
    TaskCommand,
    TaskExecutionHandle,
    TaskMessage,
    TaskState,
)


def handle(n=1):
    return TaskExecutionHandle(
        task_def_id=f"def-{n}", execution_id=f"exec-{n}", event_name=f"Event{n}"
    )


def status_message(state, task_id="def-1", **payload):
    return TaskMessage(
        task_id=task_id,
        message_type=MessageType.STATUS_UPDATE,
        payload={"state": state.value, **payload},
    )


async def eventually(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


@pytest_asyncio.fixture
async def bridge():
    b = LocalTaskCommunicationBridge(MagicMock())
    yield b
    await b.shutdown()


def test_a_bridge_can_be_built_outside_a_running_loop():
    """Locks/events used to be created in __init__, which binds them to a loop
    on Python 3.8/3.9."""
    LocalTaskCommunicationBridge(MagicMock())


@pytest.mark.asyncio
async def test_registering_a_task_returns_a_channel_and_a_queued_status(bridge):
    channel = await bridge.register_task(handle())

    assert channel is not None
    status = await bridge.get_status("def-1")
    assert status.state == TaskState.QUEUED
    assert status.metadata["event_name"] == "Event1"


@pytest.mark.asyncio
async def test_a_status_update_sent_by_the_event_updates_the_bridge(bridge):
    """Events send state.value ("running"); the bridge looked it up by NAME and
    raised KeyError, and used the bare task id against full-id keys."""
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()

    await channel.send_message(status_message(TaskState.RUNNING))

    assert await eventually(
        lambda: bridge._statuses["def-1::exec-1"].state == TaskState.RUNNING
    )


@pytest.mark.asyncio
async def test_progress_and_error_details_are_recorded(bridge):
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()

    await channel.send_message(
        TaskMessage(
            "def-1",
            MessageType.PROGRESS_UPDATE,
            {"state": "running", "step": 2, "total": 4, "step_name": "_process"},
        )
    )
    assert await eventually(lambda: (bridge._statuses["def-1::exec-1"].progress == 0.5))

    await channel.send_message(
        TaskMessage(
            "def-1",
            MessageType.ERROR,
            {"state": "failed", "error": "kaboom", "step": 3},
        )
    )
    assert await eventually(
        lambda: bridge._statuses["def-1::exec-1"].state == TaskState.FAILED
    )
    assert "kaboom" in bridge._statuses["def-1::exec-1"].message


@pytest.mark.asyncio
async def test_an_unknown_state_is_ignored_not_fatal(bridge):
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()

    await channel.send_message(
        TaskMessage("def-1", MessageType.STATUS_UPDATE, {"state": "banana"})
    )
    await channel.send_message(status_message(TaskState.RUNNING))

    assert await eventually(
        lambda: bridge._statuses["def-1::exec-1"].state == TaskState.RUNNING
    )


@pytest.mark.asyncio
async def test_many_tasks_are_all_served_promptly(bridge):
    """A serial poll of every channel (0.1 s each) made updates for N tasks take
    N * 0.1 s."""
    channels = [await bridge.register_task(handle(n)) for n in range(30)]
    await bridge.start_monitoring()

    t0 = time.perf_counter()
    for n, channel in enumerate(channels):
        await channel.send_message(status_message(TaskState.RUNNING, f"def-{n}"))

    assert await eventually(
        lambda: all(
            bridge._statuses[f"def-{n}::exec-{n}"].state == TaskState.RUNNING
            for n in range(30)
        ),
        timeout=3,
    )
    assert time.perf_counter() - t0 < 1.0


@pytest.mark.asyncio
async def test_tasks_registered_after_monitoring_started_are_monitored(bridge):
    await bridge.start_monitoring()
    channel = await bridge.register_task(handle())

    await channel.send_message(status_message(TaskState.RUNNING))

    assert await eventually(
        lambda: bridge._statuses["def-1::exec-1"].state == TaskState.RUNNING
    )


@pytest.mark.asyncio
async def test_start_monitoring_is_idempotent(bridge):
    await bridge.register_task(handle())
    await bridge.start_monitoring()
    first = dict(bridge._pumps)
    await bridge.start_monitoring()
    assert bridge._pumps == first


@pytest.mark.asyncio
async def test_a_terminal_state_ends_that_tasks_pump(bridge):
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()
    pump = bridge._pumps["def-1::exec-1"]

    await channel.send_message(status_message(TaskState.COMPLETED))

    await asyncio.wait_for(pump, 2)
    assert bridge._statuses["def-1::exec-1"].state == TaskState.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", ["def-1", "exec-1", "def-1::exec-1"])
async def test_commands_reach_the_task_by_any_id(bridge, lookup):
    channel = await bridge.register_task(handle())

    sent = await bridge.send_command(
        lookup, TaskCommand(task_id="ignored", command_type=CommandType.PAUSE)
    )

    assert sent is True
    received = await channel.receive_command(timeout=1)
    assert received.command_type == CommandType.PAUSE
    assert received.task_id == "def-1::exec-1"


@pytest.mark.asyncio
async def test_a_command_for_an_unknown_task_is_refused(bridge):
    assert (
        await bridge.send_command("nope", TaskCommand("nope", CommandType.CANCEL))
        is False
    )


@pytest.mark.asyncio
async def test_is_alive_follows_the_heartbeat(bridge):
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()
    await channel.send_message(status_message(TaskState.RUNNING))
    assert await eventually(
        lambda: bridge._statuses["def-1::exec-1"].state == TaskState.RUNNING
    )
    assert await bridge.is_alive("def-1") is True

    bridge._statuses["def-1::exec-1"].last_heartbeat -= 120
    assert await bridge.is_alive("def-1", timeout=30) is False


@pytest.mark.asyncio
async def test_shutdown_stops_pumps_closes_channels_and_is_idempotent():
    bridge = LocalTaskCommunicationBridge(MagicMock())
    channel = await bridge.register_task(handle())
    closed = []

    async def close():
        closed.append(True)

    channel.close = close
    await bridge.start_monitoring()
    pump = bridge._pumps["def-1::exec-1"]

    await asyncio.wait_for(bridge.shutdown(), 3)
    await asyncio.wait_for(bridge.shutdown(), 3)  # second call is a no-op

    assert pump.done()
    assert closed == [True]


@pytest.mark.asyncio
async def test_shutdown_without_monitoring_does_not_fail():
    bridge = LocalTaskCommunicationBridge(MagicMock())
    await bridge.register_task(handle())
    await asyncio.wait_for(bridge.shutdown(), 3)
