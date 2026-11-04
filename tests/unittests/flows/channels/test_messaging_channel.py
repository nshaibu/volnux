"""Command channel and bridge over a messaging backend (Redis/Kafka in
production, the in-memory backend here).

A subclass of the in-memory backend flagged ``supports_cross_process`` stands
in for a real distributed backend: same queue semantics, one process.
"""

import asyncio
import pickle
import time
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.flows.bridge.communications.tasks import (
    ChannelUnavailable,
    CommandType,
    LocalTaskCommunicationBridge,
    MessageType,
    MessagingTaskCommunicationBridge,
    TaskCommand,
    TaskExecutionHandle,
    TaskMessage,
    TaskState,
    create_communication_bridge,
)
from volnux.flows.bridge.communications.tasks.channels.messaging import (
    ChannelEnvelope,
    MessagingCommandChannel,
)


class FakeDistributedBackend(InMemoryKeyValueStoreBackend):
    supports_cross_process = True


@pytest_asyncio.fixture
async def backend():
    store = FakeDistributedBackend(namespace_prefix="chan-test")
    async with ChannelEnvelope.change_backend(store):
        yield store


def handle(n=1):
    return TaskExecutionHandle(f"def-{n}", f"exec-{n}", f"Event{n}")


def status(state, task_id="def-1", **payload):
    return TaskMessage(
        task_id, MessageType.STATUS_UPDATE, {"state": state.value, **payload}
    )


async def eventually(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


# envelope


@pytest.mark.asyncio
async def test_envelopes_round_trip_commands_and_messages(backend):
    command = TaskCommand("t", CommandType.UPDATE_PRIORITY, {"priority": 3})
    message = TaskMessage(
        "t", MessageType.PROGRESS_UPDATE, {"state": "running", "step": 2, "total": 6}
    )

    restored_command = ChannelEnvelope.from_command(command).to_command()
    restored_message = ChannelEnvelope.from_message(message).to_message()

    assert restored_command.command_type == CommandType.UPDATE_PRIORITY
    assert restored_command.payload == {"priority": 3}
    assert restored_command.timestamp == command.timestamp
    assert restored_message.message_type == MessageType.PROGRESS_UPDATE
    assert restored_message.payload["total"] == 6


# channel


@pytest.mark.asyncio
async def test_commands_and_messages_travel_through_the_backend(backend):
    channel = MessagingCommandChannel("t", inbox="flow-1")

    await channel.send_command(TaskCommand("t", CommandType.PAUSE))
    await channel.send_message(status(TaskState.RUNNING, "t"))

    assert (await channel.receive_command(timeout=1)).command_type == CommandType.PAUSE
    assert (await channel.receive_message(timeout=1)).payload["state"] == "running"


@pytest.mark.asyncio
async def test_delivery_is_fifo(backend):
    channel = MessagingCommandChannel("t", inbox="flow-1")
    for kind in (CommandType.PAUSE, CommandType.RESUME, CommandType.CANCEL):
        await channel.send_command(TaskCommand("t", kind))

    got = [(await channel.receive_command(timeout=1)).command_type for _ in range(3)]

    assert got == [CommandType.PAUSE, CommandType.RESUME, CommandType.CANCEL]


@pytest.mark.asyncio
async def test_timeout_semantics_match_the_channel_contract(backend):
    channel = MessagingCommandChannel("t", inbox="flow-1")

    t0 = time.perf_counter()
    assert await channel.receive_command(timeout=0) is None  # only checks
    assert time.perf_counter() - t0 < 0.1

    t0 = time.perf_counter()
    assert await channel.receive_command(timeout=0.2) is None  # waits, then None
    assert time.perf_counter() - t0 >= 0.15


@pytest.mark.asyncio
async def test_a_command_sent_while_the_task_waits_is_delivered_at_once(backend):
    channel = MessagingCommandChannel("t", inbox="flow-1")
    waiter = asyncio.ensure_future(channel.receive_command(timeout=5))
    await asyncio.sleep(0.1)

    t0 = time.perf_counter()
    await channel.send_command(TaskCommand("t", CommandType.CANCEL))
    got = await asyncio.wait_for(waiter, 3)

    assert got.command_type == CommandType.CANCEL
    assert time.perf_counter() - t0 < 1.0


@pytest.mark.asyncio
async def test_tasks_have_separate_command_queues(backend):
    a = MessagingCommandChannel("task-a", inbox="flow-1")
    b = MessagingCommandChannel("task-b", inbox="flow-1")

    await a.send_command(TaskCommand("task-a", CommandType.PAUSE))

    assert await b.receive_command(timeout=0) is None
    assert (await a.receive_command(timeout=1)).command_type == CommandType.PAUSE


@pytest.mark.asyncio
async def test_a_channel_pickles_small_and_works_after_unpickling(backend):
    """It crosses into a pool worker or a Celery worker: only keys travel, the
    backend is resolved again on the other side."""
    channel = MessagingCommandChannel("t", inbox="flow-1")
    blob = pickle.dumps(channel)
    assert len(blob) < 600

    clone = pickle.loads(blob)
    await channel.send_command(TaskCommand("t", CommandType.RESUME))

    assert (await clone.receive_command(timeout=1)).command_type == CommandType.RESUME


def test_the_listener_polls_slowly_over_a_network_transport():
    assert MessagingCommandChannel("t", inbox="x").poll_interval >= 0.5


def test_a_child_channel_shares_the_inbox_but_has_its_own_commands():
    parent = MessagingCommandChannel("parent", inbox="flow-1")
    child = parent.create_child("tool-1")
    assert child.task_id == "tool-1" and child.inbox == "flow-1"


# bridge


@pytest_asyncio.fixture
async def bridge(backend):
    b = MessagingTaskCommunicationBridge(MagicMock())
    yield b
    await b.shutdown()


@pytest.mark.asyncio
async def test_status_updates_flow_through_one_shared_inbox(bridge):
    channels = [await bridge.register_task(handle(n)) for n in range(1, 6)]
    await bridge.start_monitoring()
    assert len(bridge._pumps) == 1  # one pump for all tasks, not one per task

    for n, channel in enumerate(channels, start=1):
        await channel.send_message(status(TaskState.RUNNING, f"def-{n}"))

    assert await eventually(
        lambda: all(
            bridge._statuses[f"def-{n}::exec-{n}"].state == TaskState.RUNNING
            for n in range(1, 6)
        )
    )


@pytest.mark.asyncio
async def test_a_command_reaches_the_task_through_the_backend(bridge):
    channel = await bridge.register_task(handle())

    assert await bridge.send_command("def-1", TaskCommand("x", CommandType.PAUSE))

    received = await channel.receive_command(timeout=1)
    assert received.command_type == CommandType.PAUSE


@pytest.mark.asyncio
async def test_shutdown_deletes_the_queues_and_stops_the_pump(backend):
    bridge = MessagingTaskCommunicationBridge(MagicMock())
    channel = await bridge.register_task(handle())
    await bridge.start_monitoring()
    await bridge.send_command("def-1", TaskCommand("x", CommandType.PAUSE))
    await channel.send_message(status(TaskState.RUNNING))

    await asyncio.wait_for(bridge.shutdown(), 5)

    assert all(p.done() for p in bridge._pumps.values())
    assert all(
        len(q) == 0 for q in [backend._queues.get(k) or [] for k in backend._queues]
    )


@pytest.mark.asyncio
async def test_without_a_cross_process_backend_no_channel_is_offered():
    plain = InMemoryKeyValueStoreBackend(namespace_prefix="plain")
    async with ChannelEnvelope.change_backend(plain):
        bridge = MessagingTaskCommunicationBridge(MagicMock())
        with pytest.raises(ChannelUnavailable, match="cross-process"):
            await bridge.register_task(handle())


@pytest.mark.asyncio
async def test_a_backend_without_queues_offers_no_channel():
    class NoQueues:
        supports_cross_process = True

    async with ChannelEnvelope.change_backend(NoQueues()):
        bridge = MessagingTaskCommunicationBridge(MagicMock())
        with pytest.raises(ChannelUnavailable, match="queue"):
            await bridge.register_task(handle())


# factory


@pytest.mark.parametrize(
    "kind, expected",
    [
        ("local", LocalTaskCommunicationBridge),
        ("thread", LocalTaskCommunicationBridge),
        ("process", MessagingTaskCommunicationBridge),
        ("celery", MessagingTaskCommunicationBridge),
        ("messaging", MessagingTaskCommunicationBridge),
    ],
)
def test_the_factory_maps_executor_kinds_to_the_two_bridges(kind, expected):
    assert type(create_communication_bridge(MagicMock(), kind)) is expected


def test_the_removed_bridges_are_gone():
    with pytest.raises(ValueError):
        create_communication_bridge(MagicMock(), "remote")
    for module in ("process", "celery", "remote"):
        with pytest.raises(ImportError):
            __import__(f"volnux.flows.bridge.communications.tasks.{module}")


# backend capability


@pytest.mark.asyncio
async def test_queue_delete_removes_a_queue_and_its_contents():
    from tests.unittests.backends.messaging.stores.test_inmemory_messaging import Msg

    backend = InMemoryKeyValueStoreBackend(namespace_prefix="qd")
    await backend.push("doomed", Msg(n=1), Msg(n=2))
    assert await backend.queue_length("doomed") == 2

    assert await backend.queue_delete("doomed") is True
    assert await backend.queue_length("doomed") == 0
    assert await backend.queue_delete("doomed") is False  # already gone


def test_only_distributed_backends_claim_cross_process_support():
    from volnux.backends.stores.redis import RedisStoreBackend

    assert InMemoryKeyValueStoreBackend.supports_cross_process is False
    assert RedisStoreBackend.supports_cross_process is True
