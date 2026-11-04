"""End-to-end tests of MessagingBackendIntegrationMixin on the in-memory backend.

These run the real mixin -> backend -> serialisation path (no mocks), which had
no coverage before: every receive path used to raise AttributeError.
"""

import asyncio

import pytest
from formax import BaseModel, InitStrategy

from volnux.backends.messaging.base import QueueSide
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin


class Msg(MessagingBackendIntegrationMixin, KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    n: int


@pytest.fixture
def bound():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="msg-test")
    previous = (Msg._backend_store, Msg._backend_initialized)
    Msg._backend_store, Msg._backend_initialized = backend, True
    yield backend
    Msg._backend_store, Msg._backend_initialized = previous


def ns(msgs):
    return [m.n for m in msgs]


@pytest.mark.asyncio
async def test_push_pop_round_trip_is_fifo(bound):
    await Msg.push(Msg(n=1), Msg(n=2), Msg(n=3))
    assert (await Msg.pop()).n == 1
    assert (await Msg.pop(side=QueueSide.RIGHT)).n == 3
    assert (await Msg.dequeue()).n == 2
    assert await Msg.pop() is None
    assert await Msg.queue_length() == 0


@pytest.mark.asyncio
async def test_pop_many_respects_limit_exactly(bound):
    await Msg.push(*[Msg(n=i) for i in range(5)])

    assert ns(await Msg.pop_many(limit=3)) == [0, 1, 2]  # was 2 (off by one)
    assert await Msg.queue_length() == 2
    assert ns(await Msg.pop_many()) == [3, 4]
    assert await Msg.pop_many() == []
    assert await Msg.pop_many(limit=0) == []


@pytest.mark.asyncio
async def test_queue_range_and_peek_do_not_remove(bound):
    await Msg.push(*[Msg(n=i) for i in range(5)])

    assert ns(await Msg.queue_range()) == [0, 1, 2, 3, 4]
    assert ns(await Msg.queue_range(1, 2)) == [1, 2]
    assert ns(await Msg.queue_range(-2, -1)) == [3, 4]
    assert ns(await Msg.queue_range(0, -2)) == [0, 1, 2, 3]  # LRANGE semantics
    assert await Msg.queue_range(10, 20) == []
    assert (await Msg.queue_peek()).n == 0
    assert await Msg.queue_length() == 5


@pytest.mark.asyncio
async def test_blocking_pop_on_missing_queue_receives_a_later_push(bound):
    consumer = asyncio.ensure_future(Msg.pop(timeout=2))
    await asyncio.sleep(0.05)
    await Msg.push(Msg(n=42))
    assert (await asyncio.wait_for(consumer, 1)).n == 42


@pytest.mark.asyncio
async def test_pop_timeout_zero_waits_indefinitely(bound):
    consumer = asyncio.ensure_future(Msg.pop(timeout=0))
    await asyncio.sleep(0.1)
    assert not consumer.done()  # was: returned None immediately
    await Msg.push(Msg(n=1))
    assert (await asyncio.wait_for(consumer, 1)).n == 1


@pytest.mark.asyncio
async def test_pop_timeout_expires_with_none(bound):
    t0 = asyncio.get_running_loop().time()
    assert await Msg.pop(timeout=0.1) is None
    assert 0.08 <= asyncio.get_running_loop().time() - t0 < 1


@pytest.mark.asyncio
async def test_competing_waiters_each_get_one_item_and_none_give_up_early(bound):
    waiters = [asyncio.ensure_future(Msg.pop(timeout=2)) for _ in range(3)]
    await asyncio.sleep(0.05)
    await Msg.push(Msg(n=1))
    await asyncio.sleep(0.05)
    await Msg.push(Msg(n=2))
    await asyncio.sleep(0.05)
    await Msg.push(Msg(n=3))

    results = await asyncio.wait_for(asyncio.gather(*waiters), 2)
    assert sorted(ns(results)) == [1, 2, 3]  # no waiter returned a spurious None


@pytest.mark.asyncio
async def test_cancelled_blocking_pop_loses_no_items(bound):
    consumer = asyncio.ensure_future(Msg.pop(timeout=0))
    await asyncio.sleep(0.05)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    await Msg.push(Msg(n=7))
    assert (await Msg.pop()).n == 7


@pytest.mark.asyncio
async def test_publish_subscribe_round_trip_and_on_error(bound):
    got, errors = [], []
    ready = asyncio.Event()

    async def callback(rec):
        if rec.n == 2:
            raise RuntimeError("bad record")
        got.append(rec.n)
        if rec.n == 3:
            raise asyncio.CancelledError  # stop the consumer after record 3

    async def on_error(exc, rec):
        errors.append(rec.n)

    task = asyncio.ensure_future(Msg.subscribe(callback, on_error=on_error))
    await asyncio.sleep(0.05)  # let the subscription register
    assert await Msg.publish(Msg(n=1)) == 1
    await Msg.publish(Msg(n=2))
    await Msg.publish(Msg(n=3))
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)

    assert got == [1, 3]
    assert errors == [2]


@pytest.mark.asyncio
async def test_psubscribe_matches_patterns(bound):
    got = []

    async def callback(rec):
        got.append(rec.n)
        raise asyncio.CancelledError

    task = asyncio.ensure_future(Msg.psubscribe("volnux:*", callback=callback))
    await asyncio.sleep(0.05)
    await Msg.publish(Msg(n=5))
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert got == [5]


@pytest.mark.asyncio
async def test_streams_unsupported_on_inmemory_backend_fail_clearly(bound):
    with pytest.raises(TypeError, match="StreamCapabilityMixin"):
        await Msg.stream_length()
