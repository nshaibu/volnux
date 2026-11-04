"""The in-memory stream capability: an append-only log with consumer groups."""

import asyncio
import time

import pytest
from formax import BaseModel, InitStrategy

from volnux.backends.messaging.base import StreamCapabilityMixin, StreamOffset
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins import KeyValueStoreIntegrationMixin

pytestmark = pytest.mark.asyncio


class Msg(KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    n: int


@pytest.fixture
def backend():
    b = InMemoryKeyValueStoreBackend()
    yield b
    b.close()


async def fill(backend, count, key="s"):
    return [await backend.stream_append(key, Msg(n=i)) for i in range(count)]


async def read(backend, group, count=10, key="s", block_ms=None, consumer="c1"):
    return await backend.stream_read(
        key, group, Msg, consumer_id=consumer, count=count, block_ms=block_ms
    )


def numbers(entries):
    return [e.record.n for e in entries]


def test_the_backend_has_the_stream_capability(backend):
    assert isinstance(backend, StreamCapabilityMixin)


class TestAppend:
    async def test_ids_increase_and_length_counts(self, backend):
        ids = await fill(backend, 3)
        assert len(set(ids)) == 3 and ids == sorted(
            ids, key=lambda i: int(i.split("-")[0])
        )
        assert await backend.stream_length("s") == 3

    async def test_unknown_stream_has_no_length(self, backend):
        assert await backend.stream_length("nope") == 0

    async def test_max_len_keeps_only_the_newest(self, backend):
        for i in range(10):
            await backend.stream_append("s", Msg(n=i), max_len=4)
        await backend.stream_ensure_consumer_group("s", "g")
        assert numbers(await read(backend, "g")) == [6, 7, 8, 9]

    async def test_streams_are_independent(self, backend):
        await fill(backend, 2, "a")
        await fill(backend, 3, "b")
        assert await backend.stream_length("a") == 2
        assert await backend.stream_length("b") == 3


class TestGroups:
    async def test_a_group_reads_in_order_and_each_entry_once(self, backend):
        await fill(backend, 5)
        await backend.stream_ensure_consumer_group("s", "g")
        assert numbers(await read(backend, "g", count=2)) == [0, 1]
        assert numbers(await read(backend, "g", count=2)) == [2, 3]
        assert numbers(await read(backend, "g")) == [4]
        assert await read(backend, "g") == []

    async def test_groups_do_not_share_a_cursor(self, backend):
        await fill(backend, 3)
        for group in ("a", "b"):
            await backend.stream_ensure_consumer_group("s", group)
        assert numbers(await read(backend, "a")) == [0, 1, 2]
        assert numbers(await read(backend, "b")) == [0, 1, 2]

    async def test_competing_consumers_split_the_work(self, backend):
        await fill(backend, 4)
        await backend.stream_ensure_consumer_group("s", "g")
        first = await read(backend, "g", count=2, consumer="c1")
        second = await read(backend, "g", count=2, consumer="c2")
        assert sorted(numbers(first) + numbers(second)) == [0, 1, 2, 3]
        assert not set(numbers(first)) & set(numbers(second))

    async def test_join_at_the_beginning_replays_history(self, backend):
        await fill(backend, 3)
        await backend.stream_ensure_consumer_group("s", "late", StreamOffset.BEGINNING)
        assert numbers(await read(backend, "late")) == [0, 1, 2]

    async def test_join_at_latest_sees_only_new_entries(self, backend):
        await fill(backend, 3)
        await backend.stream_ensure_consumer_group("s", "g", StreamOffset.LATEST)
        assert await read(backend, "g") == []
        await backend.stream_append("s", Msg(n=99))
        assert numbers(await read(backend, "g")) == [99]

    async def test_join_at_an_entry_id(self, backend):
        ids = await fill(backend, 4)
        await backend.stream_ensure_consumer_group("s", "g", ids[1])
        assert numbers(await read(backend, "g")) == [2, 3]

    async def test_ensuring_twice_keeps_the_position(self, backend):
        await fill(backend, 3)
        await backend.stream_ensure_consumer_group("s", "g")
        await read(backend, "g", count=2)
        await backend.stream_ensure_consumer_group("s", "g")
        assert numbers(await read(backend, "g")) == [2]

    async def test_a_consumer_may_join_before_the_producer_writes(self, backend):
        await backend.stream_ensure_consumer_group("s", "g")
        await fill(backend, 2)
        assert numbers(await read(backend, "g")) == [0, 1]

    async def test_entries_carry_their_group_and_consumer(self, backend):
        await fill(backend, 1)
        entry = (await read(backend, "g", consumer="worker-7"))[0]
        assert (entry.group_name, entry.consumer_id) == ("g", "worker-7")


class TestBlocking:
    async def test_no_block_returns_immediately(self, backend):
        started = time.monotonic()
        assert await read(backend, "g", block_ms=None) == []
        assert time.monotonic() - started < 0.2

    async def test_a_blocked_read_wakes_on_append(self, backend):
        await backend.stream_ensure_consumer_group("s", "g")
        reader = asyncio.create_task(read(backend, "g", block_ms=2000))
        await asyncio.sleep(0.05)
        await backend.stream_append("s", Msg(n=7))
        assert numbers(await asyncio.wait_for(reader, 1)) == [7]

    async def test_a_blocked_read_times_out(self, backend):
        started = time.monotonic()
        assert await read(backend, "g", block_ms=100) == []
        assert 0.08 <= time.monotonic() - started < 1

    async def test_zero_blocks_until_data_arrives(self, backend):
        reader = asyncio.create_task(read(backend, "g", block_ms=0))
        await asyncio.sleep(0.1)
        assert not reader.done()
        await backend.stream_append("s", Msg(n=1))
        assert numbers(await asyncio.wait_for(reader, 1)) == [1]

    async def test_several_blocked_readers_in_one_group_get_one_each(self, backend):
        await backend.stream_ensure_consumer_group("s", "g")
        readers = [
            asyncio.create_task(
                read(backend, "g", count=1, block_ms=1000, consumer=f"c{i}")
            )
            for i in range(3)
        ]
        await asyncio.sleep(0.05)
        await fill(backend, 3)
        results = [await asyncio.wait_for(r, 2) for r in readers]
        got = sorted(n for entries in results for n in numbers(entries))
        assert got == [0, 1, 2]


class TestAcknowledgement:
    async def test_ack_counts_and_is_idempotent(self, backend):
        ids = await fill(backend, 2)
        await read(backend, "g")
        assert await backend.stream_ack("s", "g", *ids) == 2
        assert await backend.stream_ack("s", "g", *ids) == 0

    async def test_ack_on_unknown_stream_or_group(self, backend):
        assert await backend.stream_ack("nope", "g", "1-0") == 0
        await fill(backend, 1)
        assert await backend.stream_ack("s", "nobody", "1-0") == 0

    async def test_unacked_entries_can_be_claimed_after_idling(self, backend):
        await fill(backend, 3)
        await read(backend, "g", consumer="dead")
        await asyncio.sleep(0.06)
        claimed = await backend.stream_claim_pending(
            "s", "g", "rescuer", Msg, min_idle_ms=50
        )
        assert numbers(claimed) == [0, 1, 2]
        assert {e.consumer_id for e in claimed} == {"rescuer"}

    async def test_entries_that_are_not_idle_yet_are_not_stolen(self, backend):
        await fill(backend, 2)
        await read(backend, "g", consumer="alive")
        assert (
            await backend.stream_claim_pending(
                "s", "g", "other", Msg, min_idle_ms=60_000
            )
            == []
        )

    async def test_acknowledged_entries_are_not_claimable(self, backend):
        ids = await fill(backend, 2)
        await read(backend, "g")
        await backend.stream_ack("s", "g", ids[0])
        await asyncio.sleep(0.03)
        claimed = await backend.stream_claim_pending("s", "g", "r", Msg, min_idle_ms=10)
        assert numbers(claimed) == [1]

    async def test_claim_respects_count(self, backend):
        await fill(backend, 5)
        await read(backend, "g")
        await asyncio.sleep(0.03)
        claimed = await backend.stream_claim_pending(
            "s", "g", "r", Msg, min_idle_ms=10, count=2
        )
        assert len(claimed) == 2


class TestTrimAndDelete:
    async def test_trim_evicts_the_oldest(self, backend):
        await fill(backend, 6)
        assert await backend.stream_trim("s", 2) == 4
        assert await backend.stream_length("s") == 2
        assert await backend.stream_trim("s", 5) == 0
        assert await backend.stream_trim("nope", 1) == 0

    async def test_delete_removes_the_stream_and_its_groups(self, backend):
        await fill(backend, 2)
        await backend.stream_ensure_consumer_group("s", "g")
        assert await backend.stream_delete("s") is True
        assert await backend.stream_delete("s") is False
        assert await backend.stream_length("s") == 0
        await fill(backend, 1)
        assert numbers(await read(backend, "g")) == [0]  # a fresh group, from the start

    async def test_delete_wakes_blocked_readers(self, backend):
        await backend.stream_ensure_consumer_group("s", "g")
        reader = asyncio.create_task(read(backend, "g", block_ms=0))
        await asyncio.sleep(0.05)
        await backend.stream_delete("s")
        await asyncio.sleep(0.05)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


class TestReadAndAck:
    async def test_read_and_ack_is_at_most_once(self, backend):
        await fill(backend, 2)
        records = await backend.stream_read_and_ack(
            "s", "g", Msg, count=5, block_ms=None
        )
        assert [r.n for r in records] == [0, 1]
        assert (
            await backend.stream_claim_pending("s", "g", "r", Msg, min_idle_ms=0) == []
        )
