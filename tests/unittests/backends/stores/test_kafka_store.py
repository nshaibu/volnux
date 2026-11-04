"""KafkaStoreBackend integration tests (key-value, queue, pub/sub, streams).

They need a Kafka broker and are skipped when none is reachable. Configure with:

    VOLNUX_TEST_KAFKA_HOST  (default: 127.0.0.1)
    VOLNUX_TEST_KAFKA_PORT  (default: 19092)

For a throwaway single-node KRaft broker (group join is instant with
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0; the default 3s only slows the tests):

    docker run -d -p 127.0.0.1:19092:19092 \\
      -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \\
      -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:9093 \\
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \\
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \\
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \\
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \\
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \\
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \\
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \\
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \\
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:3.8.0
"""

import asyncio
import os
import time
import uuid

import pytest
from formax import BaseModel, InitStrategy

pytest.importorskip("confluent_kafka")

from volnux.backends.messaging.base import StreamOffset  # noqa: E402
from volnux.backends.stores.kafka import KafkaStoreBackend  # noqa: E402
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError  # noqa: E402
from volnux.mixins.key_value_store_integration import (  # noqa: E402
    KeyValueStoreIntegrationMixin,
)
from volnux.mixins.messaging import MessagingBackendIntegrationMixin  # noqa: E402

KAFKA = dict(
    host=os.environ.get("VOLNUX_TEST_KAFKA_HOST", "127.0.0.1"),
    port=int(os.environ.get("VOLNUX_TEST_KAFKA_PORT", "19092")),
    request_timeout=5.0,
)


def new_backend(**options):
    try:
        return KafkaStoreBackend(**KAFKA, **options)
    except Exception as exc:  # no broker: skip rather than fail
        pytest.skip(f"Kafka not reachable at {KAFKA['host']}:{KAFKA['port']}: {exc}")


@pytest.fixture(scope="module")
def backend():
    b = new_backend()
    yield b
    b.close()


def bind(model, backend):
    model._backend_store, model._backend_initialized = backend, True


@pytest.fixture
def Model(backend):
    """A model class with a unique name, so every test owns its topics."""

    class Item(
        MessagingBackendIntegrationMixin, KeyValueStoreIntegrationMixin, BaseModel
    ):
        class Config:
            init_strategy = InitStrategy.DATACLASS

        n: int
        group: str = "a"

    Item.__name__ = Item.__qualname__ = f"T{uuid.uuid4().hex[:10]}"
    bind(Item, backend)
    return Item


async def ns(stream):
    return sorted([o.n async for o in stream])


class TestConfiguration:
    @pytest.mark.asyncio
    async def test_selected_through_key_value_store_config(self, Model):
        from unittest.mock import patch

        config = {
            "ENGINE": "volnux.backends.stores.kafka.KafkaStoreBackend",
            "CONNECTOR_CONFIG": {
                "bootstrap_servers": f"{KAFKA['host']}:{KAFKA['port']}",
                "request_timeout": 5.0,
            },
        }
        Model._backend_store, Model._backend_initialized = None, False
        try:
            with patch.object(Model, "get_backend_config", return_value=config):
                backend = await Model.get_backend()
            assert isinstance(backend, KafkaStoreBackend)

            item = Model(n=3)
            await item.save()
            assert (await Model.get(item.id)).n == 3
            await Model.enqueue(Model(n=4))
            assert (await Model.dequeue(timeout=5)).n == 4
        finally:
            await Model.close_backend()

    def test_missing_bootstrap_server_fails_with_connection_error(self):
        with pytest.raises(ConnectionError):
            KafkaStoreBackend(host="127.0.0.1", port=1, request_timeout=1.0)


# Key-value


class TestKeyValue:
    @pytest.mark.asyncio
    async def test_crud_round_trip(self, Model):
        item = Model(n=1)
        await item.save(force_insert=True)
        assert (await Model.get(item.id)).n == 1

        with pytest.raises(ObjectExistError):
            await item.save(force_insert=True)

        item.n = 2
        await item.save()  # upsert
        assert (await Model.get(item.id)).n == 2

        await item.delete()
        with pytest.raises(ObjectDoesNotExist):
            await Model.get(item.id)
        assert await Model.exists_in_backend(item.id) is False

    @pytest.mark.asyncio
    async def test_update_and_delete_require_an_existing_record(self, Model, backend):
        ghost = Model(n=1)
        with pytest.raises(ObjectDoesNotExist):
            await ghost.update()
        with pytest.raises(ObjectDoesNotExist):
            await ghost.delete()

    @pytest.mark.asyncio
    async def test_another_backend_instance_sees_writes_and_deletes(self, Model):
        other = new_backend()
        try:
            item = Model(n=5)
            await item.save()
            schema = await Model.get_schema_name()
            assert other.get(schema, item.id, Model).n == 5

            await item.delete()
            assert other.exists(schema, item.id) is False
        finally:
            other.close()

    @pytest.mark.asyncio
    async def test_ttl_expires_records_lazily(self, Model, backend):
        schema = await Model.get_schema_name()
        item = Model(n=1)
        backend.insert(schema, item.id, item, ttl=1)
        assert backend.exists(schema, item.id)

        await asyncio.sleep(1.3)

        assert backend.exists(schema, item.id) is False
        assert backend.count(schema) == 0
        with pytest.raises(ObjectDoesNotExist):
            backend.get(schema, item.id, Model)
        backend.insert(schema, item.id, item)  # an expired key can be reused

    @pytest.mark.asyncio
    async def test_filter_count_ordering_and_paging(self, Model):
        for n in (3, 1, 2, 5, 4):
            await Model(n=n, group="a" if n % 2 else "b").save()

        assert await ns(await Model.filter(group="a")) == [1, 3, 5]
        assert await Model.count() == 5

        schema = await Model.get_schema_name()
        backend = Model._backend_store

        async def ordered(**kw):
            stream = backend.filter(schema, Model, **kw)
            return [o.n async for o in stream]

        assert await ordered(order_by="n") == [1, 2, 3, 4, 5]
        assert await ordered(order_by="-n") == [5, 4, 3, 2, 1]
        assert await ordered(order_by="n", limit=2, offset=1) == [2, 3]
        assert backend.count(schema, Model, group="a") == 3

    @pytest.mark.asyncio
    async def test_bulk_operations(self, Model, backend):
        schema = await Model.get_schema_name()
        records = {f"k{i}": Model(n=i) for i in range(4)}

        assert backend.bulk_insert(schema, records) == 4
        with pytest.raises(ObjectExistError):
            backend.bulk_insert(schema, {"k0": Model(n=0)})

        many = backend.get_many(schema, ["k0", "k3", "missing"], Model)
        assert sorted(many) == ["k0", "k3"]

        assert backend.bulk_delete(schema, ["k0", "k1", "missing"]) == 2
        assert backend.count(schema) == 2

        backend.clear_schema(schema)
        assert backend.count(schema) == 0
        assert schema.replace(":", "_") in backend.list_schemas()

    @pytest.mark.asyncio
    async def test_result_stream_loads_through_get_many(self, Model):
        for n in range(5):
            await Model(n=n).save()
        stream = await Model.filter(n__gte=2)
        assert await ns(stream) == [2, 3, 4]

    @pytest.mark.asyncio
    async def test_reload_refreshes_in_place(self, Model):
        item = Model(n=1)
        await item.save()
        stale = Model(n=99)
        stale._id = item.id
        await stale.reload()
        assert stale.n == 1


# Queue


class TestQueue:
    @pytest.mark.asyncio
    async def test_fifo_length_and_peek(self, Model):
        for i in range(4):
            await Model.enqueue(Model(n=i))
        assert await Model.queue_length() == 4
        assert [o.n for o in await Model.queue_range(1, 2)] == [1, 2]
        assert [o.n for o in await Model.queue_range(-2, -1)] == [2, 3]
        assert (await Model.queue_peek()).n == 0
        assert await Model.queue_length() == 4  # peeking consumes nothing

        assert (await Model.dequeue(timeout=5)).n == 0
        assert [o.n for o in await Model.pop_many(limit=2, timeout=2)] == [1, 2]
        assert [o.n for o in await Model.pop_many(timeout=2)] == [3]
        assert await Model.queue_length() == 0
        assert await Model.pop() is None

    @pytest.mark.asyncio
    async def test_timeouts(self, Model):
        t0 = time.perf_counter()
        assert await Model.pop(timeout=1) is None
        assert 0.9 <= time.perf_counter() - t0 < 4

        consumer = asyncio.ensure_future(Model.pop(timeout=0))  # forever
        await asyncio.sleep(0.5)
        assert not consumer.done()
        await Model.enqueue(Model(n=7))
        assert (await asyncio.wait_for(consumer, 10)).n == 7

    @pytest.mark.asyncio
    async def test_concurrent_poppers_never_get_the_same_item(self, Model):
        for i in range(6):
            await Model.enqueue(Model(n=i))

        results = await asyncio.wait_for(
            asyncio.gather(*(Model.pop(timeout=5) for _ in range(6))), 30
        )

        assert sorted(r.n for r in results if r) == list(range(6))

    @pytest.mark.asyncio
    async def test_cancelled_blocking_pop_loses_nothing(self, Model):
        waiter = asyncio.ensure_future(Model.pop(timeout=0))
        await asyncio.sleep(0.5)  # worker is inside a broker fetch
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        await Model.enqueue(Model(n=42))  # arrives while the old fetch may still run
        await asyncio.sleep(2)  # let the orphaned worker finish its slice

        got = await Model.pop(timeout=10)
        assert got is not None and got.n == 42

    @pytest.mark.asyncio
    async def test_push_and_length_do_not_join_the_consumer_group(self, Model, backend):
        """A producer-only process must not take partitions away from consumers."""
        await Model.enqueue(Model(n=1))
        await Model.queue_length()
        await Model.queue_range()

        topic = f"{await Model.get_schema_name()}.q".replace(":", "_")
        group = f"volnux-queue-{topic}"
        (future,) = backend.connector.admin.describe_consumer_groups([group]).values()
        try:
            members = future.result().members
        except Exception:  # group not created at all: nothing joined
            members = []
        assert members == []


# Pub/Sub


class TestPubSub:
    @staticmethod
    async def collect(coro_factory, want, timeout=15):
        got = []

        async def callback(rec):
            got.append(rec.n)

        task = asyncio.ensure_future(coro_factory(callback))
        return task, got

    @staticmethod
    async def wait_for(got, count, timeout=15):
        deadline = time.monotonic() + timeout
        while len(got) < count and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

    @staticmethod
    async def stop(task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_delivery_and_publish_result(self, Model):
        task, got = await self.collect(Model.subscribe, 2)
        await asyncio.sleep(1)
        assert await Model.publish(Model(n=1)) == 1
        await Model.publish(Model(n=2))
        await self.wait_for(got, 2)
        await self.stop(task)
        assert got == [1, 2]

    @pytest.mark.asyncio
    async def test_subscribers_only_see_messages_published_after_subscribing(
        self, Model
    ):
        await Model.publish(Model(n=0))  # published before anyone listens

        task, got = await self.collect(Model.subscribe, 1)
        await asyncio.sleep(1)
        await Model.publish(Model(n=1))
        await self.wait_for(got, 1)
        await asyncio.sleep(0.5)
        await self.stop(task)

        assert got == [1]

    @pytest.mark.asyncio
    async def test_every_subscriber_gets_every_message(self, Model):
        t1, g1 = await self.collect(Model.subscribe, 3)
        t2, g2 = await self.collect(Model.subscribe, 3)
        await asyncio.sleep(1.5)
        for n in (1, 2, 3):
            await Model.publish(Model(n=n))
        await self.wait_for(g1, 3)
        await self.wait_for(g2, 3)
        await self.stop(t1)
        await self.stop(t2)
        assert g1 == g2 == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_psubscribe_matches_patterns_and_new_channels(self, Model, backend):
        prefix = f"events.{uuid.uuid4().hex[:8]}"
        seen = []

        async def drain():
            async with backend.psubscribe(
                f"{prefix}.*", record_class=Model
            ) as messages:
                async for message in messages:
                    seen.append((message.channel, message.pattern, message.data.n))

        task = asyncio.ensure_future(drain())
        await asyncio.sleep(1)
        await backend.publish(
            f"{prefix}.a", Model(n=1)
        )  # channel created after subscribing
        await backend.publish(f"{prefix}.b", Model(n=2))
        await backend.publish(f"other.{uuid.uuid4().hex[:6]}", Model(n=99))  # no match
        deadline = time.monotonic() + 20
        while len(seen) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        await self.stop(task)

        assert sorted(seen) == [
            (f"{prefix}.a", f"{prefix}.*", 1),
            (f"{prefix}.b", f"{prefix}.*", 2),
        ]

    @pytest.mark.asyncio
    async def test_original_channel_name_survives_topic_sanitising(
        self, Model, backend
    ):
        channel = f"volnux:task:cmd:{uuid.uuid4().hex[:6]}"  # ':' is illegal in topics
        seen = []

        async def drain():
            async with backend.subscribe(channel, record_class=Model) as messages:
                async for message in messages:
                    seen.append(message.channel)

        task = asyncio.ensure_future(drain())
        await asyncio.sleep(1)
        await backend.publish(channel, Model(n=1))
        deadline = time.monotonic() + 15
        while not seen and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        await self.stop(task)
        assert seen == [channel]

    @pytest.mark.asyncio
    async def test_exiting_the_context_stops_the_listener(self, Model, backend):
        context = backend.subscribe(await Model.get_schema_name(), record_class=Model)
        async with context:
            assert context._listener is not None
        assert context._listener is None
        assert context._closing


# Streams


class TestStreams:
    @pytest.mark.asyncio
    async def test_append_read_ack(self, Model):
        await Model.stream_ensure_consumer_group("g")
        for n in range(3):
            await Model.stream_append(Model(n=n))
        assert await Model.stream_length() == 3

        batch = await Model.stream_read("g", "c1", count=2, block_ms=5000)
        assert [e.record.n for e in batch] == [0, 1]
        assert all(e.group_name == "g" and e.consumer_id == "c1" for e in batch)
        assert await Model.stream_ack("g", *[e.entry_id for e in batch]) == 2

        rest = await Model.stream_read("g", "c1", count=5, block_ms=5000)
        assert [e.record.n for e in rest] == [2]
        assert await Model.stream_read("g", "c1", block_ms=None) == []

    @pytest.mark.asyncio
    async def test_read_and_ack_is_at_most_once(self, Model):
        await Model.stream_ensure_consumer_group("g")
        await Model.stream_append(Model(n=1))
        got = await Model.stream_read_and_ack("g", "c", count=1, block_ms=5000)
        assert [r.n for r in got] == [1]
        assert await Model.stream_read("g", "c", block_ms=None) == []

    @pytest.mark.asyncio
    async def test_unacknowledged_entries_are_redelivered_after_a_restart(self, Model):
        await Model.stream_ensure_consumer_group("g")
        for n in range(3):
            await Model.stream_append(Model(n=n))

        first = new_backend()
        bind(Model, first)
        seen = await Model.stream_read("g", "c", count=3, block_ms=5000)
        assert [e.record.n for e in seen] == [0, 1, 2]
        first.close()  # the worker dies without acknowledging anything

        second = new_backend()
        try:
            bind(Model, second)
            again = await Model.stream_read("g", "c", count=3, block_ms=10000)
            assert [e.record.n for e in again] == [0, 1, 2]
        finally:
            second.close()

    @pytest.mark.asyncio
    async def test_an_unacknowledged_gap_holds_back_the_commit(self, Model):
        """Kafka commits are cumulative: acking 1 and 2 must not skip entry 0."""
        await Model.stream_ensure_consumer_group("g")
        for n in range(3):
            await Model.stream_append(Model(n=n))

        first = new_backend()
        bind(Model, first)
        entries = await Model.stream_read("g", "c", count=3, block_ms=5000)
        assert (
            await Model.stream_ack("g", entries[1].entry_id, entries[2].entry_id) == 2
        )
        first.close()

        second = new_backend()
        try:
            bind(Model, second)
            again = await Model.stream_read("g", "c", count=3, block_ms=10000)
            assert [e.record.n for e in again] == [0, 1, 2]  # replayed from the gap
        finally:
            second.close()

    @pytest.mark.asyncio
    async def test_fully_acknowledged_entries_are_not_redelivered(self, Model):
        await Model.stream_ensure_consumer_group("g")
        for n in range(2):
            await Model.stream_append(Model(n=n))

        first = new_backend()
        bind(Model, first)
        entries = await Model.stream_read("g", "c", count=2, block_ms=5000)
        await Model.stream_ack("g", *[e.entry_id for e in entries])
        first.close()

        second = new_backend()
        try:
            bind(Model, second)
            assert await Model.stream_read("g", "c", block_ms=2000) == []
        finally:
            second.close()

    @pytest.mark.asyncio
    async def test_consumer_group_start_positions(self, Model):
        for n in range(3):
            await Model.stream_append(Model(n=n))
        await Model.stream_ensure_consumer_group("all", StreamOffset.BEGINNING)
        await Model.stream_ensure_consumer_group("new", StreamOffset.LATEST)
        await Model.stream_append(Model(n=3))
        entries = await Model.stream_read("all", "c", count=1)
        await Model.stream_ensure_consumer_group("after", entries[0].entry_id)

        everything = await Model.stream_read("all", "c", count=10, block_ms=5000)
        latest = await Model.stream_read("new", "c", count=10, block_ms=5000)
        after = await Model.stream_read("after", "c", count=10, block_ms=5000)

        assert [e.record.n for e in everything] == [1, 2, 3]  # 0 was read above
        assert [e.record.n for e in latest] == [3]
        assert [e.record.n for e in after] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_ensure_group_is_idempotent_and_never_rewinds(self, Model):
        await Model.stream_append(Model(n=0))
        await Model.stream_ensure_consumer_group("g", StreamOffset.BEGINNING)
        entries = await Model.stream_read("g", "c", block_ms=5000)
        await Model.stream_ack("g", entries[0].entry_id)

        await Model.stream_ensure_consumer_group("g", StreamOffset.BEGINNING)  # again

        assert await Model.stream_read("g", "c", block_ms=1500) == []

    @pytest.mark.asyncio
    async def test_groups_are_independent(self, Model):
        await Model.stream_ensure_consumer_group("a")
        await Model.stream_ensure_consumer_group("b")
        await Model.stream_append(Model(n=1))

        a = await Model.stream_read("a", "c", block_ms=5000)
        b = await Model.stream_read("b", "c", block_ms=5000)
        assert [e.record.n for e in a] == [e.record.n for e in b] == [1]

    @pytest.mark.asyncio
    async def test_claim_pending_takes_over_stuck_entries(self, Model):
        await Model.stream_ensure_consumer_group("g")
        await Model.stream_append(Model(n=1))
        stuck = await Model.stream_read("g", "worker-that-hung", block_ms=5000)
        assert len(stuck) == 1

        assert await Model.stream_claim_pending("g", "me", min_idle_ms=60_000) == []
        claimed = await Model.stream_claim_pending("g", "me", min_idle_ms=0)

        assert [e.record.n for e in claimed] == [1]
        assert claimed[0].consumer_id == "me"
        assert await Model.stream_ack("g", claimed[0].entry_id) == 1

    @pytest.mark.asyncio
    async def test_read_timeouts(self, Model):
        await Model.stream_ensure_consumer_group("g")

        t0 = time.perf_counter()
        assert await Model.stream_read("g", "c", block_ms=1200) == []
        assert 1.0 <= time.perf_counter() - t0 < 5

        reader = asyncio.ensure_future(Model.stream_read("g", "c", block_ms=0))
        await asyncio.sleep(0.5)
        assert not reader.done()
        await Model.stream_append(Model(n=9))
        assert [e.record.n for e in await asyncio.wait_for(reader, 10)] == [9]

    @pytest.mark.asyncio
    async def test_cancelled_read_does_not_swallow_entries(self, Model):
        await Model.stream_ensure_consumer_group("g")
        waiter = asyncio.ensure_future(Model.stream_read("g", "c", block_ms=0))
        await asyncio.sleep(0.5)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        await Model.stream_append(Model(n=5))
        await asyncio.sleep(2)

        got = await Model.stream_read("g", "c", block_ms=10000)
        assert [e.record.n for e in got] == [5]

    @pytest.mark.asyncio
    async def test_trim_and_max_len(self, Model):
        for n in range(6):
            await Model.stream_append(Model(n=n))
        assert await Model.stream_trim(2) == 4
        assert await Model.stream_length() == 2

        await Model.stream_append(Model(n=6), max_len=1, approximate_trim=False)
        assert await Model.stream_length() == 1

    @pytest.mark.asyncio
    async def test_delete_then_recreate(self, Model):
        await Model.stream_append(Model(n=1))
        assert await Model.stream_delete() is True
        assert await Model.stream_length() == 0
        assert await Model.stream_delete() is False

        await Model.stream_append(Model(n=2))  # the stream can be created again
        assert await Model.stream_length() == 1
