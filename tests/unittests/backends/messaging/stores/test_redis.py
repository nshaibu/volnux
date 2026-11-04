import asyncio
import queue
import threading
import time
from unittest.mock import MagicMock

import orjson as json
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from volnux.backends.messaging.base import (
    Message,
    QueueSide,
    StreamEntry,
    StreamOffset,
)
from volnux.backends.messaging.stores.redis import (
    RedisStorePubSubMixin,
    RedisStorePushPopMixin,
    RedisStoreSubscriptionContext,
    RedisStreamCapabilityMixin,
)
from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import SerializationError


class Item:
    def __init__(self, id, name=""):
        self.id = id
        self.name = name

    def __getstate__(self):
        return {"id": self.id, "name": self.name}

    def __setstate__(self, state):
        self.id = state["id"]
        self.name = state["name"]

    def __eq__(self, other):
        return isinstance(other, Item) and self.__getstate__() == other.__getstate__()


def payload(id, name=""):
    # Redis returns str with decode_responses=True (the connector default)
    return json.dumps({"id": id, "name": name}).decode()


class Unserializable:
    def __getstate__(self):
        raise TypeError("cannot serialize")


class FakeBackend(
    RedisStreamCapabilityMixin, RedisStorePubSubMixin, RedisStorePushPopMixin
):
    """Messaging mixins over a mocked redis client, with real serialization."""

    _serialize_record = KeyValueStoreBackendBase._serialize_record
    _deserialize_record = KeyValueStoreBackendBase._deserialize_record

    def __init__(self, cursor, pool_size=10):
        self.cursor = cursor
        self.connector = MagicMock()
        self.connector.get_cursor.return_value = cursor
        self.connector.config.pool_size = pool_size
        self.ensure_threads = []

    def _ensure_connected(self):
        self.ensure_threads.append(threading.current_thread())


@pytest.fixture
def cursor():
    return MagicMock()


@pytest.fixture
def backend(cursor):
    return FakeBackend(cursor)


def thread_recorder(return_value=None):
    """side_effect that records the calling thread's name."""
    names = []

    def side_effect(*args, **kwargs):
        names.append(threading.current_thread().name)
        return return_value

    return names, side_effect


class TestCommandExecution:
    @pytest.mark.asyncio
    async def test_ensure_connected_runs_off_event_loop(self, backend, cursor):
        cursor.llen.return_value = 0
        await backend.queue_length("q")
        assert backend.ensure_threads
        assert all(t is not threading.main_thread() for t in backend.ensure_threads)

    @pytest.mark.asyncio
    async def test_blocking_commands_use_dedicated_executor(self, backend, cursor):
        names, cursor.blpop.side_effect = thread_recorder(None)
        await backend.pop("q", Item, timeout=1)
        assert names[0].startswith("volnux-messaging-block")

    @pytest.mark.asyncio
    async def test_non_blocking_commands_use_default_pool(self, backend, cursor):
        names, cursor.lpop.side_effect = thread_recorder(None)
        await backend.pop("q", Item)
        assert not names[0].startswith("volnux-messaging-block")

    @pytest.mark.asyncio
    async def test_blocking_concurrency_capped_below_pool_size(self, cursor):
        backend = FakeBackend(cursor, pool_size=3)  # limit = 3 - 2 = 1
        lock = threading.Lock()
        active = {"now": 0, "max": 0}

        def slow_blpop(*args, **kwargs):
            with lock:
                active["now"] += 1
                active["max"] = max(active["max"], active["now"])
            time.sleep(0.05)
            with lock:
                active["now"] -= 1
            return None

        cursor.blpop.side_effect = slow_blpop
        await asyncio.gather(*(backend.pop("q", Item, timeout=0.05) for _ in range(4)))
        assert active["max"] == 1
        assert cursor.blpop.call_count == 4

    def test_semaphore_minimum_is_one(self, cursor):
        backend = FakeBackend(cursor, pool_size=1)

        async def get():
            return backend._get_blocking_semaphore()

        assert asyncio.run(get())._value == 1

    def test_semaphore_recreated_per_event_loop(self, backend):
        async def get():
            sem = backend._get_blocking_semaphore()
            assert backend._get_blocking_semaphore() is sem
            return sem

        first = asyncio.run(get())
        second = asyncio.run(get())
        assert first is not second
        assert first._value == 8


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_serializes_record(self, backend, cursor):
        cursor.publish.return_value = 2
        assert await backend.publish("events", Item("1", "a")) == 2
        channel, sent = cursor.publish.call_args.args
        assert channel == "events"
        assert json.loads(sent) == {"id": "1", "name": "a"}

    @pytest.mark.asyncio
    async def test_publish_wraps_redis_errors(self, backend, cursor):
        cursor.publish.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="Failed to publish to events: down"):
            await backend.publish("events", Item("1"))

    @pytest.mark.asyncio
    async def test_publish_serialization_error_propagates(self, backend, cursor):
        with pytest.raises(SerializationError):
            await backend.publish("events", Unserializable())
        cursor.publish.assert_not_called()


class TestSubscribeFactories:
    def test_subscribe(self, backend):
        ctx = backend.subscribe("a", "b", record_class=Item)
        assert isinstance(ctx, RedisStoreSubscriptionContext)
        assert ctx.channels == ["a", "b"]
        assert ctx.patterns == []
        assert ctx.record_class is Item

    def test_psubscribe(self, backend):
        ctx = backend.psubscribe("a:*", record_class=Item)
        assert ctx.channels == []
        assert ctx.patterns == ["a:*"]

    def test_subscribe_requires_channel(self, backend):
        with pytest.raises(ValueError, match="at least one channel"):
            backend.subscribe(record_class=Item)

    def test_psubscribe_requires_pattern(self, backend):
        with pytest.raises(ValueError, match="at least one pattern"):
            backend.psubscribe(record_class=Item)

    def test_creating_context_needs_no_event_loop(self, backend):
        # Construction is synchronous; the loop is only captured on __aenter__.
        backend.subscribe("a", record_class=Item)


class FakePubSub:
    """Thread-safe stand-in for redis-py's PubSub."""

    def __init__(self):
        self.inbox = queue.Queue()
        self.subscribed = []
        self.psubscribed = []
        self.closed = threading.Event()
        self.get_message_calls = 0

    def subscribe(self, *channels):
        self.subscribed.extend(channels)

    def psubscribe(self, *patterns):
        self.psubscribed.extend(patterns)

    def get_message(self, ignore_subscribe_messages=False, timeout=0.0):
        self.get_message_calls += 1
        try:
            item = self.inbox.get(timeout=timeout)
        except queue.Empty:
            return None
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        self.closed.set()

    def send(self, channel, data, pattern=None):
        self.inbox.put(
            {
                "type": "pmessage" if pattern else "message",
                "channel": channel,
                "pattern": pattern,
                "data": data,
            }
        )


@pytest.fixture
def pubsub(cursor, monkeypatch):
    fake = FakePubSub()
    cursor.pubsub.return_value = fake
    monkeypatch.setattr(RedisStoreSubscriptionContext, "_POLL_INTERVAL", 0.02)
    return fake


async def next_message(sub, timeout=2):
    return await asyncio.wait_for(sub.__anext__(), timeout)


class TestSubscriptionContext:
    @pytest.mark.asyncio
    async def test_receives_messages(self, backend, cursor, pubsub):
        async with backend.subscribe("events", record_class=Item) as sub:
            assert pubsub.subscribed == ["events"]
            cursor.pubsub.assert_called_once_with(ignore_subscribe_messages=True)
            pubsub.send("events", payload("1", "a"))
            msg = await next_message(sub)

        assert isinstance(msg, Message)
        assert msg.channel == "events"
        assert msg.pattern is None
        assert msg.data == Item("1", "a")
        assert msg.raw == payload("1", "a")

    @pytest.mark.asyncio
    async def test_pattern_subscription_decodes_bytes(self, backend, pubsub):
        async with backend.psubscribe("events:*", record_class=Item) as sub:
            assert pubsub.psubscribed == ["events:*"]
            pubsub.send(b"events:x", payload("1").encode(), pattern=b"events:*")
            msg = await next_message(sub)

        assert msg.channel == "events:x"
        assert msg.pattern == "events:*"
        assert msg.data == Item("1")

    @pytest.mark.asyncio
    async def test_messages_arrive_in_order(self, backend, pubsub):
        async with backend.subscribe("events", record_class=Item) as sub:
            for i in range(5):
                pubsub.send("events", payload(str(i)))
            ids = [(await next_message(sub)).data.id for _ in range(5)]
        assert ids == ["0", "1", "2", "3", "4"]

    @pytest.mark.asyncio
    async def test_non_message_types_ignored(self, backend, pubsub):
        async with backend.subscribe("events", record_class=Item) as sub:
            pubsub.inbox.put({"type": "subscribe", "channel": "events", "data": 1})
            pubsub.send("events", payload("1"))
            assert (await next_message(sub)).data.id == "1"

    @pytest.mark.asyncio
    async def test_exit_closes_pubsub_and_stops_listener(self, backend, pubsub):
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            listener = ctx._listener
        assert pubsub.closed.is_set()
        assert listener.done()
        assert ctx._pubsub is None

    @pytest.mark.asyncio
    async def test_iteration_stops_after_exit(self, backend, pubsub):
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            pass
        with pytest.raises(StopAsyncIteration):
            await next_message(ctx)
        # Stays stopped rather than hanging
        with pytest.raises(StopAsyncIteration):
            await next_message(ctx)

    @pytest.mark.asyncio
    async def test_async_for_ends_when_listener_stops(self, backend, pubsub):
        received = []
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            pubsub.send("events", payload("1"))
            assert (await next_message(ctx)).data.id == "1"
            ctx._closing = True  # simulate shutdown requested elsewhere
            async for msg in ctx:
                received.append(msg)
        assert received == []

    @pytest.mark.asyncio
    async def test_unexpected_listener_error_surfaces(self, backend, pubsub):
        async with backend.subscribe("events", record_class=Item) as sub:
            pubsub.inbox.put(RedisConnectionError("socket closed"))
            with pytest.raises(ConnectionError, match="terminated unexpectedly"):
                await next_message(sub)

    @pytest.mark.asyncio
    async def test_open_failure_raises_connection_error(self, backend, cursor):
        cursor.pubsub.side_effect = RedisConnectionError("refused")
        with pytest.raises(ConnectionError, match="Failed to subscribe: refused"):
            async with backend.subscribe("events", record_class=Item):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_failure_closes_pubsub(self, backend, pubsub):
        pubsub.subscribe = MagicMock(side_effect=RedisConnectionError("boom"))
        with pytest.raises(ConnectionError, match="Failed to subscribe"):
            async with backend.subscribe("events", record_class=Item):
                pass
        assert pubsub.closed.is_set()

    @pytest.mark.asyncio
    async def test_cannot_enter_twice(self, backend, pubsub):
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            pass
        with pytest.raises(RuntimeError, match="cannot be entered twice"):
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_iterating_before_enter(self, backend):
        ctx = backend.subscribe("events", record_class=Item)
        with pytest.raises(RuntimeError, match="not active"):
            await ctx.__anext__()

    @pytest.mark.asyncio
    async def test_deserialization_error_propagates(self, backend, pubsub):
        async with backend.subscribe("events", record_class=Item) as sub:
            pubsub.send("events", "not json")
            with pytest.raises(SerializationError):
                await next_message(sub)

    @pytest.mark.asyncio
    async def test_queue_full_drops_messages(self, backend, pubsub, monkeypatch):
        monkeypatch.setattr(RedisStoreSubscriptionContext, "_QUEUE_MAXSIZE", 1)
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            ctx._enqueue_nowait({"x": 1})
            ctx._enqueue_nowait({"x": 2})
            assert ctx._async_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_queue_full_still_accepts_close_sentinel(
        self, backend, pubsub, monkeypatch
    ):
        monkeypatch.setattr(RedisStoreSubscriptionContext, "_QUEUE_MAXSIZE", 1)
        ctx = backend.subscribe("events", record_class=Item)
        async with ctx:
            pass
        # After exit the sentinel must be present even though the queue is tiny
        with pytest.raises(StopAsyncIteration):
            await next_message(ctx)


class TestPush:
    @pytest.mark.asyncio
    async def test_push_right_by_default(self, backend, cursor):
        cursor.rpush.return_value = 2
        assert await backend.push("q", Item("1"), Item("2")) == 2
        key, *sent = cursor.rpush.call_args.args
        assert key == "q"
        assert [json.loads(s)["id"] for s in sent] == ["1", "2"]
        cursor.lpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_left(self, backend, cursor):
        cursor.lpush.return_value = 1
        assert await backend.push("q", Item("1"), side=QueueSide.LEFT) == 1
        cursor.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_accepts_plain_string_side(self, backend, cursor):
        await backend.push("q", Item("1"), side="left")
        cursor.lpush.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_requires_values(self, backend, cursor):
        with pytest.raises(ValueError, match="at least one value"):
            await backend.push("q")
        cursor.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_wraps_redis_errors(self, backend, cursor):
        cursor.rpush.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="Failed to push to q: down"):
            await backend.push("q", Item("1"))


class TestPop:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "side, command", [(QueueSide.LEFT, "lpop"), (QueueSide.RIGHT, "rpop")]
    )
    async def test_non_blocking(self, backend, cursor, side, command):
        getattr(cursor, command).return_value = payload("1", "a")
        assert await backend.pop("q", Item, side=side) == Item("1", "a")
        getattr(cursor, command).assert_called_once_with("q")

    @pytest.mark.asyncio
    async def test_non_blocking_empty(self, backend, cursor):
        cursor.lpop.return_value = None
        assert await backend.pop("q", Item) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "side, command", [(QueueSide.LEFT, "blpop"), (QueueSide.RIGHT, "brpop")]
    )
    async def test_blocking(self, backend, cursor, side, command):
        getattr(cursor, command).return_value = ("q", payload("1"))
        assert await backend.pop("q", Item, timeout=2, side=side) == Item("1")
        getattr(cursor, command).assert_called_once_with("q", timeout=1.0)  # one slice

    @pytest.mark.asyncio
    async def test_blocking_timeout_expired(self, backend, cursor):
        cursor.blpop.return_value = None
        assert await backend.pop("q", Item, timeout=1) is None

    @pytest.mark.asyncio
    async def test_timeout_zero_blocks_indefinitely(self, backend, cursor):
        cursor.blpop.return_value = ("q", payload("1"))
        await backend.pop("q", Item, timeout=0)
        cursor.blpop.assert_called_once_with("q", timeout=1.0)  # one slice

    @pytest.mark.asyncio
    async def test_negative_timeout(self, backend, cursor):
        with pytest.raises(ValueError, match="timeout"):
            await backend.pop("q", Item, timeout=-1)

    @pytest.mark.asyncio
    async def test_wraps_redis_errors(self, backend, cursor):
        cursor.blpop.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="Failed to pop from q: down"):
            await backend.pop("q", Item, timeout=1)

    @pytest.mark.asyncio
    async def test_dequeue_and_enqueue_helpers(self, backend, cursor):
        cursor.lpop.return_value = payload("1")
        assert await backend.dequeue("q", Item) == Item("1")
        await backend.enqueue("q", Item("2"))
        cursor.rpush.assert_called_once()


class TestPopMany:
    @pytest.mark.asyncio
    async def test_empty_queue(self, backend, cursor):
        cursor.lpop.return_value = None
        assert await backend.pop_many("q", Item) == []
        cursor.llen.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, -1])
    async def test_non_positive_limit(self, backend, cursor, limit):
        assert await backend.pop_many("q", Item, limit=limit) == []
        cursor.lpop.assert_not_called()

    @pytest.mark.asyncio
    async def test_limit_one(self, backend, cursor):
        cursor.lpop.return_value = payload("1")
        assert await backend.pop_many("q", Item, limit=1) == [Item("1")]
        cursor.lpop.assert_called_once_with("q")

    @pytest.mark.asyncio
    async def test_drains_with_count(self, backend, cursor):
        cursor.lpop.side_effect = [payload("1"), [payload("2"), payload("3")]]
        result = await backend.pop_many("q", Item, limit=3)
        assert [i.id for i in result] == ["1", "2", "3"]
        assert cursor.lpop.call_args_list[1].args == ("q", 2)

    @pytest.mark.asyncio
    async def test_drain_returns_fewer_than_limit(self, backend, cursor):
        cursor.lpop.side_effect = [payload("1"), [payload("2")]]
        result = await backend.pop_many("q", Item, limit=10)
        assert [i.id for i in result] == ["1", "2"]

    @pytest.mark.asyncio
    async def test_drain_nothing_left(self, backend, cursor):
        cursor.lpop.side_effect = [payload("1"), None]
        assert await backend.pop_many("q", Item, limit=5) == [Item("1")]

    @pytest.mark.asyncio
    async def test_no_limit_drains_current_length(self, backend, cursor):
        cursor.lpop.side_effect = [payload("1"), [payload("2"), payload("3")]]
        cursor.llen.return_value = 2
        result = await backend.pop_many("q", Item)
        assert len(result) == 3
        assert cursor.lpop.call_args_list[1].args == ("q", 2)

    @pytest.mark.asyncio
    async def test_no_limit_and_queue_now_empty(self, backend, cursor):
        cursor.lpop.return_value = payload("1")
        cursor.llen.return_value = 0
        assert await backend.pop_many("q", Item) == [Item("1")]
        cursor.lpop.assert_called_once()

    @pytest.mark.asyncio
    async def test_falls_back_when_count_unsupported(self, backend, cursor):
        def lpop(key, count=None):
            if count is not None:
                raise ResponseError("ERR wrong number of arguments for 'lpop'")
            return responses.pop(0)

        responses = [payload("1"), payload("2"), None]
        cursor.lpop.side_effect = lpop
        result = await backend.pop_many("q", Item, limit=5)
        assert [i.id for i in result] == ["1", "2"]

    @pytest.mark.asyncio
    async def test_first_pop_blocks_rest_do_not(self, backend, cursor):
        cursor.brpop.return_value = ("q", payload("1"))
        cursor.rpop.return_value = [payload("2")]
        result = await backend.pop_many(
            "q", Item, limit=2, timeout=3, side=QueueSide.RIGHT
        )
        assert [i.id for i in result] == ["1", "2"]
        cursor.brpop.assert_called_once_with("q", timeout=1.0)  # one slice
        cursor.rpop.assert_called_once_with("q", 1)

    @pytest.mark.asyncio
    async def test_drain_wraps_redis_errors(self, backend, cursor):
        cursor.lpop.side_effect = [payload("1"), RedisConnectionError("down")]
        with pytest.raises(ConnectionError, match="Failed to pop from q"):
            await backend.pop_many("q", Item, limit=3)


class TestQueueInspection:
    @pytest.mark.asyncio
    async def test_queue_length(self, backend, cursor):
        cursor.llen.return_value = 4
        assert await backend.queue_length("q") == 4

    @pytest.mark.asyncio
    async def test_queue_length_wraps_errors(self, backend, cursor):
        cursor.llen.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="queue length for q"):
            await backend.queue_length("q")

    @pytest.mark.asyncio
    async def test_queue_range(self, backend, cursor):
        cursor.lrange.return_value = [payload("1"), payload("2")]
        assert await backend.queue_range("q", Item, 0, 1) == [Item("1"), Item("2")]
        cursor.lrange.assert_called_once_with("q", 0, 1)

    @pytest.mark.asyncio
    async def test_queue_range_wraps_errors(self, backend, cursor):
        cursor.lrange.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="queue range for q"):
            await backend.queue_range("q", Item)

    @pytest.mark.asyncio
    async def test_queue_peek(self, backend, cursor):
        cursor.lrange.return_value = [payload("1")]
        assert await backend.queue_peek("q", Item) == Item("1")
        cursor.lrange.assert_called_once_with("q", 0, 0)

    @pytest.mark.asyncio
    async def test_queue_peek_empty(self, backend, cursor):
        cursor.lrange.return_value = []
        assert await backend.queue_peek("q", Item) is None


def entry(entry_id, id, name=""):
    return (entry_id, {"data": payload(id, name)})


class TestStreamAppend:
    @pytest.mark.asyncio
    async def test_append(self, backend, cursor):
        cursor.xadd.return_value = "1-0"
        assert await backend.stream_append("s", Item("1", "a")) == "1-0"
        args, kwargs = cursor.xadd.call_args
        assert args[0] == "s"
        assert json.loads(args[1]["data"]) == {"id": "1", "name": "a"}
        assert kwargs == {"id": "*"}

    @pytest.mark.asyncio
    async def test_append_with_trim(self, backend, cursor):
        await backend.stream_append("s", Item("1"), max_len=10, approximate_trim=False)
        assert cursor.xadd.call_args.kwargs == {
            "id": "*",
            "maxlen": 10,
            "approximate": False,
        }

    @pytest.mark.asyncio
    async def test_append_decodes_bytes_id(self, backend, cursor):
        cursor.xadd.return_value = b"1-0"
        assert await backend.stream_append("s", Item("1")) == "1-0"

    @pytest.mark.asyncio
    async def test_append_wraps_errors(self, backend, cursor):
        cursor.xadd.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="append to stream 's'"):
            await backend.stream_append("s", Item("1"))


class TestStreamConsumerGroup:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "offset, redis_id",
        [
            (StreamOffset.BEGINNING, "0"),
            (StreamOffset.LATEST, "$"),
            ("1700000000000-0", "1700000000000-0"),
        ],
    )
    async def test_create(self, backend, cursor, offset, redis_id):
        await backend.stream_ensure_consumer_group("s", "g", start_from=offset)
        cursor.xgroup_create.assert_called_once_with(
            "s", "g", id=redis_id, mkstream=True
        )

    @pytest.mark.asyncio
    async def test_existing_group_is_ignored(self, backend, cursor):
        cursor.xgroup_create.side_effect = ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )
        await backend.stream_ensure_consumer_group("s", "g")

    @pytest.mark.asyncio
    async def test_other_response_errors_raise(self, backend, cursor):
        cursor.xgroup_create.side_effect = ResponseError("WRONGTYPE not a stream")
        with pytest.raises(ConnectionError, match="consumer group 'g' on 's'"):
            await backend.stream_ensure_consumer_group("s", "g")

    @pytest.mark.asyncio
    async def test_connection_errors_raise(self, backend, cursor):
        cursor.xgroup_create.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="down"):
            await backend.stream_ensure_consumer_group("s", "g")


class TestStreamRead:
    @pytest.mark.asyncio
    async def test_non_blocking(self, backend, cursor):
        cursor.xreadgroup.return_value = [["s", [entry("1-0", "1", "a")]]]
        result = await backend.stream_read("s", "g", Item, "c1", count=5, block_ms=None)
        assert result == [
            StreamEntry(
                entry_id="1-0", record=Item("1", "a"), group_name="g", consumer_id="c1"
            )
        ]
        cursor.xreadgroup.assert_called_once_with("g", "c1", {"s": ">"}, count=5)

    @pytest.mark.asyncio
    async def test_blocking_uses_dedicated_executor(self, backend, cursor):
        names, cursor.xreadgroup.side_effect = thread_recorder([])
        assert await backend.stream_read("s", "g", Item, "c1", block_ms=100) == []
        assert names[0].startswith("volnux-messaging-block")
        assert cursor.xreadgroup.call_args.kwargs == {"count": 1, "block": 100}

    @pytest.mark.asyncio
    async def test_default_consumer(self, backend, cursor):
        cursor.xreadgroup.return_value = [["s", [entry("1-0", "1")]]]
        result = await backend.stream_read("s", "g", Item, block_ms=None)
        assert cursor.xreadgroup.call_args.args[1] == "default"
        assert result[0].consumer_id == "default"

    @pytest.mark.asyncio
    async def test_empty_response(self, backend, cursor):
        cursor.xreadgroup.return_value = None
        assert await backend.stream_read("s", "g", Item, block_ms=None) == []

    @pytest.mark.asyncio
    async def test_bytes_response(self, backend, cursor):
        cursor.xreadgroup.return_value = [
            [b"s", [(b"1-0", {b"data": payload("1").encode()})]]
        ]
        result = await backend.stream_read("s", "g", Item, block_ms=None)
        assert result[0].entry_id == "1-0"
        assert result[0].record == Item("1")

    @pytest.mark.asyncio
    async def test_entries_without_data_skipped(self, backend, cursor):
        cursor.xreadgroup.return_value = [
            ["s", [("1-0", {"other": "x"}), ("2-0", None), entry("3-0", "3")]]
        ]
        result = await backend.stream_read("s", "g", Item, block_ms=None)
        assert [e.entry_id for e in result] == ["3-0"]

    @pytest.mark.asyncio
    async def test_multiple_streams_flattened(self, backend, cursor):
        cursor.xreadgroup.return_value = [
            ["s", [entry("1-0", "1")]],
            ["s", [entry("2-0", "2")]],
            ["s"],
        ]
        result = await backend.stream_read("s", "g", Item, block_ms=None)
        assert [e.entry_id for e in result] == ["1-0", "2-0"]

    @pytest.mark.asyncio
    async def test_wraps_errors(self, backend, cursor):
        cursor.xreadgroup.side_effect = ResponseError("NOGROUP")
        with pytest.raises(ConnectionError, match="read from stream 's'"):
            await backend.stream_read("s", "g", Item, block_ms=None)

    @pytest.mark.asyncio
    async def test_read_and_ack(self, backend, cursor):
        cursor.xreadgroup.return_value = [["s", [entry("1-0", "1"), entry("2-0", "2")]]]
        cursor.xack.return_value = 2
        records = await backend.stream_read_and_ack("s", "g", Item, block_ms=None)
        assert records == [Item("1"), Item("2")]
        cursor.xack.assert_called_once_with("s", "g", "1-0", "2-0")


class TestStreamClaimPending:
    @pytest.mark.asyncio
    async def test_xautoclaim(self, backend, cursor):
        cursor.xautoclaim.return_value = ["0-0", [entry("1-0", "1")], []]
        result = await backend.stream_claim_pending(
            "s", "g", "c2", Item, min_idle_ms=1000, count=10
        )
        assert result == [
            StreamEntry(
                entry_id="1-0", record=Item("1"), group_name="g", consumer_id="c2"
            )
        ]
        cursor.xautoclaim.assert_called_once_with(
            "s", "g", "c2", min_idle_time=1000, start_id="0-0", count=10
        )
        cursor.xpending_range.assert_not_called()

    @pytest.mark.asyncio
    async def test_xautoclaim_unexpected_shape(self, backend, cursor):
        cursor.xautoclaim.return_value = None
        assert await backend.stream_claim_pending("s", "g", "c2", Item) == []

    @pytest.mark.asyncio
    async def test_skips_deleted_entries(self, backend, cursor):
        cursor.xautoclaim.return_value = ["0-0", [("1-0", None), entry("2-0", "2")]]
        result = await backend.stream_claim_pending("s", "g", "c2", Item)
        assert [e.entry_id for e in result] == ["2-0"]

    @pytest.mark.asyncio
    async def test_fallback_for_old_redis(self, backend, cursor):
        cursor.xautoclaim.side_effect = ResponseError(
            "ERR unknown command 'XAUTOCLAIM'"
        )
        cursor.xpending_range.return_value = [
            {"message_id": "1-0", "time_since_delivered": 5000},
            {"message_id": "2-0", "time_since_delivered": 10},
        ]
        cursor.xclaim.return_value = [entry("1-0", "1")]
        result = await backend.stream_claim_pending(
            "s", "g", "c2", Item, min_idle_ms=1000, count=10
        )
        assert [e.entry_id for e in result] == ["1-0"]
        cursor.xpending_range.assert_called_once_with(
            "s", "g", min="-", max="+", count=10
        )
        cursor.xclaim.assert_called_once_with(
            "s", "g", "c2", min_idle_time=1000, message_ids=["1-0"]
        )

    @pytest.mark.asyncio
    async def test_fallback_nothing_eligible(self, backend, cursor):
        cursor.xautoclaim.side_effect = ResponseError("ERR unknown command")
        cursor.xpending_range.return_value = [
            {"message_id": "1-0", "time_since_delivered": 10}
        ]
        assert await backend.stream_claim_pending("s", "g", "c2", Item) == []
        cursor.xclaim.assert_not_called()

    @pytest.mark.asyncio
    async def test_other_errors_do_not_fall_back(self, backend, cursor):
        cursor.xautoclaim.side_effect = ResponseError("NOGROUP No such key")
        with pytest.raises(ConnectionError, match="claim pending entries on 's'"):
            await backend.stream_claim_pending("s", "g", "c2", Item)
        cursor.xpending_range.assert_not_called()

    @pytest.mark.asyncio
    async def test_connection_errors_do_not_fall_back(self, backend, cursor):
        cursor.xautoclaim.side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match="down"):
            await backend.stream_claim_pending("s", "g", "c2", Item)
        cursor.xpending_range.assert_not_called()


class TestStreamMaintenance:
    @pytest.mark.asyncio
    async def test_ack(self, backend, cursor):
        cursor.xack.return_value = 2
        assert await backend.stream_ack("s", "g", "1-0", "2-0") == 2
        cursor.xack.assert_called_once_with("s", "g", "1-0", "2-0")

    @pytest.mark.asyncio
    async def test_ack_nothing(self, backend, cursor):
        assert await backend.stream_ack("s", "g") == 0
        cursor.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_length(self, backend, cursor):
        cursor.xlen.return_value = 7
        assert await backend.stream_length("s") == 7

    @pytest.mark.asyncio
    async def test_trim(self, backend, cursor):
        cursor.xtrim.return_value = 3
        assert await backend.stream_trim("s", 10, approximate=False) == 3
        cursor.xtrim.assert_called_once_with("s", maxlen=10, approximate=False)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("deleted, expected", [(1, True), (0, False)])
    async def test_delete(self, backend, cursor, deleted, expected):
        cursor.delete.return_value = deleted
        assert await backend.stream_delete("s") is expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method, args, command, message",
        [
            ("stream_ack", ("s", "g", "1-0"), "xack", "ACK entries on 's'"),
            ("stream_length", ("s",), "xlen", "length of 's'"),
            ("stream_trim", ("s", 1), "xtrim", "trim 's'"),
            ("stream_delete", ("s",), "delete", "delete stream 's'"),
        ],
    )
    async def test_wraps_errors(self, backend, cursor, method, args, command, message):
        getattr(cursor, command).side_effect = RedisConnectionError("down")
        with pytest.raises(ConnectionError, match=message):
            await getattr(backend, method)(*args)


class TestTranslateOffset:
    def test_offsets(self):
        translate = RedisStreamCapabilityMixin._translate_offset
        assert translate(StreamOffset.BEGINNING) == "0"
        assert translate(StreamOffset.LATEST) == "$"
        assert translate("5-0") == "5-0"


class TestBlockingPopCancellation:
    """A caller that leaves mid-BLPOP must not cause an item to be dropped."""

    @staticmethod
    def blocking_cursor(cursor, deliver_after):
        """blpop that returns an item once `deliver_after` seconds have passed,
        honouring the slice timeout it is given (like a real server)."""
        start = time.monotonic()
        delivered = {"done": False}

        def blpop(key, timeout):
            while time.monotonic() - start < deliver_after:
                if time.monotonic() - start + 0.01 > deliver_after:
                    break
                time.sleep(0.01)
                if time.monotonic() - start >= timeout:
                    return None
            if delivered["done"]:
                return None
            delivered["done"] = True
            return (key, payload("late"))

        cursor.blpop.side_effect = blpop
        return delivered

    @pytest.mark.asyncio
    async def test_item_popped_after_caller_left_is_pushed_back(self, backend, cursor):
        self.blocking_cursor(cursor, deliver_after=0.3)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(backend.pop("q", Item, timeout=5), 0.1)

        await asyncio.sleep(1.5)  # worker finishes its slice, restore task runs
        cursor.lpush.assert_called_once_with("q", payload("late"))

    @pytest.mark.asyncio
    async def test_brpop_item_is_restored_to_the_tail(self, backend, cursor):
        self.blocking_cursor(cursor, deliver_after=0.3)
        cursor.brpop.side_effect = cursor.blpop.side_effect

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                backend.pop("q", Item, timeout=5, side=QueueSide.RIGHT), 0.1
            )

        await asyncio.sleep(1.5)
        cursor.rpush.assert_called_once_with("q", payload("late"))

    @pytest.mark.asyncio
    async def test_worker_stops_within_one_slice_after_cancellation(
        self, backend, cursor
    ):
        calls = []

        def blpop(key, timeout):
            calls.append(timeout)
            time.sleep(min(timeout, 0.2))
            return None

        cursor.blpop.side_effect = blpop
        backend._BLOCKING_SLICE = 0.2
        task = asyncio.ensure_future(backend.pop("q", Item, timeout=0))  # forever
        await asyncio.sleep(0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        settled = len(calls)
        await asyncio.sleep(0.6)

        assert len(calls) <= settled + 1  # at most the in-flight slice finishes
        cursor.lpush.assert_not_called()  # nothing popped, nothing to restore

    @pytest.mark.asyncio
    async def test_semaphore_slot_held_until_worker_really_finishes(self, cursor):
        backend = FakeBackend(cursor, pool_size=3)  # limit = 1
        backend._BLOCKING_SLICE = 0.3
        cursor.blpop.side_effect = lambda key, timeout: time.sleep(timeout)

        task = asyncio.ensure_future(backend.pop("q", Item, timeout=0))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert backend._get_blocking_semaphore().locked()  # worker still in its slice
        await asyncio.sleep(0.6)
        assert not backend._get_blocking_semaphore().locked()

    @pytest.mark.asyncio
    async def test_normal_pop_is_unaffected(self, backend, cursor):
        cursor.blpop.return_value = ("q", payload("1"))
        assert (await backend.pop("q", Item, timeout=3)) == Item("1")
        cursor.lpush.assert_not_called()
