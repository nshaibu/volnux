import asyncio
import contextlib
import functools
import logging
import threading
import time
from typing import (
    Any,
    Iterable,
    Iterator,
    List,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Type,
    Union,
)

from redis.exceptions import RedisError, ResponseError

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    AsyncSubscriptionContext,
    QueueSide,
    Record,
    Message,
    StreamCapabilityMixin,
    StreamEntry,
    StreamOffset,
)
from volnux.backends.messaging.util import _BLOCKING_EXECUTOR

if TYPE_CHECKING:
    from volnux.backends.connectors.redis import RedisConnector

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _redis_errors(message: str) -> Iterator[None]:
    """Re-raise RedisError as ConnectionError prefixed with ``message``."""
    try:
        yield
    except RedisError as exc:
        raise ConnectionError(f"{message}: {exc}") from exc


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


# Strong references to in-flight restore tasks (bare tasks can be garbage collected).
_PENDING_RESTORES: "set[asyncio.Future[Any]]" = set()


class RedisMessagingMixin:
    """Shared command execution for the Redis messaging capability mixins.

    Expects the host backend to provide ``connector``, ``_ensure_connected()``,
    ``_serialize_record()`` and ``_deserialize_record()``
    (see ``KeyValueStoreBackendBase``).
    """

    connector: "RedisConnector"

    # Connections left free for non-blocking commands (push/publish/etc.)
    # so that blocking callers (BLPOP/BRPOP/XREADGROUP BLOCK) can never
    # claim the whole pool.
    _BLOCKING_POOL_RESERVE = 2

    def _execute_sync(self, command: str, *args: Any, **kwargs: Any) -> Any:
        # Runs in a worker thread: reconnecting is blocking network I/O and
        # must not happen on the event loop.
        self._ensure_connected()  # type: ignore[attr-defined]
        return getattr(self.connector.get_cursor(), command)(*args, **kwargs)

    async def _execute(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Run a non-blocking Redis command in the default thread pool."""
        return await asyncio.to_thread(self._execute_sync, command, *args, **kwargs)

    async def _execute_blocking(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking Redis command in the dedicated blocking executor.

        Blocking commands hold a pooled connection for their full duration,
        so concurrency is capped below the pool size.
        """
        loop = asyncio.get_running_loop()
        async with self._get_blocking_semaphore():
            return await loop.run_in_executor(
                _BLOCKING_EXECUTOR,
                functools.partial(self._execute_sync, command, *args, **kwargs),
            )

    # A blocking pop is issued as a series of BLPOP/BRPOP calls of at most this
    # many seconds. A thread blocked inside one call cannot be interrupted, so
    # slicing bounds how long a worker outlives a cancelled caller.
    _BLOCKING_SLICE = 1.0

    def _blocking_pop_sync(
        self,
        command: str,
        key: str,
        timeout: float,
        cancelled: threading.Event,
    ) -> Any:
        """Worker-thread loop behind a blocking pop (``timeout=0`` = forever).

        Stops between slices once ``cancelled`` is set.
        """
        deadline = None if timeout == 0 else time.monotonic() + timeout
        while not cancelled.is_set():
            slice_timeout = self._BLOCKING_SLICE
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                slice_timeout = min(slice_timeout, remaining)
            result = self._execute_sync(command, key, timeout=slice_timeout)
            if result is not None:
                return result
        return None

    async def _blocking_pop(self, command: str, key: str, timeout: float) -> Any:
        """Cancellation-safe blocking pop.

        If the caller is cancelled (or times out via ``wait_for``) while the
        worker thread is inside BLPOP/BRPOP, the thread may still receive an
        item afterwards. That item has no consumer any more, so it is pushed
        back to the end it was taken from instead of being dropped.
        """
        loop = asyncio.get_running_loop()
        semaphore = self._get_blocking_semaphore()
        cancelled = threading.Event()

        await semaphore.acquire()
        try:
            future = loop.run_in_executor(
                _BLOCKING_EXECUTOR,
                functools.partial(
                    self._blocking_pop_sync, command, key, timeout, cancelled
                ),
            )
        except BaseException:
            semaphore.release()
            raise
        # The slot is held until the worker really finishes, not merely until
        # the caller stops waiting, so the pool-size cap stays accurate.
        future.add_done_callback(lambda _f: semaphore.release())

        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled.set()
            future.add_done_callback(
                functools.partial(self._restore_orphaned_pop, command, key)
            )
            raise

    def _restore_orphaned_pop(
        self, command: str, key: str, future: "asyncio.Future[Any]"
    ) -> None:
        """Done-callback: push back an item popped for a caller that left."""
        if future.cancelled() or future.exception() is not None:
            return  # nothing was popped (errors are the dead caller's to lose)
        result = future.result()
        if result is None:
            return
        _, raw = result
        restore = "lpush" if command == "blpop" else "rpush"
        task = asyncio.ensure_future(self._restore_item(restore, key, raw))
        _PENDING_RESTORES.add(task)
        task.add_done_callback(_PENDING_RESTORES.discard)

    async def _restore_item(self, command: str, key: str, raw: Any) -> None:
        try:
            await self._execute(command, key, raw)
            logger.info("Restored item popped for a cancelled caller on '%s'", key)
        except Exception:
            logger.critical(
                "Could not restore an item popped for a cancelled caller on '%s'; "
                "the message is lost: %r",
                key,
                raw,
                exc_info=True,
            )

    def _get_blocking_semaphore(self) -> asyncio.Semaphore:
        # asyncio primitives bind to the loop they are first used on, so keep
        # one semaphore per running loop.
        loop = asyncio.get_running_loop()
        cached: Optional[Tuple[asyncio.AbstractEventLoop, asyncio.Semaphore]] = getattr(
            self, "_blocking_semaphore", None
        )
        if cached is None or cached[0] is not loop:
            pool_size = getattr(self.connector.config, "pool_size", 10)
            limit = max(1, pool_size - self._BLOCKING_POOL_RESERVE)
            cached = (loop, asyncio.Semaphore(limit))
            self._blocking_semaphore = cached
        return cached[1]

    def _deserialize(self, raw: Any, record_class: Type[Record]) -> Record:
        return self._deserialize_record(raw, record_class)  # type: ignore[attr-defined]


class RedisStorePubSubMixin(RedisMessagingMixin, PubSubCapabilityMixin):

    async def publish(self, channel: str, record: Record) -> int:
        payload = self._serialize_record(record)  # type: ignore[attr-defined]
        with _redis_errors(f"Failed to publish to {channel}"):
            return await self._execute("publish", channel, payload)

    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        if not channels:
            raise ValueError("subscribe() requires at least one channel")
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
            record_class=record_class,
        )

    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        if not patterns:
            raise ValueError("psubscribe() requires at least one pattern")
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
            record_class=record_class,
        )


class RedisStoreSubscriptionContext(AsyncSubscriptionContext):
    """Bridges a blocking redis-py PubSub connection to an async iterator.

    A worker thread polls the PubSub connection and hands messages to the
    event loop through an ``asyncio.Queue``. The thread polls with a short
    timeout and checks ``_closing`` between polls, so shutdown never has to
    interrupt a blocked read from another thread.
    """

    _CLOSE_SENTINEL = object()
    _POLL_INTERVAL = 0.5
    _QUEUE_MAXSIZE = 10000

    def __init__(
        self,
        backend: "RedisStorePubSubMixin",
        channels: List[str],
        patterns: List[str],
        record_class: Type[Record],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self.record_class = record_class
        self._pubsub: Any = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_queue: Optional[asyncio.Queue] = None
        self._listener: Optional[asyncio.Future] = None
        self._closing = False
        self._listen_exception: Optional[BaseException] = None

    async def __aenter__(self) -> "RedisStoreSubscriptionContext":
        if self._loop is not None:
            raise RuntimeError("Subscription context cannot be entered twice")

        self._loop = asyncio.get_running_loop()
        self._async_queue = asyncio.Queue(maxsize=self._QUEUE_MAXSIZE)

        try:
            self._pubsub = await asyncio.to_thread(self._open_pubsub)
        except Exception as exc:
            await self._cleanup()
            raise ConnectionError(f"Failed to subscribe: {exc}") from exc

        self._listener = self._loop.run_in_executor(_BLOCKING_EXECUTOR, self._listen)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._cleanup()

    def _open_pubsub(self) -> Any:
        self.backend._ensure_connected()  # type: ignore[attr-defined]
        pubsub = self.backend.connector.get_cursor().pubsub(
            ignore_subscribe_messages=True
        )
        try:
            if self.channels:
                pubsub.subscribe(*self.channels)
            if self.patterns:
                pubsub.psubscribe(*self.patterns)
        except Exception:
            pubsub.close()
            raise
        return pubsub

    def _listen(self) -> None:
        """Worker-thread loop that forwards messages to the event loop."""
        try:
            while not self._closing:
                msg = self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=self._POLL_INTERVAL
                )
                if msg is not None and msg.get("type") in ("message", "pmessage"):
                    self._loop.call_soon_threadsafe(self._enqueue_nowait, msg)
        except Exception as exc:
            if self._closing:
                logger.debug("Redis listen loop terminated: %s", exc)
            else:
                logger.warning(
                    "Redis subscription listen loop terminated unexpectedly: %s", exc
                )
                self._listen_exception = exc
        finally:
            # Always unblock __anext__, or `async for` consumers would wait forever.
            self._loop.call_soon_threadsafe(self._enqueue_nowait, self._CLOSE_SENTINEL)

    def _enqueue_nowait(self, item: Any) -> None:
        try:
            self._async_queue.put_nowait(item)
        except asyncio.QueueFull:
            if item is self._CLOSE_SENTINEL:
                # The termination signal must not be dropped, or __anext__
                # would hang forever. Evict the oldest message to make room.
                try:
                    self._async_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._async_queue.put_nowait(item)
            else:
                logger.warning("Redis subscription queue full; dropping message")

    async def _cleanup(self) -> None:
        self._closing = True

        if self._listener is not None:
            # The worker notices _closing within one poll interval.
            try:
                await self._listener
            except Exception as exc:
                logger.debug("Error while awaiting Redis listener: %s", exc)
            self._listener = None

        if self._pubsub is not None:
            # Closing drops the subscriptions server-side; no UNSUBSCRIBE needed.
            pubsub, self._pubsub = self._pubsub, None
            try:
                await asyncio.to_thread(pubsub.close)
            except Exception as exc:
                logger.debug("Error while closing Redis pubsub: %s", exc)

    def __aiter__(self) -> "RedisStoreSubscriptionContext":
        return self

    async def __anext__(self) -> Message:
        if self._async_queue is None:
            raise RuntimeError(
                "Subscription is not active; use 'async with' before iterating"
            )

        msg = await self._async_queue.get()

        if msg is self._CLOSE_SENTINEL:
            # Re-queue so further iteration also stops instead of hanging.
            self._enqueue_nowait(msg)
            if self._listen_exception is not None:
                raise ConnectionError(
                    f"Redis subscription terminated unexpectedly: {self._listen_exception}"
                ) from self._listen_exception
            raise StopAsyncIteration

        return Message(
            channel=_decode(msg["channel"]),
            pattern=_decode(msg.get("pattern")),
            data=self.backend._deserialize(msg["data"], self.record_class),
            raw=msg["data"],
        )


class RedisStorePushPopMixin(RedisMessagingMixin, PushPopCapabilityMixin):

    async def push(
        self, key: str, *values: Record, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        if not values:
            raise ValueError("push() requires at least one value")
        payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-defined]
        command = "lpush" if side == QueueSide.LEFT else "rpush"
        with _redis_errors(f"Failed to push to {key}"):
            return await self._execute(command, key, *payloads)

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[Union[float, int]] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Record]:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be None or >= 0")

        left = side == QueueSide.LEFT
        with _redis_errors(f"Failed to pop from {key}"):
            if timeout is None:
                raw = await self._execute("lpop" if left else "rpop", key)
            else:
                result = await self._blocking_pop(
                    "blpop" if left else "brpop", key, float(timeout)
                )
                if result is None:
                    return None
                _, raw = result

        return None if raw is None else self._deserialize(raw, record_class)

    async def pop_many(
        self,
        key: str,
        record_class: Type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Record]:
        """
        Block until the queue has data, then drain a batch of items up to `limit`.
        """
        if limit is not None and limit <= 0:
            return []

        # Only the first item waits up to `timeout`; the rest is drained
        # from whatever is already available.
        first_item = await self.pop(key, record_class, timeout=timeout, side=side)
        if first_item is None:
            return []

        remaining = None if limit is None else limit - 1
        if remaining == 0:
            return [first_item]
        return [first_item] + await self._drain(key, record_class, remaining, side)

    async def _drain(
        self,
        key: str,
        record_class: Type[Record],
        count: Optional[int],
        side: QueueSide,
    ) -> List[Record]:
        """Non-blocking pop of up to ``count`` items (all available if None)."""
        command = "lpop" if side == QueueSide.LEFT else "rpop"
        with _redis_errors(f"Failed to pop from {key}"):
            if count is None:
                count = await self._execute("llen", key)
            if not count:
                return []
            try:
                # LPOP/RPOP with COUNT (Redis 6.2+) drains atomically in one call
                raws = await self._execute(command, key, count) or []
            except ResponseError:
                # Older servers reject the COUNT argument; pop one at a time
                raws = []
                for _ in range(count):
                    raw = await self._execute(command, key)
                    if raw is None:
                        break
                    raws.append(raw)
        return [self._deserialize(raw, record_class) for raw in raws]

    async def queue_delete(self, key: str) -> bool:
        with _redis_errors(f"Failed to delete queue {key}"):
            return bool(await self._execute("delete", key))

    async def queue_expire(self, key: str, ttl: float) -> bool:
        if ttl <= 0:
            raise ValueError(f"ttl must be > 0, got {ttl!r}")
        with _redis_errors(f"Failed to set the expiry of queue {key}"):
            return bool(await self._execute("pexpire", key, max(1, int(ttl * 1000))))

    async def queue_length(self, key: str) -> int:
        with _redis_errors(f"Failed to get queue length for {key}"):
            return await self._execute("llen", key)

    async def queue_range(
        self, key: str, record_class: Type[Record], start: int = 0, stop: int = -1
    ) -> List[Record]:
        with _redis_errors(f"Failed to get queue range for {key}"):
            raws = await self._execute("lrange", key, start, stop)
        return [self._deserialize(raw, record_class) for raw in raws]


class RedisStreamCapabilityMixin(RedisMessagingMixin, StreamCapabilityMixin):
    """Redis Streams implementation of the append-only durable event log."""

    _DEFAULT_CONSUMER = "default"

    async def stream_append(
        self,
        stream_key: str,
        record: Any,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        payload = self._serialize_record(record)  # type: ignore[attr-defined]

        kwargs: dict[str, Any] = {"id": "*"}
        if max_len is not None:
            kwargs["maxlen"] = max_len
            kwargs["approximate"] = approximate_trim

        with _redis_errors(f"Failed to append to stream '{stream_key}'"):
            entry_id = await self._execute(
                "xadd", stream_key, {"data": payload}, **kwargs
            )
        return _decode(entry_id)

    async def stream_ensure_consumer_group(
        self,
        stream_key: str,
        group_name: str,
        start_from: str = StreamOffset.BEGINNING,
    ) -> None:
        with _redis_errors(
            f"Failed to ensure consumer group '{group_name}' on '{stream_key}'"
        ):
            try:
                # MKSTREAM creates the stream if needed; BUSYGROUP means the
                # group already exists, which is fine (idempotent create).
                await self._execute(
                    "xgroup_create",
                    stream_key,
                    group_name,
                    id=self._translate_offset(start_from),
                    mkstream=True,
                )
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc).upper():
                    raise

    async def stream_read(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[Record]]:
        consumer = consumer_id or self._DEFAULT_CONSUMER
        args = (group_name, consumer, {stream_key: ">"})

        with _redis_errors(f"Failed to read from stream '{stream_key}'"):
            if block_ms is None:
                raw = await self._execute("xreadgroup", *args, count=count)
            else:
                raw = await self._execute_blocking(
                    "xreadgroup", *args, count=count, block=block_ms
                )

        return self._parse_xreadgroup_response(
            raw, stream_key, group_name, consumer, record_class
        )

    async def stream_claim_pending(
        self,
        stream_key: str,
        group_name: str,
        consumer_id: str,
        record_class: Type[Record],
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> List[StreamEntry]:
        """Claims orphaned pending entries from crashed workers.

        Uses XAUTOCLAIM (Redis 6.2+) for atomic claim-and-return.
        Falls back to XPENDING + XCLAIM for older Redis versions.
        """
        with _redis_errors(f"Failed to claim pending entries on '{stream_key}'"):
            try:
                raw = await self._execute(
                    "xautoclaim",
                    stream_key,
                    group_name,
                    consumer_id,
                    min_idle_time=min_idle_ms,
                    start_id="0-0",
                    count=count,
                )
                # xautoclaim returns (next_start_id, [(id, fields), ...], [deleted_ids])
                claimed = (
                    raw[1] if isinstance(raw, (list, tuple)) and len(raw) > 1 else []
                )
            except ResponseError as exc:
                if "unknown command" not in str(exc).lower():
                    raise
                claimed = await self._claim_with_xpending(
                    stream_key, group_name, consumer_id, min_idle_ms, count
                )

        return self._build_entries(claimed, group_name, consumer_id, record_class)

    async def _claim_with_xpending(
        self,
        stream_key: str,
        group_name: str,
        consumer_id: str,
        min_idle_ms: int,
        count: int,
    ) -> List[Any]:
        """XPENDING + XCLAIM fallback for Redis < 6.2 (no XAUTOCLAIM)."""
        pending = await self._execute(
            "xpending_range",
            stream_key,
            group_name,
            min="-",
            max="+",
            count=count,
        )
        eligible_ids = [
            p["message_id"]
            for p in pending
            if p.get("time_since_delivered", 0) >= min_idle_ms
        ]
        if not eligible_ids:
            return []
        return await self._execute(
            "xclaim",
            stream_key,
            group_name,
            consumer_id,
            min_idle_time=min_idle_ms,
            message_ids=eligible_ids,
        )

    async def stream_ack(
        self,
        stream_key: str,
        group_name: str,
        *entry_ids: str,
    ) -> int:
        if not entry_ids:
            return 0
        with _redis_errors(f"Failed to ACK entries on '{stream_key}'"):
            return await self._execute("xack", stream_key, group_name, *entry_ids)

    async def stream_length(self, stream_key: str) -> int:
        with _redis_errors(f"Failed to get length of '{stream_key}'"):
            return await self._execute("xlen", stream_key)

    async def stream_trim(
        self,
        stream_key: str,
        max_len: int,
        approximate: bool = True,
    ) -> int:
        with _redis_errors(f"Failed to trim '{stream_key}'"):
            return await self._execute(
                "xtrim", stream_key, maxlen=max_len, approximate=approximate
            )

    async def stream_delete(self, stream_key: str) -> bool:
        with _redis_errors(f"Failed to delete stream '{stream_key}'"):
            return bool(await self._execute("delete", stream_key))

    @staticmethod
    def _translate_offset(offset: str) -> str:
        """Translates Volnux canonical offsets to Redis XGROUP CREATE IDs."""
        if offset == StreamOffset.BEGINNING:
            return "0"
        if offset == StreamOffset.LATEST:
            return "$"
        # Assume already a valid Redis stream ID (e.g., "1700000000000-0")
        return offset

    def _parse_xreadgroup_response(
        self,
        raw: Any,
        stream_key: str,
        group_name: str,
        consumer_id: Optional[str],
        record_class: Type[Any],
    ) -> List[StreamEntry]:
        """Parses Redis XREADGROUP response into typed StreamEntry list."""
        if not raw:
            return []
        # XREADGROUP returns [[stream_key, [(id, fields), ...]], ...]
        entries = [
            entry
            for stream_result in raw
            if len(stream_result) > 1
            for entry in stream_result[1]
        ]
        return self._build_entries(entries, group_name, consumer_id, record_class)

    def _build_entries(
        self,
        entries: Iterable[Any],
        group_name: str,
        consumer_id: Optional[str],
        record_class: Type[Any],
    ) -> List[StreamEntry]:
        """Convert ``(entry_id, fields)`` pairs into StreamEntry objects.

        Entries whose fields are missing (deleted while pending) or that have
        no ``data`` field are skipped.
        """
        results: List[StreamEntry] = []
        for entry_id, fields in entries:
            eid = _decode(entry_id)
            data = None
            if fields:
                data = fields.get(b"data")
                if data is None:
                    data = fields.get("data")
            if data is None:
                logger.warning("Stream entry %s missing 'data' field, skipping", eid)
                continue
            results.append(
                StreamEntry(
                    entry_id=eid,
                    record=self._deserialize(_decode(data), record_class),
                    group_name=group_name,
                    consumer_id=consumer_id,
                )
            )
        return results
