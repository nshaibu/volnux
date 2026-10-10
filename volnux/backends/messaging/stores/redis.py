import asyncio
import logging
from typing import Any, List, Optional, TYPE_CHECKING, Type, Callable, Union

from redis.exceptions import RedisError

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

_CLOSE_SENTINEL = object()


class RedisStorePubSubMixin(PubSubCapabilityMixin):

    connector: "RedisConnector"

    async def publish(self, channel: str, record: Record) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            payload = self._serialize_record(record)  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            receivers = await asyncio.to_thread(cursor.publish, channel, payload)
            return receivers
        except RedisError as exc:
            raise ConnectionError(f"Failed to publish to {channel}: {exc}") from exc

    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
            record_class=record_class,
        )

    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
            record_class=record_class,
        )


class RedisStoreSubscriptionContext(AsyncSubscriptionContext):

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
        self._pubsub = None
        self._async_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._listener_task: Optional[asyncio.Task] = None
        self._loop = asyncio.get_running_loop()
        self._closing = False
        self._listen_exception: Optional[BaseException] = None

    async def __aenter__(self) -> "RedisStoreSubscriptionContext":
        try:
            self.backend._ensure_connected()  # type: ignore

            cursor = self.backend.connector.get_cursor()

            # Get pubsub object from the existing pooled client
            self._pubsub = await asyncio.to_thread(cursor.pubsub)

            if self.channels:
                await asyncio.to_thread(self._pubsub.subscribe, *self.channels)
            if self.patterns:
                await asyncio.to_thread(self._pubsub.psubscribe, *self.patterns)

            # Start the background bridge task
            self._listener_task = asyncio.create_task(self._bridge_listen())
            return self
        except Exception as exc:
            await self._cleanup()
            raise ConnectionError(f"Failed to subscribe: {exc}") from exc

    async def _bridge_listen(self):
        """Runs the blocking listen() in a dedicated thread pool and bridges to async."""

        def sync_listen():
            try:
                # pubsub.listen() is a blocking generator that yields messages
                for msg in self._pubsub.listen():
                    if msg["type"] in ("message", "pmessage"):
                        # Thread-safe bridge to the async event loop
                        self._loop.call_soon_threadsafe(self._enqueue_nowait, msg)
            except Exception as e:
                if self._closing:
                    logger.debug("Redis listen loop terminated: %s", e)
                else:
                    logger.warning(
                        "Redis subscription listen loop terminated unexpectedly: %s", e
                    )
                    self._listen_exception = e
            finally:
                # Always unblock __anext__, even on a clean/unexpected exit,
                # or consumers of `async for` would await the queue forever.
                self._loop.call_soon_threadsafe(
                    self._enqueue_nowait, self._CLOSE_SENTINEL
                )

        # Run in the dedicated blocking executor, NOT the default to_thread pool
        await self._loop.run_in_executor(_BLOCKING_EXECUTOR, sync_listen)

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

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cleanup()

    async def _cleanup(self):
        self._closing = True

        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()

        if self._pubsub:
            try:
                if self.channels:
                    await asyncio.to_thread(self._pubsub.unsubscribe, *self.channels)
                if self.patterns:
                    await asyncio.to_thread(self._pubsub.punsubscribe, *self.patterns)
                # Closing the connection also unblocks the listener thread,
                # which is otherwise parked in a blocking listen() read.
                await asyncio.to_thread(self._pubsub.close)
            except Exception as exc:
                logger.debug("Error while closing Redis pubsub: %s", exc)

        if self._listener_task:
            # Wait for the bridge thread to actually exit before returning,
            # since cancel() alone cannot interrupt a thread already blocked
            # in listen() — closing the connection above is what does that.
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug("Error while awaiting Redis listener task: %s", exc)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Message:
        # Pure async wait. Wakes up instantly when the bridge thread pushes a
        # message, or when the listener stops (see _CLOSE_SENTINEL below).
        msg = await self._async_queue.get()

        if msg is self._CLOSE_SENTINEL:
            if self._listen_exception is not None:
                raise ConnectionError(
                    f"Redis subscription terminated unexpectedly: {self._listen_exception}"
                ) from self._listen_exception
            raise StopAsyncIteration

        channel = (
            msg["channel"].decode()
            if isinstance(msg["channel"], bytes)
            else msg["channel"]
        )

        pattern = msg.get("pattern")
        if isinstance(pattern, bytes):
            pattern = pattern.decode()

        return Message(
            **{
                "channel": channel,
                "pattern": pattern,
                "data": self.backend._deserialise_record(msg["data"], self.record_class),  # type: ignore[attr-unresolved]
                "raw": msg["data"],
            }
        )


class RedisStorePushPopMixin(PushPopCapabilityMixin):

    connector: "RedisConnector"

    # Connections left free for non-blocking commands (push/publish/etc.)
    # so that BLPOP/BRPOP callers can never claim the whole pool.
    _BLOCKING_POOL_RESERVE = 2

    def _get_blocking_pop_semaphore(self) -> asyncio.Semaphore:
        sem = getattr(self, "_blocking_pop_semaphore", None)
        if sem is None:
            pool_size = getattr(self.connector.config, "pool_size", 10)
            limit = max(1, pool_size - self._BLOCKING_POOL_RESERVE)
            sem = asyncio.Semaphore(limit)
            self._blocking_pop_semaphore = sem
        return sem

    async def push(
        self, key: str, *values: Record, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            if side == QueueSide.LEFT:
                return await asyncio.to_thread(cursor.lpush, key, *payloads)
            else:
                return await asyncio.to_thread(cursor.rpush, key, *payloads)
        except RedisError as exc:
            raise ConnectionError(f"Failed to push to {key}: {exc}") from exc

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[Union[float, int]] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Record]:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            loop = asyncio.get_running_loop()

            cursor = self.connector.get_cursor()

            if timeout is None:
                # Non-blocking pop (safe for default to_thread)
                if side == QueueSide.LEFT:
                    raw = await asyncio.to_thread(cursor.lpop, key)
                else:
                    raw = await asyncio.to_thread(cursor.rpop, key)
            else:
                # Blocking pop (MUST use dedicated executor to prevent thread-pool starvation)
                func: Callable[[str, Union[float, int]], Any] = (
                    cursor.blpop if side == QueueSide.LEFT else cursor.brpop
                )

                async with self._get_blocking_pop_semaphore():
                    result = await loop.run_in_executor(
                        _BLOCKING_EXECUTOR, lambda: func(key, float(timeout))
                    )

                if result is None:
                    return None
                _, raw = result

            return self._deserialise_record(raw, record_class) if raw is not None else None  # type: ignore
        except RedisError as exc:
            raise ConnectionError(f"Failed to pop from {key}: {exc}") from exc

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
        results = []

        first_item = await self.pop(key, record_class, timeout=timeout, side=side)
        if first_item is None:
            return []
        results.append(first_item)

        # Drain whatever is already available, non-blocking — only the
        # first item above waits up to `timeout`.
        items_popped = 1
        while True:
            if limit is not None and items_popped >= limit:
                break

            result = await self.pop(key, record_class, timeout=None, side=side)

            if result is None:
                break

            results.append(result)

            items_popped += 1

        return results

    async def queue_length(self, key: str) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            return await asyncio.to_thread(cursor.llen, key)
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to get queue length for {key}: {exc}"
            ) from exc

    async def queue_range(
        self, key: str, record_class: Type[Record], start: int = 0, stop: int = -1
    ) -> List[Record]:
        try:
            self._ensure_connected()  # type: ignore

            cursor = self.connector.get_cursor()

            raws = await asyncio.to_thread(cursor.lrange, key, start, stop)
            return [self._deserialise_record(r, record_class) for r in raws]  # type: ignore
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to get queue range for {key}: {exc}"
            ) from exc


class RedisStreamCapabilityMixin(StreamCapabilityMixin):
    """Redis Streams implementation of the append-only durable event log."""

    connector: "RedisConnector"

    async def stream_append(
        self,
        stream_key: str,
        record: Any,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            payload = self._serialize_record(record)  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            kwargs: dict[str, Any] = {"id": "*"}
            if max_len is not None:
                kwargs["maxlen"] = max_len
                kwargs["approximate"] = approximate_trim

            entry_id = await asyncio.to_thread(
                cursor.xadd,
                stream_key,
                {"data": payload},
                **kwargs,
            )
            return entry_id

        except RedisError as exc:
            raise ConnectionError(
                f"Failed to append to stream '{stream_key}': {exc}"
            ) from exc

    async def stream_ensure_consumer_group(
        self,
        stream_key: str,
        group_name: str,
        start_from: str = StreamOffset.BEGINNING,
    ) -> None:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]
            # Translate Volnux canonical offsets to Redis-native values
            redis_start_id = self._translate_offset(start_from)

            cursor = self.connector.get_cursor()

            # XGROUP CREATE is idempotent when using MKSTREAM + ignoring BUSYGROUP
            try:
                await asyncio.to_thread(
                    cursor.xgroup_create,
                    stream_key,
                    group_name,
                    id=redis_start_id,
                    mkstream=True,
                )
            except RedisError as exc:
                if "BUSYGROUP" in str(exc).upper():
                    pass  # Group already exists — expected idempotent behavior
                else:
                    raise

        except RedisError as exc:
            raise ConnectionError(
                f"Failed to ensure consumer group '{group_name}' on '{stream_key}': {exc}"
            ) from exc

    async def stream_read(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[Record]]:
        try:
            self._ensure_connected()  # type: ignore
            loop = asyncio.get_running_loop()

            cursor = self.connector.get_cursor()

            if block_ms is None:
                # Non-blocking read — safe for default thread pool
                raw = await asyncio.to_thread(
                    cursor.xreadgroup,
                    group_name,
                    consumer_id or "default",
                    {stream_key: ">"},
                    count=count,
                )
            else:
                # BLOCKING read — MUST use dedicated executor
                effective_block = 0 if block_ms == 0 else block_ms
                raw = await loop.run_in_executor(
                    _BLOCKING_EXECUTOR,
                    lambda: cursor.xreadgroup(
                        group_name,
                        consumer_id or "default",
                        {stream_key: ">"},
                        count=count,
                        block=effective_block,
                    ),
                )

            return self._parse_xreadgroup_response(
                raw, stream_key, group_name, consumer_id, record_class
            )

        except RedisError as exc:
            raise ConnectionError(
                f"Failed to read from stream '{stream_key}': {exc}"
            ) from exc

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
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            try:
                # Preferred: XAUTOCLAIM is atomic and returns claimed entries directly
                raw = await asyncio.to_thread(
                    cursor.xautoclaim,
                    stream_key,
                    group_name,
                    consumer_id,
                    min_idle_time=min_idle_ms,
                    start_id="0-0",
                    count=count,
                )
                # xautoclaim returns (new_start_id, [(id, fields), ...], [deleted_ids])
                claimed_entries = (
                    raw[1] if isinstance(raw, (list, tuple)) and len(raw) > 1 else []
                )

            except (RedisError, AttributeError):
                # Fallback for Redis < 6.2: manual XPENDING + XCLAIM
                pending = await asyncio.to_thread(
                    cursor.xpending_range,
                    stream_key,
                    group_name,
                    min="-",
                    max="+",
                    count=count,
                    consumername=None,
                )
                eligible_ids = [
                    p["message_id"]
                    for p in pending
                    if p.get("time_since_delivered", 0) >= min_idle_ms
                ]
                if not eligible_ids:
                    return []
                claimed_entries = await asyncio.to_thread(
                    cursor.xclaim,
                    stream_key,
                    group_name,
                    consumer_id,
                    min_idle_time=min_idle_ms,
                    message_ids=eligible_ids,
                )

            results: List[StreamEntry] = []
            for entry_id, fields in claimed_entries:
                eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                data = fields.get(b"data") or fields.get("data")
                if data is None:
                    continue
                payload = data.decode() if isinstance(data, bytes) else data
                results.append(
                    StreamEntry(
                        entry_id=eid,
                        record=self._deserialise_record(payload, record_class),  # type: ignore[attr-unresolved]
                        group_name=group_name,
                        consumer_id=consumer_id,
                    )
                )
            return results

        except RedisError as exc:
            raise ConnectionError(
                f"Failed to claim pending entries on '{stream_key}': {exc}"
            ) from exc

    async def stream_ack(
        self,
        stream_key: str,
        group_name: str,
        *entry_ids: str,
    ) -> int:
        if not entry_ids:
            return 0
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]
            cursor = self.connector.get_cursor()
            return await asyncio.to_thread(
                cursor.xack, stream_key, group_name, *entry_ids
            )
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to ACK entries on '{stream_key}': {exc}"
            ) from exc

    async def stream_length(self, stream_key: str) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]
            cursor = self.connector.get_cursor()
            return await asyncio.to_thread(cursor.xlen, stream_key)
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to get length of '{stream_key}': {exc}"
            ) from exc

    async def stream_trim(
        self,
        stream_key: str,
        max_len: int,
        approximate: bool = True,
    ) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]
            cursor = self.connector.get_cursor()
            return await asyncio.to_thread(
                cursor.xtrim,
                stream_key,
                maxlen=max_len,
                approximate=approximate,
            )
        except RedisError as exc:
            raise ConnectionError(f"Failed to trim '{stream_key}': {exc}") from exc

    async def stream_delete(self, stream_key: str) -> bool:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]
            cursor = self.connector.get_cursor()
            result = await asyncio.to_thread(cursor.delete, stream_key)
            return bool(result)
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to delete stream '{stream_key}': {exc}"
            ) from exc

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

        results: List[StreamEntry] = []
        # XREADGROUP returns [[stream_key, [(id, fields), ...]], ...]
        for stream_result in raw:
            entries = stream_result[1] if len(stream_result) > 1 else []
            for entry_id, fields in entries:
                eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                data = fields.get(b"data") or fields.get("data")
                if data is None:
                    logger.warning(
                        "Stream entry %s missing 'data' field, skipping", eid
                    )
                    continue
                payload = data.decode() if isinstance(data, bytes) else data
                results.append(
                    StreamEntry(
                        entry_id=eid,
                        record=self._deserialise_record(payload, record_class),  # type: ignore[attr-unresolved]
                        group_name=group_name,
                        consumer_id=consumer_id,
                    )
                )
        return results
