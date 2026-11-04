import asyncio
import collections
import fnmatch
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    StreamCapabilityMixin,
    StreamEntry,
    StreamOffset,
    AsyncSubscriptionContext,
    QueueSide,
    Record,
    Message,
)

logger = logging.getLogger(__name__)


class _AsyncDeque:

    def __init__(self):
        self._deque: collections.deque = collections.deque()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()

    async def push(self, value: str, side: QueueSide) -> int:
        async with self._lock:
            if side == QueueSide.RIGHT:
                self._deque.append(value)
            else:
                self._deque.appendleft(value)
            self._not_empty.set()
        return len(self._deque)

    async def pop(
        self, side: QueueSide, timeout: Optional[float] = None
    ) -> Optional[str]:
        """Pop one value.

        ``timeout=None`` never waits; ``0`` waits indefinitely; ``> 0`` waits at
        most that many seconds (the Volnux contract). The wait is re-armed when
        another consumer wins the item, so a waiter only gives up when its own
        deadline passes, never because a sibling took the value first.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout else None

        while True:
            async with self._lock:
                if self._deque:
                    val = (
                        self._deque.popleft()
                        if side == QueueSide.LEFT
                        else self._deque.pop()
                    )
                    if not self._deque:
                        self._not_empty.clear()
                    return val
                # Cleared under the lock, and set under the lock in push(), so
                # a push can never slip between the emptiness check and the wait.
                self._not_empty.clear()

            if timeout is None:
                return None
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return None
            try:
                await asyncio.wait_for(self._not_empty.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return None

    def length(self) -> int:
        return len(self._deque)

    def range(self, start: int, stop: int) -> List[str]:
        """Inclusive slice with Redis LRANGE semantics (negative = from tail)."""
        items = list(self._deque)
        n = len(items)
        if start < 0:
            start = max(n + start, 0)
        if stop < 0:
            stop = n + stop
        stop = min(stop, n - 1)
        if start > stop:
            return []
        return items[start : stop + 1]


class InMemoryPubSubMixin(PubSubCapabilityMixin):

    def __post_init_pubsub__(self):
        if not hasattr(self, "_pubsub_lock"):
            self._pubsub_channels: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
            self._pubsub_patterns: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
            self._pubsub_lock = asyncio.Lock()

    async def publish(self, channel: str, record: Record) -> int:
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()

        # Delegates to KeyValueStoreBackendBase._serialize_record
        payload = self._serialize_record(record)  # type: ignore[attr-defined]
        receivers = 0

        async with self._pubsub_lock:
            for q in self._pubsub_channels.get(channel, set()):
                try:
                    q.put_nowait(
                        {"type": "message", "channel": channel, "data": payload}
                    )
                    receivers += 1
                except asyncio.QueueFull:
                    logger.warning(
                        "In-memory pubsub queue full for channel %r; dropping message",
                        channel,
                    )

            for pattern, queues in self._pubsub_patterns.items():
                if fnmatch.fnmatch(channel, pattern):
                    for q in queues:
                        try:
                            q.put_nowait(
                                {
                                    "type": "pmessage",
                                    "channel": channel,
                                    "pattern": pattern,
                                    "data": payload,
                                }
                            )
                            receivers += 1
                        except asyncio.QueueFull:
                            logger.warning(
                                "In-memory pubsub queue full for pattern %r (channel %r); "
                                "dropping message",
                                pattern,
                                channel,
                            )

        return receivers

    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "InMemorySubscriptionContext":
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()
        return InMemorySubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
            record_class=record_class,
        )

    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "InMemorySubscriptionContext":
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()
        return InMemorySubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
            record_class=record_class,
        )


class InMemorySubscriptionContext(AsyncSubscriptionContext):

    def __init__(
        self,
        backend: "InMemoryPubSubMixin",
        channels: List[str],
        patterns: List[str],
        record_class: Type[Record],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self.record_class = record_class
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

    async def __aenter__(self) -> "InMemorySubscriptionContext":
        async with self.backend._pubsub_lock:
            for c in self.channels:
                self.backend._pubsub_channels[c].add(self._queue)
            for p in self.patterns:
                self.backend._pubsub_patterns[p].add(self._queue)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        async with self.backend._pubsub_lock:
            for c in self.channels:
                channel_set = self.backend._pubsub_channels.get(c)
                if channel_set is not None:
                    channel_set.discard(self._queue)
                    if not channel_set:
                        del self.backend._pubsub_channels[c]
            for p in self.patterns:
                pattern_set = self.backend._pubsub_patterns.get(p)
                if pattern_set is not None:
                    pattern_set.discard(self._queue)
                    if not pattern_set:
                        del self.backend._pubsub_patterns[p]

        # Drain leftover messages to prevent memory leaks
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    def __aiter__(self) -> "InMemorySubscriptionContext":
        # NOTE: is synchronous (`def`) for native `async for` compatibility.
        # The async iteration logic lives in `__anext__`.
        return self

    async def __anext__(self) -> Message:
        msg = await self._queue.get()

        return Message(
            channel=msg["channel"],
            pattern=msg.get("pattern"),
            data=self.backend._deserialize_record(msg["data"], self.record_class),  # type: ignore[attr-defined]
            raw=msg,
        )


class InMemoryPushPopMixin(PushPopCapabilityMixin):

    def __post_init_queues__(self):
        if not hasattr(self, "_queues_lock"):
            self._queues: Dict[str, _AsyncDeque] = {}
            self._queues_lock = asyncio.Lock()

    async def _get_or_create_queue(self, key: str) -> _AsyncDeque:
        async with self._queues_lock:
            if key not in self._queues:
                self._queues[key] = _AsyncDeque()
            return self._queues[key]

    async def push(
        self, key: str, *values: Any, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()

        q = await self._get_or_create_queue(key)
        payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-defined]

        length = 0
        for p in payloads:
            length = await q.push(p, side)
        return length

    async def _get_queue(self, key: str, create: bool) -> Optional[_AsyncDeque]:
        async with self._queues_lock:
            q = self._queues.get(key)
            if q is None and create:
                q = self._queues[key] = _AsyncDeque()
            return q

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()

        # A blocking pop on a queue that doesn't exist yet must wait on the
        # deque a later push() will fill, so create it instead of sleeping.
        q = await self._get_queue(key, create=timeout is not None)
        if q is None:
            return None

        raw = await q.pop(side, timeout)
        return self._deserialize_record(raw, record_class) if raw is not None else None  # type: ignore[attr-defined]

    async def pop_many(
        self,
        key: str,
        record_class: Type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Record]:
        """
        Wait (per ``timeout``) for the first item, then drain up to ``limit``
        items in total (everything available if ``limit`` is None).
        """
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()
        if limit is not None and limit <= 0:
            return []

        q = await self._get_queue(key, create=timeout is not None)
        if q is None:
            return []

        first = await q.pop(side, timeout)
        if first is None:
            return []

        results = [self._deserialize_record(first, record_class)]  # type: ignore[attr-defined]
        while limit is None or len(results) < limit:
            raw = await q.pop(side, None)
            if raw is None:
                break
            results.append(self._deserialize_record(raw, record_class))  # type: ignore[attr-defined]
        return results

    async def queue_delete(self, key: str) -> bool:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()
        async with self._queues_lock:
            return self._queues.pop(key, None) is not None

    async def queue_length(self, key: str) -> int:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()
        async with self._queues_lock:
            q = self._queues.get(key)
        return q.length() if q else 0

    async def queue_range(
        self, key: str, record_class: Type[Record], start: int = 0, stop: int = -1
    ) -> List[Record]:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()
        async with self._queues_lock:
            q = self._queues.get(key)
        if not q:
            return []

        raws = q.range(start, stop)
        return [self._deserialize_record(r, record_class) for r in raws]  # type: ignore[attr-defined]


@dataclass
class _Group:
    """A consumer group's position in one stream."""

    cursor: int = 0  # sequence number of the last entry handed out
    #: delivered but not yet acknowledged: seq -> (consumer id, delivered at)
    pending: Dict[int, Tuple[Optional[str], float]] = field(default_factory=dict)


class _MemoryStream:
    """One append-only log: entries in order, plus its consumer groups."""

    def __init__(self) -> None:
        self.entries: List[Tuple[int, Any]] = []  # (sequence number, payload)
        self.next_seq = 1
        self.groups: Dict[str, _Group] = {}
        self.changed = asyncio.Condition()


def _entry_id(seq: int) -> str:
    return f"{seq}-0"


def _seq_of(entry_id: str) -> int:
    return int(str(entry_id).split("-", 1)[0])


class InMemoryStreamMixin(StreamCapabilityMixin):
    """Append-only streams with consumer groups, held in memory (tests, single process).

    Follows the contract of :class:`StreamCapabilityMixin`: appends never block, a
    group reads each entry once until it is acknowledged, an unacknowledged entry
    can be claimed by another consumer once idle, and a consumer can replay the
    whole log by joining a group at the beginning.
    """

    def __post_init_streams__(self) -> None:
        if not hasattr(self, "_streams"):
            self._streams: Dict[str, _MemoryStream] = {}

    def _stream(self, key: str, create: bool = True) -> Optional[_MemoryStream]:
        self.__post_init_streams__()
        stream = self._streams.get(key)
        if stream is None and create:
            stream = self._streams[key] = _MemoryStream()
        return stream

    @staticmethod
    def _group(stream: _MemoryStream, name: str) -> _Group:
        # Joining a group that was never created starts it at the beginning.
        group = stream.groups.get(name)
        if group is None:
            group = stream.groups[name] = _Group()
        return group

    async def stream_append(
        self,
        stream_key: str,
        record: Record,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        payload = self._serialize_record(record)  # type: ignore[attr-defined]
        stream = self._stream(stream_key)
        async with stream.changed:
            seq = stream.next_seq
            stream.next_seq += 1
            stream.entries.append((seq, payload))
            if max_len is not None and len(stream.entries) > max_len:
                del stream.entries[: len(stream.entries) - max_len]
            stream.changed.notify_all()
        return _entry_id(seq)

    async def stream_ensure_consumer_group(
        self,
        stream_key: str,
        group_name: str,
        start_from: Union[StreamOffset, str] = StreamOffset.BEGINNING,
    ) -> None:
        stream = self._stream(stream_key)  # a consumer may join before the producer
        async with stream.changed:
            if group_name in stream.groups:
                return
            if start_from == StreamOffset.BEGINNING:
                cursor = 0
            elif start_from == StreamOffset.LATEST:
                cursor = stream.next_seq - 1
            else:
                cursor = _seq_of(start_from)
            stream.groups[group_name] = _Group(cursor=cursor)

    async def stream_read(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[Record]]:
        stream = self._stream(stream_key)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + block_ms / 1000 if block_ms not in (None, 0) else None

        async with stream.changed:
            group = self._group(stream, group_name)
            while True:
                fresh = [(q, p) for q, p in stream.entries if q > group.cursor][:count]
                if fresh:
                    now = time.monotonic()
                    for seq, _payload in fresh:
                        group.cursor = seq
                        group.pending[seq] = (consumer_id, now)
                    return [
                        StreamEntry(
                            entry_id=_entry_id(seq),
                            record=self._deserialize_record(  # type: ignore[attr-defined]
                                payload, record_class
                            ),
                            group_name=group_name,
                            consumer_id=consumer_id,
                        )
                        for seq, payload in fresh
                    ]
                if block_ms is None:
                    return []
                remaining = None if deadline is None else deadline - loop.time()
                if remaining is not None and remaining <= 0:
                    return []
                try:
                    await asyncio.wait_for(stream.changed.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return []

    async def stream_ack(
        self, stream_key: str, group_name: str, *entry_ids: str
    ) -> int:
        stream = self._stream(stream_key, create=False)
        if stream is None or group_name not in stream.groups:
            return 0
        group = stream.groups[group_name]
        acknowledged = 0
        for entry_id in entry_ids:
            if group.pending.pop(_seq_of(entry_id), None) is not None:
                acknowledged += 1
        return acknowledged

    async def stream_claim_pending(
        self,
        stream_key: str,
        group_name: str,
        consumer_id: str,
        record_class: Type[Record],
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> List[StreamEntry[Record]]:
        stream = self._stream(stream_key, create=False)
        if stream is None or group_name not in stream.groups:
            return []
        group = stream.groups[group_name]
        payloads = dict(stream.entries)
        now = time.monotonic()
        claimed: List[StreamEntry[Record]] = []
        for seq in sorted(group.pending):
            if len(claimed) >= count:
                break
            _owner, delivered_at = group.pending[seq]
            if (now - delivered_at) * 1000 < min_idle_ms:
                continue
            if seq not in payloads:  # trimmed away
                group.pending.pop(seq)
                continue
            group.pending[seq] = (consumer_id, now)
            claimed.append(
                StreamEntry(
                    entry_id=_entry_id(seq),
                    record=self._deserialize_record(  # type: ignore[attr-defined]
                        payloads[seq], record_class
                    ),
                    group_name=group_name,
                    consumer_id=consumer_id,
                )
            )
        return claimed

    async def stream_length(self, stream_key: str) -> int:
        stream = self._stream(stream_key, create=False)
        return len(stream.entries) if stream else 0

    async def stream_trim(
        self, stream_key: str, max_len: int, approximate: bool = True
    ) -> int:
        stream = self._stream(stream_key, create=False)
        if stream is None or len(stream.entries) <= max_len:
            return 0
        evicted = len(stream.entries) - max_len
        del stream.entries[:evicted]
        return evicted

    async def stream_delete(self, stream_key: str) -> bool:
        self.__post_init_streams__()
        stream = self._streams.pop(stream_key, None)
        if stream is None:
            return False
        async with stream.changed:
            stream.entries.clear()
            stream.changed.notify_all()  # wake blocked readers; they find nothing
        return True
