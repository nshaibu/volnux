import asyncio
import collections
import fnmatch
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
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
        if not self._deque:
            if timeout is None:
                return None  # Non-blocking immediate return

            # timeout=0 means wait forever per Volnux contract
            wait_time = None if timeout == 0 else timeout
            try:
                await asyncio.wait_for(self._not_empty.wait(), timeout=wait_time)
            except asyncio.TimeoutError:
                return None

        async with self._lock:
            if not self._deque:
                self._not_empty.clear()
                return None

            val = self._deque.popleft() if side == QueueSide.LEFT else self._deque.pop()
            if not self._deque:
                self._not_empty.clear()
            return val

    def length(self) -> int:
        return len(self._deque)

    def range(self, start: int, stop: int) -> List[str]:
        q_list = list(self._deque)
        if stop == -1:
            stop = len(q_list)
        else:
            stop = stop + 1
        return q_list[start:stop]


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
        payload = self._serialize_record(record)  # type: ignore[attr-unresolved]
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
            data=self.backend._deserialise_record(msg["data"], self.record_class),  # type: ignore[attr-unresolved]
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
        payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-unresolved]

        length = 0
        for p in payloads:
            length = await q.push(p, side)
        return length

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()

        async with self._queues_lock:
            q = self._queues.get(key)

        if not q:
            if timeout is None:
                return None
            await asyncio.sleep(timeout)
            return None

        raw = await q.pop(side, timeout)
        return self._deserialise_record(raw, record_class) if raw is not None else None  # type: ignore[attr-unresolved]

    async def pop_many(
        self,
        key: str,
        record_class: Type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Record]:
        """
        Block until the queue has data, then drain a batch of items.
        """
        if not hasattr(self, "_queues_lock"):
            self.__post_init_queues__()

        async with self._queues_lock:
            q = self._queues.get(key)

        if not q and timeout is None:
            return []

        if not q:
            q = await self._get_or_create_queue(key)

        if q.length() == 0:
            if timeout is None:
                return []  # Non-blocking immediate return

            # Use the underlying _AsyncDeque's pop with timeout just to wait for the signal,
            # but we don't want to consume the item yet if we are going to drain.
            # Actually, it's easier to just use the _not_empty event directly if exposed,
            # or just do a blocking pop for the first item.
            first_item_raw = await q.pop(side, timeout=timeout)
            if first_item_raw is None:
                return []  # Timeout expired

            # We got the first item!
            results = [self._deserialise_record(first_item_raw, record_class)]  # type: ignore[attr-unresolved]
        else:
            results = []

        # Since this is in-memory, draining is instantaneous.
        items_popped = 1
        while True:
            if limit is not None and items_popped >= limit:
                break

            # Non-blocking pop for subsequent items
            raw = await q.pop(side, timeout=None)
            if raw is None:
                break

            results.append(self._deserialise_record(raw, record_class))  # type: ignore[attr-unresolved]
            items_popped += 1

        return results

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
        return [self._deserialise_record(r) for r in raws]  # type: ignore[attr-unresolved]
