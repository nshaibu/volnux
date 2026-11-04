"""Kafka implementations of the Volnux messaging capabilities.

Mapping onto Kafka (every logical name becomes a topic with a per-paradigm
suffix, so the same model can use pub/sub, a queue and a stream side by side):

=============  ===========  ===================================================
Capability     Topic        Semantics
=============  ===========  ===================================================
Pub/Sub        ``<n>.ps``   Broadcast. Each subscription uses a private consumer
                            group and sees only messages published after it
                            subscribed. Retention is short (``pubsub_retention_ms``).
Push/Pop       ``<n>.q``    FIFO work queue. All poppers share one consumer group;
                            ``pop`` commits the offset (destructive). Kafka is an
                            append-only log, so only ``push(side=RIGHT)`` and
                            ``pop(side=LEFT)`` are supported.
Streams        ``<n>.log``  Append-only log with consumer groups. ``stream_ack``
                            commits offsets (up to the contiguous acknowledged
                            prefix per partition: Kafka has no per-message ack).
=============  ===========  ===================================================

Ordering is only guaranteed per partition; topics default to one partition so
queues and streams are totally ordered. Raise ``num_partitions`` to scale out
and accept per-partition ordering.

Blocking reads run on worker threads in short slices and are cancellation safe:
messages fetched for a caller that has gone away are re-queued (consumer
rewound / item re-produced) instead of being lost.
"""

import asyncio
import concurrent.futures
import functools
import hashlib
import logging
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

from confluent_kafka import (
    OFFSET_BEGINNING,
    KafkaError,
    KafkaException,
    Message as KafkaMessage,
    TopicPartition,
)
from confluent_kafka.admin import NewTopic

from volnux.backends.messaging.base import (
    AsyncSubscriptionContext,
    Message,
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    QueueSide,
    Record,
    StreamCapabilityMixin,
    StreamEntry,
    StreamOffset,
)
from volnux.backends.messaging.util import _BLOCKING_EXECUTOR
from volnux.concurrency.kv_executor import run_in_kv_executor

logger = logging.getLogger(__name__)

# Topic kinds -> name suffix
KIND_KV = "kv"
KIND_PUBSUB = "ps"
KIND_QUEUE = "q"
KIND_STREAM = "log"

CHANNEL_HEADER = "volnux-channel"
EXPIRES_HEADER = "volnux-expires-at"

# Commit errors meaning "this group has live members" (so it already exists).
_GROUP_ALREADY_ACTIVE = (
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.REBALANCE_IN_PROGRESS,
)

_MAX_TOPIC_LEN = 249
_INVALID_TOPIC_CHARS = re.compile(r"[^a-zA-Z0-9._-]")

# Longest a worker thread stays inside one broker poll, which bounds how long it
# outlives a cancelled caller.
_BLOCKING_SLICE = 1.0

# Strong references to in-flight background repair tasks.
_PENDING_REPAIRS: "Set[asyncio.Future[Any]]" = set()


# Naming helpers


def sanitize_name(name: str) -> str:
    """Map an arbitrary logical name onto the characters Kafka allows."""
    return _INVALID_TOPIC_CHARS.sub("_", name)


def topic_name(base: str, kind: str) -> str:
    """Kafka topic for logical ``base`` and paradigm ``kind``.

    Names over Kafka's 249 character limit are truncated and disambiguated with
    a hash, so they stay unique and stable.
    """
    topic = f"{sanitize_name(base)}.{kind}"
    if len(topic) > _MAX_TOPIC_LEN:
        digest = hashlib.sha1(topic.encode()).hexdigest()[:12]
        topic = f"{topic[: _MAX_TOPIC_LEN - 13]}-{digest}"
    return topic


def glob_to_regex(pattern: str) -> "re.Pattern[str]":
    """Compile a Redis-style glob (``* ? [abc]``) against sanitized topic bases."""
    out: List[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "*":
            out.append("[a-zA-Z0-9._-]*")
        elif ch == "?":
            out.append("[a-zA-Z0-9._-]")
        elif ch == "[":
            end = pattern.find("]", i + 1)
            if end == -1:
                out.append(re.escape(sanitize_name(ch)))
            else:
                body = pattern[i + 1 : end]
                negate = body.startswith("!") or body.startswith("^")
                body = sanitize_name(body[1:] if negate else body)
                out.append("[" + ("^" if negate else "") + re.escape(body) + "]")
                i = end
        else:
            out.append(re.escape(sanitize_name(ch)))
        i += 1
    return re.compile("".join(out))


def _lrange(items: List[Any], start: int, stop: int) -> List[Any]:
    """Inclusive slice with Redis ``LRANGE`` semantics (negative = from the tail)."""
    n = len(items)
    if start < 0:
        start = max(n + start, 0)
    if stop < 0:
        stop = n + stop
    stop = min(stop, n - 1)
    return [] if start > stop else items[start : stop + 1]


def _split_entry_id(entry_id: str) -> Tuple[int, int]:
    partition, _, offset = entry_id.partition("-")
    return int(partition), int(offset)


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def _header(msg: KafkaMessage, name: str) -> Optional[bytes]:
    for key, value in msg.headers() or ():
        if key == name:
            return value
    return None


def _kafka_errors(message: str) -> "_KafkaErrors":
    return _KafkaErrors(message)


class _KafkaErrors:
    """Re-raise ``KafkaException`` as ``ConnectionError`` prefixed with a message."""

    def __init__(self, message: str) -> None:
        self.message = message

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if isinstance(exc, KafkaException):
            raise ConnectionError(f"{self.message}: {exc}") from exc


# Group consumer: one Consumer + at-least-once bookkeeping


@dataclass
class _Pending:
    raw: bytes
    delivered_at: float
    owner: str


class _GroupConsumer:
    """A ``confluent_kafka.Consumer`` that is a member of a consumer group.

    Adds the pieces Kafka lacks natively but the Volnux capabilities need:

    * **Per-entry acknowledgement.** Delivered entries are tracked as pending;
      ``ack`` commits the offset after the longest contiguous acknowledged
      prefix of each partition, so an unacknowledged entry (and everything after
      it) is redelivered after a restart or rebalance (at-least-once).
    * **Cancellation safety.** ``poll_batch`` honours a cancel flag and rewinds
      the consumer for anything fetched for a caller that has left.

    The consumer is not thread safe, so fetching is serialised by ``_poll_lock``
    while the bookkeeping has its own lock and is never held across a broker
    call.
    """

    def __init__(self, consumer: Any, topic: str, group_id: str) -> None:
        self.consumer = consumer
        self.topic = topic
        self.group_id = group_id
        self._state = threading.RLock()
        self._poll_lock = threading.Lock()
        self._assigned = threading.Event()
        self._delivered: Dict[int, Deque[int]] = {}
        self._acked: Dict[int, Set[int]] = {}
        self._pending: Dict[str, _Pending] = {}
        self._closed = False
        self._subscribed = False

    def _ensure_subscribed(self) -> None:
        """Join the group on first fetch, never earlier.

        Metadata-only uses (``lag``, ``ack``...) must not make this process a
        group member: a member that never polls would hold partitions and starve
        the real consumers until ``max.poll.interval.ms`` expires.
        """
        if not self._subscribed:
            self.consumer.subscribe(
                [self.topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_revoke,
            )
            self._subscribed = True

    # rebalance callbacks (run inside consume(), i.e. under _poll_lock)

    def _on_assign(self, consumer: Any, partitions: List[TopicPartition]) -> None:
        self._assigned.set()

    def _on_revoke(self, consumer: Any, partitions: List[TopicPartition]) -> None:
        revoked = {tp.partition for tp in partitions}
        with self._state:
            for partition in revoked:
                self._delivered.pop(partition, None)
                self._acked.pop(partition, None)
            for entry_id in [
                eid for eid in self._pending if _split_entry_id(eid)[0] in revoked
            ]:
                del self._pending[entry_id]
        self._assigned.clear()

    # fetching

    def poll_batch(
        self,
        count: int,
        timeout: Optional[float],
        cancelled: threading.Event,
        join_timeout: float,
    ) -> List[KafkaMessage]:
        """Fetch up to ``count`` messages.

        ``timeout``: ``None`` = best-effort without waiting for new data,
        ``0`` = wait indefinitely, ``> 0`` = wait at most that many seconds.
        Messages are returned *unregistered*: the caller registers them only
        once it has really received them (see ``register``).
        """
        nonblocking = timeout is None
        deadline: Optional[float] = None
        if timeout is not None and timeout > 0:
            deadline = time.monotonic() + timeout
        join_deadline = time.monotonic() + join_timeout
        was_assigned = self._assigned.is_set()
        grace_used = False

        with self._poll_lock:
            self._ensure_subscribed()
            while True:
                if cancelled.is_set() or self._closed:
                    return []
                if nonblocking:
                    wait = 0.1 if was_assigned else (0.3 if grace_used else 0.2)
                else:
                    wait = _BLOCKING_SLICE
                    if deadline is not None:
                        wait = min(wait, max(deadline - time.monotonic(), 0.0))

                messages = self.consumer.consume(num_messages=count, timeout=wait)
                good = self._filter_errors(messages)
                if good:
                    if cancelled.is_set():
                        self.rewind(good)
                        return []
                    return good

                if nonblocking:
                    if not self._assigned.is_set():
                        if time.monotonic() >= join_deadline:
                            return []
                        continue
                    if was_assigned or grace_used:
                        return []
                    grace_used = True  # one more fetch right after joining
                    continue
                if deadline is not None and time.monotonic() >= deadline:
                    return []

    def drain(
        self, limit: Optional[int], cancelled: threading.Event
    ) -> List[KafkaMessage]:
        """Fetch what is already available (up to ``limit``) without waiting."""
        out: List[KafkaMessage] = []
        with self._poll_lock:
            self._ensure_subscribed()
            while limit is None or len(out) < limit:
                batch = self.consumer.consume(
                    num_messages=min(500, (limit - len(out)) if limit else 500),
                    timeout=0.05,
                )
                good = self._filter_errors(batch)
                if not good:
                    break
                out.extend(good)
            if cancelled.is_set() and out:
                self.rewind(out)
                return []
        return out

    @staticmethod
    def _filter_errors(messages: List[KafkaMessage]) -> List[KafkaMessage]:
        good: List[KafkaMessage] = []
        for msg in messages:
            error = msg.error()
            if error is None:
                good.append(msg)
            elif error.code() == KafkaError._PARTITION_EOF:
                continue
            elif error.fatal():
                raise KafkaException(error)
            else:
                logger.warning("Kafka consumer error: %s", error)
        return good

    def rewind(self, messages: List[KafkaMessage]) -> None:
        """Seek back so ``messages`` are fetched again (they were never handed out)."""
        first: Dict[int, int] = {}
        for msg in messages:
            first[msg.partition()] = min(
                msg.offset(), first.get(msg.partition(), msg.offset())
            )
        for partition, offset in first.items():
            try:
                self.consumer.seek(TopicPartition(self.topic, partition, offset))
            except KafkaException as exc:
                # Partition no longer ours: the new owner re-reads from the
                # committed offset anyway.
                logger.debug("Could not rewind %s[%s]: %s", self.topic, partition, exc)

    # delivery bookkeeping

    def register(
        self, messages: List[KafkaMessage], owner: str
    ) -> List[Tuple[str, KafkaMessage]]:
        """Record ``messages`` as delivered to ``owner`` and awaiting an ack."""
        now = time.monotonic()
        entries: List[Tuple[str, KafkaMessage]] = []
        with self._state:
            for msg in messages:
                entry_id = f"{msg.partition()}-{msg.offset()}"
                self._delivered.setdefault(msg.partition(), deque()).append(
                    msg.offset()
                )
                self._pending[entry_id] = _Pending(msg.value(), now, owner)
                entries.append((entry_id, msg))
        return entries

    def ack(self, entry_ids: List[str]) -> int:
        """Acknowledge entries and commit the contiguous acknowledged prefix."""
        commits: Dict[int, int] = {}
        with self._state:
            acked = [eid for eid in entry_ids if eid in self._pending]
            for entry_id in acked:
                partition, offset = _split_entry_id(entry_id)
                self._acked.setdefault(partition, set()).add(offset)
                del self._pending[entry_id]
            for partition in {_split_entry_id(eid)[0] for eid in acked}:
                queue = self._delivered.get(partition)
                done = self._acked.get(partition, set())
                last = None
                while queue and queue[0] in done:
                    last = queue.popleft()
                    done.discard(last)
                if last is not None:
                    commits[partition] = last + 1
        if commits:
            self.consumer.commit(
                offsets=[
                    TopicPartition(self.topic, partition, offset)
                    for partition, offset in commits.items()
                ],
                asynchronous=False,
            )
        return len(acked)

    def has_pending(self, entry_id: str) -> bool:
        with self._state:
            return entry_id in self._pending

    def claim_idle(
        self, owner: str, min_idle_ms: int, count: int
    ) -> List[Tuple[str, bytes]]:
        """Hand entries stuck with another owner over to ``owner``."""
        now = time.monotonic()
        claimed: List[Tuple[str, bytes]] = []
        with self._state:
            stale = sorted(
                (
                    eid
                    for eid, p in self._pending.items()
                    if (now - p.delivered_at) * 1000 >= min_idle_ms
                ),
                key=_split_entry_id,
            )
            for entry_id in stale[:count]:
                pending = self._pending[entry_id]
                pending.owner, pending.delivered_at = owner, now
                claimed.append((entry_id, pending.raw))
        return claimed

    def lag(self, timeout: float = 10.0) -> int:
        """Messages not yet committed by the group, across all partitions."""
        metadata = self.consumer.list_topics(self.topic, timeout=timeout)
        topic_meta = metadata.topics.get(self.topic)
        if topic_meta is None or topic_meta.error is not None:
            return 0
        partitions = [TopicPartition(self.topic, p) for p in topic_meta.partitions]
        total = 0
        for tp in self.consumer.committed(partitions, timeout=timeout):
            low, high = self.consumer.get_watermark_offsets(
                tp, timeout=timeout, cached=False
            )
            start = tp.offset if tp.offset >= 0 else low
            total += max(0, high - max(start, low))
        return total

    def close(self) -> None:
        self._closed = True
        with self._poll_lock:  # polls leave within one slice once _closed is set
            try:
                self.consumer.close()
            except Exception as exc:  # already closed / broker gone
                logger.debug("Error closing Kafka consumer: %s", exc)


# Shared plumbing


class _KafkaState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.topics: Set[str] = set()
        self.group_consumers: Dict[Tuple[str, str, str], _GroupConsumer] = {}
        self.append_counts: Dict[str, int] = {}
        self.meta_consumer: Any = None
        self.meta_lock = threading.Lock()


class KafkaMessagingMixin:
    """Topic management, producing and blocking-read plumbing for the Kafka capabilities.

    Expects the host backend to provide ``connector`` (a ``KafkaConnector``),
    ``_ensure_connected()``, ``_serialize_record()`` and ``_deserialize_record()``
    (see ``KeyValueStoreBackendBase``).
    """

    connector: Any

    # Defaults, overridable through connector options (``KafkaStoreBackend(...)``).
    _DEFAULT_OPTIONS: Dict[str, Any] = {
        "num_partitions": 1,
        "replication_factor": 1,
        "topic_config": {},
        "pubsub_retention_ms": 3_600_000,
        "group_join_timeout": 15.0,
        "produce_timeout": 30.0,
        "max_blocking": 32,
        "pattern_refresh_interval": 1.0,
        "default_consumer_id": "default",
    }

    @property
    def _kafka(self) -> _KafkaState:
        state = self.__dict__.get("_kafka_state")
        if state is None:
            state = self.__dict__.setdefault("_kafka_state", _KafkaState())
        return state  # type: ignore[no-any-return]

    def _opt(self, name: str) -> Any:
        return self.connector.config.extra_params.get(name, self._DEFAULT_OPTIONS[name])

    # topic management

    def _topic_config(self, kind: str) -> Dict[str, str]:
        config: Dict[str, str] = {}
        if kind == KIND_KV:
            config["cleanup.policy"] = "compact"
        elif kind == KIND_PUBSUB:
            config["retention.ms"] = str(self._opt("pubsub_retention_ms"))
        config.update({k: str(v) for k, v in self._opt("topic_config").items()})
        return config

    def _ensure_topic_sync(self, topic: str, kind: str) -> None:
        state = self._kafka
        if topic in state.topics:
            return
        with state.lock:
            if topic in state.topics:
                return
            self._ensure_connected()  # type: ignore[attr-defined]
            new_topic = NewTopic(
                topic,
                num_partitions=int(self._opt("num_partitions")),
                replication_factor=int(self._opt("replication_factor")),
                config=self._topic_config(kind),
            )
            futures = self.connector.admin.create_topics(
                [new_topic], request_timeout=self.connector.request_timeout
            )
            try:
                futures[topic].result()
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise ConnectionError(
                        f"Failed to create Kafka topic '{topic}': {exc}"
                    ) from exc
            self._wait_topic_visible(topic)
            state.topics.add(topic)

    def _wait_topic_visible(self, topic: str, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            metadata = self.connector.admin.list_topics(
                topic, timeout=self.connector.request_timeout
            )
            topic_meta = metadata.topics.get(topic)
            if (
                topic_meta is not None
                and topic_meta.error is None
                and topic_meta.partitions
            ):
                return
            if time.monotonic() >= deadline:
                raise ConnectionError(f"Kafka topic '{topic}' did not become available")
            time.sleep(0.05)

    async def _ensure_topic(self, topic: str, kind: str) -> None:
        if topic not in self._kafka.topics:
            await self._run(self._ensure_topic_sync, topic, kind)

    async def _run(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        return await run_in_kv_executor(func, *args, **kwargs)

    # producing

    def _produce_sync(
        self,
        topic: str,
        items: List[
            Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]
        ],
    ) -> List[Tuple[int, int]]:
        """Produce ``(key, value, headers)`` items and wait for broker acks.

        Returns the ``(partition, offset)`` of every item, in order.
        """
        self._ensure_connected()  # type: ignore[attr-defined]
        producer = self.connector.producer
        results: List[Optional[Tuple[int, int]]] = [None] * len(items)
        errors: List[Any] = []

        def callback_for(index: int) -> Any:
            def on_delivery(error: Any, msg: KafkaMessage) -> None:
                if error is not None:
                    errors.append(error)
                else:
                    results[index] = (msg.partition(), msg.offset())

            return on_delivery

        for index, (key, value, headers) in enumerate(items):
            while True:
                try:
                    producer.produce(
                        topic,
                        value=value,
                        key=key,
                        headers=headers,
                        on_delivery=callback_for(index),
                    )
                    break
                except BufferError:  # local queue full: let deliveries drain
                    producer.poll(0.1)
        unsent = producer.flush(float(self._opt("produce_timeout")))
        if errors:
            raise ConnectionError(f"Failed to produce to '{topic}': {errors[0]}")
        if unsent:
            raise ConnectionError(
                f"Timed out producing to '{topic}': {unsent} message(s) unsent"
            )
        return [r for r in results if r is not None]

    # metadata

    def _watermarks(self, topic: str) -> Dict[int, Tuple[int, int]]:
        """``{partition: (low, high)}`` for ``topic`` (empty if it doesn't exist)."""
        state = self._kafka
        with state.meta_lock:
            if state.meta_consumer is None:
                state.meta_consumer = self.connector.new_consumer(
                    {"group.id": f"volnux-meta-{uuid.uuid4().hex[:8]}"}
                )
            consumer = state.meta_consumer
            timeout = self.connector.request_timeout
            topic_meta = consumer.list_topics(topic, timeout=timeout).topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                return {}
            return {
                p: consumer.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=timeout, cached=False
                )
                for p in topic_meta.partitions
            }

    # group consumers

    def _group_consumer_sync(
        self, topic: str, group_id: str, consumer_id: str, reset: str = "earliest"
    ) -> _GroupConsumer:
        state = self._kafka
        key = (topic, group_id, consumer_id)
        with state.lock:
            existing = state.group_consumers.get(key)
            if existing is not None:
                return existing
            self._ensure_connected()  # type: ignore[attr-defined]
            consumer = self.connector.new_consumer(
                {
                    "group.id": group_id,
                    "client.id": f"{self.connector.client_id}-{consumer_id}",
                    "auto.offset.reset": reset,
                    "enable.partition.eof": False,
                }
            )
            created = _GroupConsumer(consumer, topic, group_id)
            state.group_consumers[key] = created
            return created

    def _close_group_consumers(
        self, topic: str, group_id: Optional[str] = None
    ) -> None:
        state = self._kafka
        with state.lock:
            doomed = [
                key
                for key in state.group_consumers
                if key[0] == topic and (group_id is None or key[1] == group_id)
            ]
            consumers = [state.group_consumers.pop(key) for key in doomed]
        for consumer in consumers:
            consumer.close()

    def _consumers_for(self, topic: str, group_id: str) -> List[_GroupConsumer]:
        state = self._kafka
        with state.lock:
            return [
                gc
                for (t, g, _), gc in state.group_consumers.items()
                if t == topic and g == group_id
            ]

    def close_messaging(self) -> None:
        """Close every consumer this backend opened (called from ``close()``)."""
        state = self._kafka
        with state.lock:
            consumers = list(state.group_consumers.values())
            state.group_consumers.clear()
            meta, state.meta_consumer = state.meta_consumer, None
            state.topics.clear()
        for consumer in consumers:
            consumer.close()
        if meta is not None:
            try:
                meta.close()
            except Exception as exc:
                logger.debug("Error closing Kafka metadata consumer: %s", exc)

    def _delete_topic_sync(self, topic: str) -> bool:
        existed = bool(self._watermarks(topic))
        self._close_group_consumers(topic)
        self._kafka.topics.discard(topic)
        if not existed:
            return False
        futures = self.connector.admin.delete_topics(
            [topic], request_timeout=self.connector.request_timeout
        )
        try:
            futures[topic].result()
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return False
            raise
        deadline = time.monotonic() + 15.0
        while self._watermarks(topic) and time.monotonic() < deadline:
            time.sleep(0.1)  # deletion is asynchronous on the broker
        return True

    # cancellation-safe blocking fetch

    def _get_blocking_semaphore(self) -> asyncio.Semaphore:
        # asyncio primitives bind to a loop; keep one semaphore per running loop.
        loop = asyncio.get_running_loop()
        cached = self.__dict__.get("_blocking_semaphore")
        if cached is None or cached[0] is not loop:
            cached = (loop, asyncio.Semaphore(max(1, int(self._opt("max_blocking")))))
            self.__dict__["_blocking_semaphore"] = cached
        return cached[1]  # type: ignore[no-any-return]

    async def _poll(
        self, gc: _GroupConsumer, count: int, timeout: Optional[float]
    ) -> List[KafkaMessage]:
        """Fetch messages on a worker thread without losing any on cancellation.

        If the caller is cancelled while the worker is fetching, the worker
        stops within one slice; anything it still managed to fetch is rewound
        so it is delivered to the next reader instead of vanishing.
        """
        loop = asyncio.get_running_loop()
        semaphore = self._get_blocking_semaphore()
        cancelled = threading.Event()

        await semaphore.acquire()
        try:
            future = loop.run_in_executor(
                _BLOCKING_EXECUTOR,
                functools.partial(
                    gc.poll_batch,
                    count,
                    timeout,
                    cancelled,
                    float(self._opt("group_join_timeout")),
                ),
            )
        except BaseException:
            semaphore.release()
            raise
        future.add_done_callback(lambda _f: semaphore.release())

        try:
            return await asyncio.shield(future)  # type: ignore[no-any-return]
        except asyncio.CancelledError:
            cancelled.set()
            future.add_done_callback(functools.partial(self._rewind_orphan, gc))
            raise

    def _rewind_orphan(self, gc: _GroupConsumer, future: "asyncio.Future[Any]") -> None:
        if future.cancelled() or future.exception() is not None:
            return
        messages = future.result()
        if messages:
            task = asyncio.ensure_future(self._run(gc.rewind, messages))
            _PENDING_REPAIRS.add(task)
            task.add_done_callback(_PENDING_REPAIRS.discard)

    def _deserialize(self, raw: Any, record_class: Type[Record]) -> Record:
        return self._deserialize_record(raw, record_class)  # type: ignore[attr-defined,no-any-return]


# Pub/Sub


class KafkaStorePubSubMixin(KafkaMessagingMixin, PubSubCapabilityMixin):
    """Broadcast messaging on ``<channel>.ps`` topics."""

    async def publish(self, channel: str, record: Record) -> int:
        """Publish ``record`` to ``channel``.

        Kafka does not track live subscribers, so unlike Redis the return value
        is not a receiver count: it is ``1`` once the broker has durably
        accepted the message. Subscribers that join later never see it.
        """
        payload = self._serialize_record(record)  # type: ignore[attr-defined]
        topic = topic_name(channel, KIND_PUBSUB)
        with _kafka_errors(f"Failed to publish to {channel}"):
            await self._ensure_topic(topic, KIND_PUBSUB)
            await self._run(
                self._produce_sync,
                topic,
                [(None, payload, [(CHANNEL_HEADER, channel.encode())])],
            )
        return 1

    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "KafkaSubscriptionContext":
        if not channels:
            raise ValueError("subscribe() requires at least one channel")
        return KafkaSubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
            record_class=record_class,
        )

    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "KafkaSubscriptionContext":
        if not patterns:
            raise ValueError("psubscribe() requires at least one pattern")
        return KafkaSubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
            record_class=record_class,
        )


class KafkaSubscriptionContext(AsyncSubscriptionContext):
    """Bridges a blocking Kafka consumer to an async iterator.

    A worker thread owns a private consumer group (so every subscription gets
    every message) and hands messages to the event loop through a bounded
    ``asyncio.Queue``. A full queue blocks the worker, so Kafka applies natural
    backpressure instead of messages being dropped.

    Subscriptions start at the *end* of each topic (only messages published
    afterwards are delivered) and ``__aenter__`` returns only once that position
    is fixed, so a publish made right after entering is never missed.

    Patterns are matched client-side: the worker subscribes to the topics that
    currently match and re-lists topics periodically, picking up channels created
    later. Topics that appear after subscribing are read from their beginning.
    """

    _CLOSE_SENTINEL = object()
    _QUEUE_MAXSIZE = 10000
    _READY_TIMEOUT = 30.0

    def __init__(
        self,
        backend: "KafkaStorePubSubMixin",
        channels: List[str],
        patterns: List[str],
        record_class: Type[Record],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self.record_class = record_class
        self._pattern_res = [glob_to_regex(p) for p in patterns]
        self._exact_topics = {topic_name(c, KIND_PUBSUB) for c in channels}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional["asyncio.Queue[Any]"] = None
        self._listener: Optional["asyncio.Future[None]"] = None
        self._ready: Optional[asyncio.Event] = None
        self._closing = False
        self._listen_exception: Optional[BaseException] = None
        # worker-thread state
        self._initial_topics: Set[str] = set()
        self._subscribed: Set[str] = set()
        self._positions: Dict[Tuple[str, int], int] = {}

    async def __aenter__(self) -> "KafkaSubscriptionContext":
        if self._loop is not None:
            raise RuntimeError("Subscription context cannot be entered twice")
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self._QUEUE_MAXSIZE)
        self._ready = asyncio.Event()

        try:
            for topic in sorted(self._exact_topics):
                await self.backend._ensure_topic(topic, KIND_PUBSUB)
            self._listener = self._loop.run_in_executor(
                _BLOCKING_EXECUTOR, self._listen
            )
            await asyncio.wait_for(self._ready.wait(), self._READY_TIMEOUT)
        except BaseException as exc:
            await self._cleanup()
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
                raise
            raise ConnectionError(f"Failed to subscribe: {exc}") from exc
        if self._listen_exception is not None:
            failure = self._listen_exception
            await self._cleanup()
            raise ConnectionError(f"Failed to subscribe: {failure}") from failure
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._cleanup()

    def _signal_ready(self) -> None:
        if self._loop is not None and self._ready is not None:
            self._loop.call_soon_threadsafe(self._ready.set)

    # worker thread

    def _matching_topics(self, consumer: Any) -> Set[str]:
        suffix = f".{KIND_PUBSUB}"
        metadata = consumer.list_topics(timeout=self.backend.connector.request_timeout)
        matched: Set[str] = set()
        for name, topic_meta in metadata.topics.items():
            if topic_meta.error is not None or not name.endswith(suffix):
                continue
            base = name[: -len(suffix)]
            if any(rx.fullmatch(base) for rx in self._pattern_res):
                matched.add(name)
        return matched

    def _on_assign(self, consumer: Any, partitions: List[TopicPartition]) -> None:
        for tp in partitions:
            key = (tp.topic, tp.partition)
            if key in self._positions:  # rebalance: resume where we left off
                tp.offset = self._positions[key]
            elif tp.topic in self._initial_topics:  # only messages from now on
                _, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                tp.offset = high
            else:  # topic created after we subscribed: all of it is "new"
                tp.offset = OFFSET_BEGINNING
        consumer.assign(partitions)
        self._signal_ready()

    def _subscribe(self, consumer: Any, topics: Set[str]) -> None:
        self._subscribed = set(topics)
        consumer.subscribe(sorted(topics), on_assign=self._on_assign)

    def _listen(self) -> None:
        """Worker-thread loop that forwards messages to the event loop."""
        consumer = None
        try:
            consumer = self.backend.connector.new_consumer(
                {
                    "group.id": f"volnux-ps-{uuid.uuid4().hex}",
                    "auto.offset.reset": "latest",
                    "enable.partition.eof": False,
                }
            )
            interval = float(self.backend._opt("pattern_refresh_interval"))
            if self._pattern_res:
                self._initial_topics = self._matching_topics(consumer)
            else:
                self._initial_topics = set(self._exact_topics)

            if self._initial_topics:
                self._subscribe(consumer, self._initial_topics)
            else:
                self._signal_ready()  # nothing to wait for yet

            next_refresh = time.monotonic() + interval
            while not self._closing:
                if self._pattern_res and time.monotonic() >= next_refresh:
                    next_refresh = time.monotonic() + interval
                    topics = self._matching_topics(consumer)
                    if topics != self._subscribed and topics:
                        self._subscribe(consumer, topics | self._subscribed)
                if not self._subscribed:
                    time.sleep(min(0.25, interval))
                    continue
                for msg in consumer.consume(num_messages=100, timeout=0.5):
                    if msg.error() is not None:
                        error = msg.error()
                        if error.code() == KafkaError._PARTITION_EOF:
                            continue
                        if error.fatal():
                            raise KafkaException(error)
                        logger.warning("Kafka subscription error: %s", error)
                        continue
                    self._positions[(msg.topic(), msg.partition())] = msg.offset() + 1
                    if not self._put(msg):
                        break
        except Exception as exc:
            if self._closing:
                logger.debug("Kafka listen loop terminated: %s", exc)
            else:
                logger.warning("Kafka subscription terminated unexpectedly: %s", exc)
                self._listen_exception = exc
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as exc:
                    logger.debug("Error closing Kafka subscriber: %s", exc)
            self._signal_ready()  # never leave __aenter__ waiting
            self._put_sentinel()

    def _put(self, item: Any) -> bool:
        """Hand ``item`` to the loop, blocking while the queue is full."""
        assert self._loop is not None and self._queue is not None
        try:
            future = asyncio.run_coroutine_threadsafe(self._queue.put(item), self._loop)
        except RuntimeError:  # loop closed
            return False
        while not self._closing:
            try:
                future.result(0.5)
                return True
            except concurrent.futures.TimeoutError:
                continue
            except Exception:
                return False
        future.cancel()
        return False

    def _put_sentinel(self) -> None:
        if self._loop is None or self._queue is None:
            return

        def put() -> None:
            assert self._queue is not None
            try:
                self._queue.put_nowait(self._CLOSE_SENTINEL)
            except asyncio.QueueFull:
                try:  # the termination signal must never be dropped
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._queue.put_nowait(self._CLOSE_SENTINEL)

        try:
            self._loop.call_soon_threadsafe(put)
        except RuntimeError:
            pass

    async def _cleanup(self) -> None:
        self._closing = True
        if self._listener is not None:
            listener, self._listener = self._listener, None
            try:
                await listener  # the worker notices _closing within one poll
            except Exception as exc:
                logger.debug("Error while awaiting Kafka listener: %s", exc)

    def __aiter__(self) -> "KafkaSubscriptionContext":
        return self

    async def __anext__(self) -> Message:
        if self._queue is None:
            raise RuntimeError(
                "Subscription is not active; use 'async with' before iterating"
            )
        msg = await self._queue.get()

        if msg is self._CLOSE_SENTINEL:
            self._put_sentinel()  # keep further iteration from hanging
            if self._listen_exception is not None:
                raise ConnectionError(
                    f"Kafka subscription terminated unexpectedly: {self._listen_exception}"
                ) from self._listen_exception
            raise StopAsyncIteration

        channel_raw = _header(msg, CHANNEL_HEADER)
        channel = (
            _decode(channel_raw)
            if channel_raw is not None
            else msg.topic()[: -len(f".{KIND_PUBSUB}")]
        )
        pattern = next((p for p in self.patterns if fnmatch(channel, p)), None)
        return Message(
            channel=channel,
            pattern=pattern,
            data=self.backend._deserialize(msg.value(), self.record_class),
            raw=msg.value(),
        )


# Push/Pop queue


class KafkaStorePushPopMixin(KafkaMessagingMixin, PushPopCapabilityMixin):
    """FIFO work queue on ``<key>.q`` topics, shared consumer group per queue."""

    _UNSUPPORTED_SIDE = (
        "Kafka queues are append-only logs: only push(side=RIGHT) and "
        "pop(side=LEFT) (FIFO) are supported"
    )

    @staticmethod
    def _queue_group(topic: str) -> str:
        return f"volnux-queue-{topic}"

    async def _queue_consumer(self, key: str) -> Tuple[str, _GroupConsumer]:
        topic = topic_name(key, KIND_QUEUE)
        await self._ensure_topic(topic, KIND_QUEUE)
        gc = await self._run(
            self._group_consumer_sync,
            topic,
            self._queue_group(topic),
            str(self._opt("default_consumer_id")),
        )
        return topic, gc

    async def push(
        self, key: str, *values: Record, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        if not values:
            raise ValueError("push() requires at least one value")
        if side != QueueSide.RIGHT:
            raise NotImplementedError(self._UNSUPPORTED_SIDE)
        payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-defined]
        topic = topic_name(key, KIND_QUEUE)
        with _kafka_errors(f"Failed to push to {key}"):
            await self._ensure_topic(topic, KIND_QUEUE)
            await self._run(
                self._produce_sync, topic, [(None, p, None) for p in payloads]
            )
            return await self.queue_length(key)

    async def _take(
        self, key: str, limit: Optional[int], timeout: Optional[float]
    ) -> List[bytes]:
        """Wait for the first item, drain up to ``limit``, commit; return payloads."""
        _, gc = await self._queue_consumer(key)
        loop = asyncio.get_running_loop()
        semaphore = self._get_blocking_semaphore()
        cancelled = threading.Event()
        join_timeout = float(self._opt("group_join_timeout"))

        def work() -> List[bytes]:
            first = gc.poll_batch(1, timeout, cancelled, join_timeout)
            if not first:
                return []
            extra = (
                gc.drain(None if limit is None else limit - 1, cancelled)
                if limit != 1
                else []
            )
            messages = first + extra
            if cancelled.is_set():
                gc.rewind(messages)
                return []
            entries = gc.register(messages, "queue")
            gc.ack([entry_id for entry_id, _ in entries])
            return [msg.value() for _, msg in entries]

        await semaphore.acquire()
        try:
            future = loop.run_in_executor(_BLOCKING_EXECUTOR, work)
        except BaseException:
            semaphore.release()
            raise
        future.add_done_callback(lambda _f: semaphore.release())
        try:
            return await asyncio.shield(future)  # type: ignore[no-any-return]
        except asyncio.CancelledError:
            cancelled.set()
            future.add_done_callback(functools.partial(self._requeue_orphan, key))
            raise

    def _requeue_orphan(self, key: str, future: "asyncio.Future[Any]") -> None:
        """Items committed for a caller that left are produced again (no loss)."""
        if future.cancelled() or future.exception() is not None:
            return
        payloads = future.result()
        if not payloads:
            return
        topic = topic_name(key, KIND_QUEUE)
        task = asyncio.ensure_future(
            self._run(self._produce_sync, topic, [(None, p, None) for p in payloads])
        )
        _PENDING_REPAIRS.add(task)
        task.add_done_callback(_PENDING_REPAIRS.discard)
        logger.info(
            "Re-queued %d item(s) popped for a cancelled caller on '%s'",
            len(payloads),
            key,
        )

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[Union[float, int]] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Record]:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be None or >= 0")
        if side != QueueSide.LEFT:
            raise NotImplementedError(self._UNSUPPORTED_SIDE)
        with _kafka_errors(f"Failed to pop from {key}"):
            payloads = await self._take(
                key, 1, None if timeout is None else float(timeout)
            )
        return self._deserialize(payloads[0], record_class) if payloads else None

    async def pop_many(
        self,
        key: str,
        record_class: Type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Record]:
        if limit is not None and limit <= 0:
            return []
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be None or >= 0")
        if side != QueueSide.LEFT:
            raise NotImplementedError(self._UNSUPPORTED_SIDE)
        with _kafka_errors(f"Failed to pop from {key}"):
            payloads = await self._take(
                key, limit, None if timeout is None else float(timeout)
            )
        return [self._deserialize(p, record_class) for p in payloads]

    async def queue_delete(self, key: str) -> bool:
        topic = topic_name(key, KIND_QUEUE)
        with _kafka_errors(f"Failed to delete queue {key}"):
            return await self._run(self._delete_topic_sync, topic)  # type: ignore[no-any-return]

    async def queue_length(self, key: str) -> int:
        with _kafka_errors(f"Failed to get queue length for {key}"):
            topic, gc = await self._queue_consumer(key)
            return await self._run(gc.lag, self.connector.request_timeout)  # type: ignore[no-any-return]

    async def queue_range(
        self, key: str, record_class: Type[Record], start: int = 0, stop: int = -1
    ) -> List[Record]:
        """Peek a slice of the queue (``stop`` inclusive) without consuming it.

        Reads from the group's committed position with a scratch consumer.
        Across several partitions the order is by message timestamp.
        """
        with _kafka_errors(f"Failed to get queue range for {key}"):
            topic, _ = await self._queue_consumer(key)
            payloads = await self._run(self._peek_sync, topic, start, stop)
        return [self._deserialize(p, record_class) for p in payloads]

    def _peek_sync(self, topic: str, start: int, stop: int) -> List[bytes]:
        """Read the unconsumed part of the queue without touching the group.

        One consumer that is *assigned* (never subscribed) and never commits is
        enough: ``committed()`` tells where the shared group stands and the same
        consumer reads from there.
        """
        group = self._queue_group(topic)
        consumer = self.connector.new_consumer({"group.id": group})
        try:
            timeout = self.connector.request_timeout
            topic_meta = consumer.list_topics(topic, timeout=timeout).topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                return []
            partitions = [TopicPartition(topic, p) for p in topic_meta.partitions]
            committed = {
                tp.partition: tp.offset
                for tp in consumer.committed(partitions, timeout=timeout)
            }

            assignments: List[TopicPartition] = []
            remaining: Dict[int, int] = {}
            for tp in partitions:
                low, high = consumer.get_watermark_offsets(
                    tp, timeout=timeout, cached=False
                )
                begin = max(committed.get(tp.partition, -1), low)
                assignments.append(TopicPartition(topic, tp.partition, begin))
                remaining[tp.partition] = max(0, high - begin)
            if not any(remaining.values()):
                return []
            consumer.assign(assignments)

            # A head slice needs at most stop+1 messages per partition.
            cap = None if (start < 0 or stop < 0) else (stop + 1) * len(partitions)
            collected: List[KafkaMessage] = []
            idle = 0
            while any(n > 0 for n in remaining.values()) and idle < 3:
                if cap is not None and len(collected) >= cap:
                    break
                good = [
                    m
                    for m in consumer.consume(num_messages=500, timeout=0.5)
                    if m.error() is None
                ]
                idle = 0 if good else idle + 1
                for msg in good:
                    collected.append(msg)
                    remaining[msg.partition()] -= 1
        finally:
            consumer.close()

        if len(partitions) > 1:  # one partition is already in queue order
            collected.sort(key=lambda m: (m.timestamp()[1], m.partition(), m.offset()))
        return _lrange([m.value() for m in collected], start, stop)


# Streams


class KafkaStreamCapabilityMixin(KafkaMessagingMixin, StreamCapabilityMixin):
    """Append-only log with consumer groups on ``<key>.log`` topics."""

    def _stream_topic(self, stream_key: str) -> str:
        return topic_name(stream_key, KIND_STREAM)

    @staticmethod
    def _stream_group(topic: str, group_name: str) -> str:
        """Kafka group id for a Volnux stream group.

        A Kafka group spans every topic its members subscribe to, but a Volnux
        group belongs to one stream (as with Redis ``XGROUP``): the same name on
        two streams must be two independent groups with independent positions.
        """
        return f"{group_name}@{topic}"

    async def stream_append(
        self,
        stream_key: str,
        record: Any,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        payload = self._serialize_record(record)  # type: ignore[attr-defined]
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to append to stream '{stream_key}'"):
            await self._ensure_topic(topic, KIND_STREAM)
            ((partition, offset),) = await self._run(
                self._produce_sync, topic, [(None, payload, None)]
            )
            if max_len is not None:
                await self._maybe_trim(topic, max_len, approximate_trim)
        return f"{partition}-{offset}"

    async def _maybe_trim(self, topic: str, max_len: int, approximate: bool) -> None:
        """Trim after an append. Approximate mode batches the work (like ``MAXLEN ~``)."""
        state = self._kafka
        if approximate:
            count = state.append_counts.get(topic, 0) + 1
            if count < max(100, max_len // 10):
                state.append_counts[topic] = count
                return
        state.append_counts[topic] = 0
        await self._run(self._trim_sync, topic, max_len)

    def _trim_sync(self, topic: str, max_len: int) -> int:
        marks = self._watermarks(topic)
        targets, removed = [], 0
        for partition, (low, high) in marks.items():
            new_low = max(low, high - max_len)
            if new_low > low:
                targets.append(TopicPartition(topic, partition, new_low))
                removed += new_low - low
        if targets:
            for future in self.connector.admin.delete_records(
                targets, request_timeout=self.connector.request_timeout
            ).values():
                future.result()
        return removed

    async def stream_ensure_consumer_group(
        self,
        stream_key: str,
        group_name: str,
        start_from: Union[StreamOffset, str] = StreamOffset.BEGINNING,
    ) -> None:
        topic = self._stream_topic(stream_key)
        with _kafka_errors(
            f"Failed to ensure consumer group '{group_name}' on '{stream_key}'"
        ):
            await self._ensure_topic(topic, KIND_STREAM)
            await self._run(self._ensure_group_sync, topic, group_name, start_from)

    def _ensure_group_sync(
        self, topic: str, group_name: str, start_from: Union[StreamOffset, str]
    ) -> None:
        """Pin the group's start position. Idempotent: an existing group is left alone."""
        consumer = self.connector.new_consumer(
            {"group.id": self._stream_group(topic, group_name)}
        )
        try:
            timeout = self.connector.request_timeout
            marks = self._watermarks(topic)
            partitions = [TopicPartition(topic, p) for p in marks]
            committed = {
                tp.partition: tp.offset
                for tp in consumer.committed(partitions, timeout=timeout)
            }
            to_commit: List[TopicPartition] = []
            for partition, (low, high) in marks.items():
                if committed.get(partition, -1) >= 0:
                    continue  # group already has a position
                if start_from == StreamOffset.BEGINNING:
                    offset = low
                elif start_from == StreamOffset.LATEST:
                    offset = high
                else:
                    start_partition, start_offset = self._parse_start(str(start_from))
                    # Redis semantics: deliver entries *after* the given id
                    offset = start_offset + 1 if partition == start_partition else low
                to_commit.append(TopicPartition(topic, partition, max(low, offset)))
            if to_commit:
                try:
                    consumer.commit(offsets=to_commit, asynchronous=False)
                except KafkaException as exc:
                    # Kafka only accepts out-of-band commits for an *empty* group.
                    # A group with live members already exists, which is exactly
                    # what "ensure" wants (like Redis' BUSYGROUP).
                    if exc.args[0].code() not in _GROUP_ALREADY_ACTIVE:
                        raise
                    logger.debug(
                        "Group '%s' on '%s' is active; keeping its position",
                        group_name,
                        topic,
                    )
        finally:
            consumer.close()

    @staticmethod
    def _parse_start(value: str) -> Tuple[int, int]:
        if "-" in value:
            return _split_entry_id(value)
        return 0, int(value)  # a bare offset addresses partition 0

    async def stream_read(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[Record]]:
        consumer = consumer_id or str(self._opt("default_consumer_id"))
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to read from stream '{stream_key}'"):
            await self._ensure_topic(topic, KIND_STREAM)
            gc = await self._run(
                self._group_consumer_sync,
                topic,
                self._stream_group(topic, group_name),
                consumer,
            )
            timeout = None if block_ms is None else block_ms / 1000.0
            messages = await self._poll(gc, count, timeout)
            entries = gc.register(messages, consumer) if messages else []
        return self._build_entries(
            [(entry_id, msg.value()) for entry_id, msg in entries],
            group_name,
            consumer,
            record_class,
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
        """Reassign entries that were delivered here but never acknowledged.

        Entries held by workers in *other processes* need no claiming: when such
        a worker dies, Kafka's group rebalance gives its partitions to a live
        member, which re-reads from the last committed offset.
        """
        topic = self._stream_topic(stream_key)
        claimed: List[Tuple[str, bytes]] = []
        for gc in self._consumers_for(topic, self._stream_group(topic, group_name)):
            if len(claimed) >= count:
                break
            claimed.extend(
                gc.claim_idle(consumer_id, min_idle_ms, count - len(claimed))
            )
        claimed.sort(key=lambda item: _split_entry_id(item[0]))
        return self._build_entries(claimed, group_name, consumer_id, record_class)

    async def stream_ack(
        self, stream_key: str, group_name: str, *entry_ids: str
    ) -> int:
        if not entry_ids:
            return 0
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to ACK entries on '{stream_key}'"):
            total = 0
            for gc in self._consumers_for(topic, self._stream_group(topic, group_name)):
                owned = [eid for eid in entry_ids if gc.has_pending(eid)]
                if owned:
                    total += await self._run(gc.ack, owned)
            return total

    async def stream_length(self, stream_key: str) -> int:
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to get length of '{stream_key}'"):
            marks = await self._run(self._watermarks, topic)
        return sum(high - low for low, high in marks.values())

    async def stream_trim(
        self, stream_key: str, max_len: int, approximate: bool = True
    ) -> int:
        """Drop the oldest entries so each partition keeps at most ``max_len``."""
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to trim '{stream_key}'"):
            return await self._run(self._trim_sync, topic, max_len)  # type: ignore[no-any-return]

    async def stream_delete(self, stream_key: str) -> bool:
        topic = self._stream_topic(stream_key)
        with _kafka_errors(f"Failed to delete stream '{stream_key}'"):
            return await self._run(self._delete_topic_sync, topic)  # type: ignore[no-any-return]

    def _build_entries(
        self,
        entries: Sequence[Tuple[str, Optional[bytes]]],
        group_name: str,
        consumer_id: Optional[str],
        record_class: Type[Any],
    ) -> List[StreamEntry]:
        results: List[StreamEntry] = []
        for entry_id, raw in entries:
            if raw is None:
                logger.warning("Stream entry %s has no value, skipping", entry_id)
                continue
            results.append(
                StreamEntry(
                    entry_id=entry_id,
                    record=self._deserialize(raw, record_class),
                    group_name=group_name,
                    consumer_id=consumer_id,
                )
            )
        return results
