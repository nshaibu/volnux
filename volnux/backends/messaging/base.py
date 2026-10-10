import logging
from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncIterator,
    List,
    Optional,
    TYPE_CHECKING,
    TypeVar,
    Type,
    Generic,
)
from enum import Enum
from dataclasses import dataclass, field

from volnux.backends.connection import BackendConnectorBase

if TYPE_CHECKING:
    from volnux.mixins.messaging import MessagingBackendIntegrationMixin

logger = logging.getLogger(__name__)


Record = TypeVar("Record", bound="MessagingBackendIntegrationMixin")


class QueueSide(str, Enum):
    """Which end of the list to push to or pop from."""

    LEFT = "left"  # Head of the list (LPUSH / LPOP)
    RIGHT = "right"  # Tail of the list (RPUSH / RPOP)


@dataclass
class Message:
    """
    A message received from a pub/sub channel.

    channel : The channel on which the message arrived.
              For pattern subscriptions, this is the specific channel
              that matched, not the pattern itself.
    pattern : The glob pattern that matched, or None for exact subscriptions.
    data    : The deserialised message payload.
    raw     : The original bytes/string from the transport, before deserialisation.
    """

    channel: str
    data: Any
    pattern: Optional[str] = None
    raw: Optional[Any] = field(default=None, repr=False)


class PubSubCapabilityMixin:

    connector: BackendConnectorBase[Any]

    @abstractmethod
    async def publish(self, channel: str, record: Record) -> int:
        """
        Publish a message to the channel.

        Parameters
        ----------
        channel:
            Target channel name.
        record:
           The record to publish to the channel

        Returns
        -------
        int
            Number of subscribers that received the message.
            0 means no subscribers were listening — the message was dropped.
        """

    @abstractmethod
    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "AsyncSubscriptionContext":
        """
        Subscribe to one or more exact channel names.

        Usage:
            async with backend.subscribe("volnux:task:cmd:abc", EventClass) as messages:
                async for message in messages:
                    handle(message)

        Parameters
        ----------
        channels:
            Channel names to subscribe to.
        record_class:
            Record class for deserialisation.

        Returns
        -------
        AsyncSubscriptionContext
            Async context manager. Enter it to open a dedicated subscriber
            connection. Exit to cleanly unsubscribe and close the connection.
        """

    @abstractmethod
    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "AsyncSubscriptionContext":
        """
        Subscribe using glob patterns.

        Patterns follow the Redis glob syntax:
            *        matches any sequence of characters
            ?        matches any single character
            [abc] matches any character in the set

        Example patterns:
            "volnux:task:cmd:*" — all task command channels
            "volnux:hitl:*" — all HITL channels

        Usage:
            async with backend.psubscribe("volnux:task:cmd:*", EventClass) as messages:
                async for message in messages:
                    handle(message)
        """

    async def get_subscribed_channels(self) -> List[str]:
        """
        Return the list of channels this backend instance is currently
        subscribed to (exact subscriptions only, not patterns).
        """
        return []


class PushPopCapabilityMixin:
    """
    Abstract push/pop queue interface.

    The underlying data structure is an ordered list where:
        - push(side=RIGHT) + pop(side=LEFT) = FIFO queue
        - push(side=LEFT) + pop(side=LEFT) = LIFO stack
        - push(side=RIGHT) + pop(side=RIGHT) = LIFO stack (other end)

    All pop operations support a blocking timeout. When timeout=0, pop
    blocks indefinitely. When timeout=None, pop is non-blocking and
    returns None immediately if the queue is empty.
    """

    connector: BackendConnectorBase[Any]

    @abstractmethod
    async def push(
        self,
        key: str,
        *values: Record,
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Push one or more values onto a queue.

        Parameters
        ----------
        key:
            Queue identifier.
        *values:
            One or more values to push. Pushed atomically in order.
            Dicts and lists are JSON-serialised.
        side:
            Which end of the list to push to.
            RIGHT (default) = tail = RPUSH = FIFO enqueue end.
            LEFT = head = LPUSH = stack push end.

        Returns
        -------
        int
            Length of the queue after the push.
        """

    @abstractmethod
    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Record]:
        """
        Pop a single value from a queue.

        Parameters
        ----------
        key:
            Queue identifier.
        record_class:
            Record class for deserialisation.
        timeout:
            Seconds to wait for a value to become available.
            None  — non-blocking; returns None immediately if the queue is empty.
            0 — block indefinitely until a value is available.
            > 0 — block for at most this many seconds.
        side:
            Which end to pop from.
            LEFT (default) = head = LPOP = FIFO dequeue end.
            RIGHT = tail = RPOP.

        Returns
        -------
        Record | None
            The deserialised value, or None if the queue was empty
            (non-blocking) or the timeout expired.
        """

    @abstractmethod
    async def pop_many(
        self,
        key: str,
        record_class: Type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Record]:
        """
        Block until the queue has at least one value, then pop a batch of items.

        Parameters
        ----------
        key:
            The single queue key to watch.
        record_class:
            Record class for deserialisation.
        limit:
            Maximum number of items to pop. If None, pops all currently
            available items in the queue.
        timeout:
            Seconds to wait for the FIRST item to arrive.
            None = non-blocking (returns [] immediately if empty).
            0 = block indefinitely.
            >0 = block for max N seconds.
        side:
            Which end to pop from.

        Returns
        -------
        List[Record]
            A list of deserialised records. Returns an empty list if the
            timeout expired before any items arrived.
        """

    @abstractmethod
    async def queue_length(self, key: str) -> int:
        """Return the number of items currently in the queue."""

    @abstractmethod
    async def queue_range(
        self,
        key: str,
        record_class: Type[Record],
        start: int = 0,
        stop: int = -1,
    ) -> List[Record]:
        """
        Return a slice of the queue without removing items.

        Parameters
        ----------
        key:
            Queue identifier
        record_class:
            Record class for deserialisation.
        start:
            Zero-based start index. Negative values count from the tail.
        stop:
            Inclusive end index. -1 means the last element.

        Returns
        -------
        List[Record]
            Deserialised elements from start to stop, inclusive.
        """

    # Convenience methods built on the abstract primitives
    # These have default implementations. Backends may override them
    # for performance (e.g. Redis has atomic GETSET for some of these).

    async def enqueue(self, key: str, value: Any) -> int:
        """
        FIFO enqueue — push to the right (tail).
        Equivalent to push(key, value, side=RIGHT).
        """
        return await self.push(key, value, side=QueueSide.RIGHT)

    async def dequeue(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """
        FIFO dequeue — pop from the left (head).
        Equivalent to pop(key, timeout, side=LEFT).
        """
        return await self.pop(
            key, record_class=record_class, timeout=timeout, side=QueueSide.LEFT
        )

    async def queue_peek(self, key: str, record_class: Type[Record]) -> Optional[Any]:
        """
        Return the head item without removing it.
        Returns None if the queue is empty.
        """
        items = await self.queue_range(key, record_class=record_class, start=0, stop=0)
        return items[0] if items else None


class AsyncSubscriptionContext(ABC):
    """
    Async context manager returned by subscribe() and psubscribe().

    Usage:
        async with backend.subscribe("channel") as messages:
            async for msg in messages:
                print(msg.data)

    The context manager owns a dedicated subscriber connection for the
    duration of the with-block. On __aexit__ it unsubscribes cleanly and
    closes the connection.
    """

    @abstractmethod
    async def __aenter__(self) -> AsyncIterator[Message]: ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...


@dataclass
class StreamEntry(Generic[Record]):
    """A single immutable record retrieved from an append-only log stream.

    Attributes:
        entry_id: Backend-assigned monotonically increasing identifier
                 (e.g., Redis Stream ID '1700000000000-0'). Used for ACKs.
        record: Deserialized domain record payload.
        group_name: The consumer group namespace that delivered this entry.
        consumer_id: Specific worker instance identifier that received the entry.
    """

    entry_id: str
    record: Record
    group_name: Optional[str] = None
    consumer_id: Optional[str] = None


class StreamOffset(str, Enum):
    BEGINNING = "__VOLNUX_BEGIN__"  # Replay from inception
    LATEST = "__VOLNUX_LATEST__"  # New entries only


class StreamCapabilityMixin:
    """Abstract append-only log capability interface.

    Key Properties
    --------------
    1. Append-Only Persistence: Producers append immutably. Reads do not mutate log state.
    2. Independent Offset Namespaces: Multiple consumer groups read from the same stream
       without interfering with each other's cursor positions.
    3. Replayability & Catch-Up: Disconnected or late-joining consumers can replay
       historical entries starting from any valid entry ID or "0".
    4. At-Least-Once Delivery: Unacknowledged (pending) entries remain in the delivery
       pipeline and are re-delivered upon worker recovery.
    """

    @abstractmethod
    async def stream_append(
        self,
        stream_key: str,
        record: Record,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        """Appends a record to the specified log stream.

        Args:
            stream_key: Target stream topic or log identifier.
            record: Data payload to serialize and append.
            max_len: Optional maximum entries allowed in stream. Trims oldest entries.
            approximate_trim: If True, allows storage engine performance optimizations
                             during trimming (e.g., Redis `MAXLEN ~`).

        Returns:
            str: Monotonically increasing entry ID assigned by the backend.
        """

    async def stream_ensure_consumer_group(
        self,
        stream_key: str,
        group_name: str,
        start_from: str = StreamOffset.BEGINNING,  # Default to full replay
    ) -> None:
        """
        start_from:
            - StreamOffset.BEGINNING: Replay all historical entries
            - StreamOffset.LATEST: Read only new entries after group creation
            - "<backend_native_id>": Resume from specific entry ID
        """

    @abstractmethod
    async def stream_read(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[Record]]:
        """Reads undelivered or pending entries for a consumer group.

        Args:
            stream_key: Target stream identifier.
            group_name: Consumer group tracking offset namespace.
            record_class: Model class used to deserialize record payloads.
            consumer_id: Unique worker instance name within the group.
            count: Maximum batch size to retrieve in a single call.
            block_ms: Non-blocking behavior control:
                      - None: Return immediately with available records.
                      - 0: Block indefinitely until new records arrive.
                      - >0: Wait up to N milliseconds for records.

        Returns:
            List[StreamEntry[Record]]: Ordered list of delivered entries.
        """

    @abstractmethod
    async def stream_ack(
        self,
        stream_key: str,
        group_name: str,
        *entry_ids: str,
    ) -> int:
        """Acknowledges processed entries, advancing the consumer group offset.

        Unacknowledged entries remain pending and will be re-delivered on
        subsequent `stream_read` calls (At-Least-Once processing).

        Args:
            stream_key: Target stream identifier.
            group_name: Consumer group namespace acknowledging execution.
            *entry_ids: One or more entry IDs to confirm.

        Returns:
            int: Total number of entries successfully acknowledged.
        """

    @abstractmethod
    async def stream_claim_pending(
        self,
        stream_key: str,
        group_name: str,
        consumer_id: str,
        record_class: Type[Record],
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> List[StreamEntry[Record]]:
        """Claims pending entries that have been idle for at least `min_idle_ms`.

        Used for worker crash recovery. When a new worker joins a consumer group,
        it should call this BEFORE `stream_read` to reclaim orphaned work from
        dead workers.

        Args:
            stream_key: Target stream identifier.
            group_name: Consumer group namespace.
            consumer_id: The NEW worker claiming ownership of orphaned entries.
            record_class: Record type to deserialize stream entries.
            min_idle_ms: Minimum idle time before an entry is eligible for claiming.
                         Prevents stealing actively-processed entries from live workers.
            count: Maximum entries to claim per call.

        Returns:
            List[StreamEntry[Record]]: Claimed entries now owned by `consumer_id`.
        """

    @abstractmethod
    async def stream_length(self, stream_key: str) -> int:
        """Returns the total number of entries currently stored in the stream."""

    @abstractmethod
    async def stream_trim(
        self,
        stream_key: str,
        max_len: int,
        approximate: bool = True,
    ) -> int:
        """Manually trims the stream log to at most `max_len` entries.

        Removes the oldest entries first. Note that trimming entries before
        consumers read them will result in skipped records for slow consumers.

        Returns:
            int: Total number of evicted entries.
        """

    @abstractmethod
    async def stream_delete(self, stream_key: str) -> bool:
        """Deletes the log stream and purges all associated consumer group states.

        Returns:
            bool: True if the stream existed and was destroyed.
        """

    async def stream_read_and_ack(
        self,
        stream_key: str,
        group_name: str,
        record_class: Type[Record],
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[Record]:
        """Reads and automatically acknowledges entries in a single call.

        WARNING: Implements At-Most-Once delivery. If the caller crashes during
        downstream processing after this call returns, the entries are already
        acknowledged and cannot be re-delivered.

        Use `stream_read` and `stream_ack` separately for strict At-Least-Once processing.
        """
        entries = await self.stream_read(
            stream_key=stream_key,
            group_name=group_name,
            record_class=record_class,
            consumer_id=consumer_id,
            count=count,
            block_ms=block_ms,
        )
        if entries:
            await self.stream_ack(
                stream_key,
                group_name,
                *[e.entry_id for e in entries],
            )
        return [e.record for e in entries]
