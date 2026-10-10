import logging
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Optional,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

from .connection import BackendConnectionIntegrationMixin
from volnux.concurrency.async_utils import as_coroutine
from volnux.backends.messaging.base import QueueSide, StreamOffset, StreamEntry
from volnux.backends.messaging.decorators import (
    ensure_pubsub,
    ensure_pushpop,
    ensure_streaming,
)

if TYPE_CHECKING:
    from volnux.backends.messaging.base import (
        PubSubCapabilityMixin,
        PushPopCapabilityMixin,
        StreamCapabilityMixin,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="MessagingBackendIntegrationMixin")

B = TypeVar(
    "B", "PushPopCapabilityMixin", "StreamCapabilityMixin", "PubSubCapabilityMixin"
)


class MessagingBackendIntegrationMixin(BackendConnectionIntegrationMixin):
    """
    Mixin providing messaging and queue backend integration for implementations that support Pub/Sub
    and push/pop queue operations.

    Detailed description of the class, its purpose, and usage.

    This mixin provides a unified interface for interacting with backend systems that support
    messaging (Pub/Sub) and queuing functionality. It enables publishing messages, subscribing to
    message channels, as well as enqueuing and dequeuing tasks in a queue model. It ensures backends
    comply with the required capabilities before invoking operations. Typically, this mixin is
    intended for classes utilizing specialized storage backends.
    """

    @classmethod
    @ensure_pubsub
    async def publish(cls, backend: B, record: T) -> int:
        """
        Publishes a given record to the appropriate channel using the backend system.

        :param backend:
        :param record: The record to be published.
        :type record: T
        :return: The number of subscribers that received the published record.
        :rtype: int
        """
        return await backend.publish(
            channel=await cls.get_schema_name(),
            record=record,
        )

    @classmethod
    @ensure_pubsub
    async def subscribe(
        cls: Type[T],
        backend: B,
        callback: Callable[[T], Awaitable[None]],
    ) -> None:
        """
        Subscribes to a publish-subscribe channel for receiving updates about a specific schema.

        This asynchronous method allows listeners to subscribe to a specific schema channel. When
        a new record is published to the channel, the provided callback is triggered with the record
        as an argument.

        :param backend:
        :param callback: A callable that accepts an instance of the class as input and returns
                         an awaitable object. It will be invoked for each record received on
                         the channel.
        :type callback: Callable[[T], Awaitable[None]]
        """
        async with backend.subscribe(
            await cls.get_schema_name(), record_class=cls
        ) as pub:
            async for record in pub:
                await as_coroutine(callback, record)

    @classmethod
    @ensure_pubsub
    async def psubscribe(
        cls, backend: B, *patterns: str, callback: Callable[[T], Awaitable[None]]
    ) -> None:
        """
        Subscribes to a set of patterns on the backend and listens for matching messages in a
        publish-subscribe model. This method enables clients to receive notifications when
        messages are published to channels that match specified patterns. The callback function
        is invoked with each matching record.

        :param backend:
        :param patterns: A variable number of string arguments representing the patterns to
            subscribe to. These patterns should adhere to the backend's supported pattern
            syntax.
        :param callback: An asynchronous callable that takes a single argument. The argument
            passed to the callback is an instance of the record matching the subscribed patterns.
        :return: This method does not return any value.
        """
        pattern_set = {await cls.get_schema_name(), *patterns}
        async with backend.psubscribe(*pattern_set, record_class=cls) as pub:
            async for record in pub:
                await as_coroutine(callback, record)

    @classmethod
    @ensure_pushpop
    async def push(
        cls,
        backend: B,
        *instances: Any,
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Pushes multiple instances to the queue on the specified side. This method is
        asynchronous and interacts with a backend to perform the operation.

        :param backend:
        :param instances: One or more instances to be added to the queue.
        :type instances: Any
        :param side: The side of the queue where the instances should be pushed.
                     Possible values are defined in the QueueSide enumeration.
        :return: The total number of instances in the queue after the operation is
                 complete.
        :rtype: int
        """
        return await backend.push(
            await cls.get_schema_name(),
            *instances,
            side=side,
        )

    @classmethod
    @ensure_pushpop
    async def pop(
        cls: Type[T],
        backend: B,
        *,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[T]:
        """
        Asynchronously removes and returns an item from the queue. The side from which
        the item is removed can be specified (left or right). If the timeout is
        provided and no item is available within the specified time, the operation
        will return `None`.

        :param backend:
        :param timeout: Optional; the maximum time in seconds to wait for an item to
                        become available in the queue before returning `None`.
                        If not provided, it waits indefinitely.
        :type timeout: Optional[float]
        :param side: The side of the queue from which the item is to be removed.
                     Defaults to `QueueSide.LEFT` (left side of the queue).
        :type side: QueueSide
        :return: The item removed from the queue, or `None` if the timeout expires
                 without any available item.
        :rtype: Optional[T]
        """
        return await backend.pop(
            await cls.get_schema_name(),
            record_class=cls,
            timeout=timeout,
            side=side,
        )

    @classmethod
    @ensure_pushpop
    async def pop_many(
        cls: Type[T],
        backend: B,
        *,
        limit: int,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[T]:
        """
        Retrieve and remove multiple items from the queue with specified conditions.

        This method asynchronously fetches a specified number of items from the queue,
        removing them as a batch. The operation can optionally block until the desired
        criteria are met or the timeout expires, depending on the implementation.

        Capability compliance is enforced declaratively using method decorators:
        - @ensure_pubsub: Ephemeral broadcast messaging
        - @ensure_pushpop: Destructive FIFO queue operations
        - @ensure_stream: Persistent, replayable, append-only log operations

        :param backend:
        :param limit: The maximum number of items to retrieve and remove from the queue.
        :param timeout: The optional maximum time, in seconds, to wait before giving
            up the operation. If not provided, the method will default to implementation-specific
            behavior of immediate return or blocking indefinitely.
        :param side: Specifies the side of the queue from which items should be retrieved
            and removed. Defaults to ``QueueSide.LEFT`` if not provided.
        :return: A list containing the items retrieved from the queue, in pop order.
            Empty if the timeout expired before any item arrived.
        """
        return await backend.pop_many(
            await cls.get_schema_name(),
            record_class=cls,
            limit=limit,
            timeout=timeout,
            side=side,
        )

    @classmethod
    @ensure_pushpop
    async def enqueue(cls, backend: B, instance: Any) -> int:
        """
        Asynchronously enqueues an instance into the backend system. This method ensures that the backend
        supports the necessary push and pop operations before proceeding.

        :param backend:
        :param instance: The instance to be enqueued.
        :return: An integer representing the result of the enqueue operation.
        """
        return await backend.enqueue(await cls.get_schema_name(), instance)

    @classmethod
    @ensure_pushpop
    async def dequeue(
        cls: Type[T],
        backend: B,
        timeout: Optional[float] = None,
    ) -> Optional[T]:
        """
        Dequeues an item from the queue with an optional timeout. This method will wait
        for an item to become available until the timeout duration has been reached. If
        the timeout is not provided or set to None, the method will not wait.

        :param backend:
        :param timeout: Optional; The maximum time in seconds to wait for an item to
            become available in the queue. If None, waits indefinitely.
        :type timeout: Optional[float]
        :return: The dequeued item of type T if an item is available, otherwise None.
        :rtype: Optional[T]
        """
        return await backend.dequeue(
            await cls.get_schema_name(),
            record_class=cls,
            timeout=timeout,
        )

    @classmethod
    @ensure_pushpop
    async def queue_length(cls, backend: B) -> int:
        """Return the number of items currently in the queue for this model identity."""
        return await backend.queue_length(await cls.get_schema_name())

    @classmethod
    @ensure_pushpop
    async def queue_range(
        cls: Type[T],
        backend: B,
        start: int = 0,
        stop: int = -1,
    ) -> List[T]:
        """Return a slice of the queue with items deserialized into model instances."""
        return await backend.queue_range(
            await cls.get_schema_name(),
            record_class=cls,
            start=start,
            stop=stop,
        )

    @classmethod
    async def queue_peek(cls: Type[T]) -> Optional[T]:
        """Return the head item as a model instance without removing it."""
        items = await cls.queue_range(start=0, stop=0)
        return items[0] if items else None

    @classmethod
    @ensure_streaming
    async def stream_append(
        cls,
        backend: B,
        record: Any,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
    ) -> str:
        """Appends a record to the model's append-only stream log."""
        return await backend.stream_append(
            stream_key=await cls.get_schema_name(),
            record=record,
            max_len=max_len,
            approximate_trim=approximate_trim,
        )

    @classmethod
    @ensure_streaming
    async def stream_ensure_consumer_group(
        cls,
        backend: B,
        group_name: str,
        start_from: StreamOffset = StreamOffset.BEGINNING,
    ) -> None:
        """Ensures a consumer group namespace exists for this stream log."""
        await backend.stream_ensure_consumer_group(
            stream_key=await cls.get_schema_name(),
            group_name=group_name,
            start_from=start_from,
        )

    @classmethod
    @ensure_streaming
    async def stream_read(
        cls: Type[T],
        backend: B,
        group_name: str,
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[StreamEntry[T]]:
        """Reads undelivered or pending entries from the stream for a consumer group."""
        return await backend.stream_read(
            stream_key=await cls.get_schema_name(),
            group_name=group_name,
            record_class=cls,
            consumer_id=consumer_id,
            count=count,
            block_ms=block_ms,
        )

    @classmethod
    @ensure_streaming
    async def stream_ack(
        cls,
        backend: B,
        group_name: str,
        *entry_ids: str,
    ) -> int:
        """Acknowledges processed entries, advancing the consumer group offset."""
        return await backend.stream_ack(
            await cls.get_schema_name(),
            group_name,
            *entry_ids,
        )

    @classmethod
    @ensure_streaming
    async def stream_claim_pending(
        cls: Type[T],
        backend: B,
        group_name: str,
        consumer_id: str,
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> List[StreamEntry[T]]:
        """Claims orphaned pending stream entries from crashed workers."""
        return await backend.stream_claim_pending(
            stream_key=await cls.get_schema_name(),
            group_name=group_name,
            consumer_id=consumer_id,
            record_class=cls,
            min_idle_ms=min_idle_ms,
            count=count,
        )

    @classmethod
    @ensure_streaming
    async def stream_length(cls, backend: B) -> int:
        """Returns the total number of entries currently stored in the stream log."""
        return await backend.stream_length(await cls.get_schema_name())

    @classmethod
    @ensure_streaming
    async def stream_trim(
        cls,
        backend: B,
        max_len: int,
        approximate: bool = True,
    ) -> int:
        """Trims the stream log to at most `max_len` entries."""
        return await backend.stream_trim(
            stream_key=await cls.get_schema_name(),
            max_len=max_len,
            approximate=approximate,
        )

    @classmethod
    @ensure_streaming
    async def stream_delete(cls, backend: B) -> bool:
        """Deletes the entire stream log and purges associated consumer group states."""
        return await backend.stream_delete(await cls.get_schema_name())

    @classmethod
    @ensure_streaming
    async def stream_read_and_ack(
        cls: Type[T],
        backend: B,
        group_name: str,
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[T]:
        """Reads and automatically acknowledges entries in a single call (At-Most-Once)."""
        return await backend.stream_read_and_ack(
            stream_key=await cls.get_schema_name(),
            group_name=group_name,
            record_class=cls,
            consumer_id=consumer_id,
            count=count,
            block_ms=block_ms,
        )

    @classmethod
    @ensure_streaming
    async def stream_length(cls, backend: B) -> int:
        """Returns the total number of entries currently stored in the stream log."""
        return await backend.stream_length(await cls.get_schema_name())

    @classmethod
    @ensure_streaming
    async def stream_trim(
        cls,
        backend: B,
        max_len: int,
        approximate: bool = True,
    ) -> int:
        """
        Trims the stream log to at most `max_len` entries, evicting the oldest entries first.

        :param backend:
        :param max_len: Maximum entries to retain.
        :param approximate: If True, allows performance-optimized approximate trimming.
        :return: Total number of evicted entries.
        """
        return await backend.stream_trim(
            stream_key=await cls.get_schema_name(),
            max_len=max_len,
            approximate=approximate,
        )

    @classmethod
    @ensure_streaming
    async def stream_delete(cls, backend: B) -> bool:
        """Deletes the entire stream log and purges associated consumer group states."""
        return await backend.stream_delete(await cls.get_schema_name())

    @classmethod
    @ensure_streaming
    async def stream_read_and_ack(
        cls: Type[T],
        backend: B,
        group_name: str,
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
    ) -> List[T]:
        """
        Reads and automatically acknowledges entries in a single call (At-Most-Once delivery).

        :param backend:
        :param group_name: Consumer group tracking offset namespace.
        :param consumer_id: Specific worker instance identifier within the group.
        :param count: Maximum batch size to retrieve.
        :param block_ms: Block time in milliseconds.
        :return: List of deserialized model instances directly.
        """
        return await backend.stream_read_and_ack(
            stream_key=await cls.get_schema_name(),
            group_name=group_name,
            record_class=cls,
            consumer_id=consumer_id,
            count=count,
            block_ms=block_ms,
        )
