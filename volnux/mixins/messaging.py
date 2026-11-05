import logging
from typing import (
    Any,
    Dict,
    Awaitable,
    Callable,
    List,
    Optional,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Union,
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
        AsyncSubscriptionContext,
        PubSubCapabilityMixin,
        PushPopCapabilityMixin,
        StreamCapabilityMixin,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="MessagingBackendIntegrationMixin")

RecordCallback = Callable[[T], Union[Awaitable[None], None]]

# Called with (exception, record) when a subscriber callback raises.
ErrorCallback = Callable[[BaseException, Any], Union[Awaitable[None], None]]


def _check_timeout(timeout: Optional[float]) -> None:
    """Reject negative timeouts for every backend.

    Without this the backends disagree: Redis raises, the in-memory store
    treated a negative timeout as "already expired".
    """
    if timeout is not None and timeout < 0:
        raise ValueError(f"timeout must be None or >= 0, got {timeout!r}")


def _check_positive(name: str, value: Optional[int], allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if value is None or value < 1:
        raise ValueError(f"{name} must be >= 1, got {value!r}")


def _check_expiry_supported(backend: Any) -> None:
    """Refuse a ``ttl`` on a backend that cannot expire a queue, before pushing."""
    from volnux.backends.messaging.base import PushPopCapabilityMixin

    if type(backend).queue_expire is PushPopCapabilityMixin.queue_expire:
        raise NotImplementedError(
            f"{type(backend).__name__} does not support expiring a queue (ttl); "
            "push without a ttl, or use a Redis or in-memory messaging backend."
        )


class MessagingBackendIntegrationMixin(BackendConnectionIntegrationMixin):
    """
    Mixin providing messaging and queue backend integration for implementations that support Pub/Sub,
    push/pop queues and append-only streams.

    Every operation is addressed by the model's resolved schema name (see ``get_schema_name``),
    which is used as the channel, queue key or stream key. Capability compliance is enforced
    declaratively with method decorators that resolve the model's backend and inject it as the
    ``backend`` argument:

    - @ensure_pubsub: Ephemeral broadcast messaging
    - @ensure_pushpop: Destructive FIFO/LIFO queue operations
    - @ensure_streaming: Persistent, replayable, append-only log operations

    Callers never pass ``backend`` themselves, e.g. ``await Model.push(instance)``.

    These models are stored in the ``"messaging"`` entry of ``DATABASES`` (``"default"``
    if there is none): publish/subscribe needs a store that supports it, which a
    SQL ``default`` does not.
    """

    @classmethod
    def get_database_alias(cls) -> str:
        return "messaging"

    # Pub/Sub

    @classmethod
    @ensure_pubsub
    async def publish(cls, backend: "PubSubCapabilityMixin", record: T) -> int:
        """
        Publishes a given record to the model's channel.

        :param record: The record to be published.
        :return: The number of subscribers that received the published record.
        """
        return await backend.publish(
            channel=await cls.get_schema_name(),
            record=record,
        )

    @classmethod
    @ensure_pubsub
    async def subscribe(
        cls: Type[T],
        backend: "PubSubCapabilityMixin",
        callback: RecordCallback,
        *,
        on_error: Optional[ErrorCallback] = None,
    ) -> None:
        """
        Subscribes to the model's channel and invokes ``callback`` with each received record.

        Runs until the subscription ends (connection closed) or the calling task is cancelled.

        :param callback: Sync or async callable invoked with each deserialized record.
        :param on_error: Optional sync or async callable ``(exception, record)``. When
            given, an exception raised by ``callback`` is passed to it and the
            subscription keeps running. When omitted (default), the exception ends
            the subscription and propagates to the caller.
        """
        await cls._consume(
            backend.subscribe(await cls.get_schema_name(), record_class=cls),
            callback,
            on_error,
        )

    @classmethod
    @ensure_pubsub
    async def psubscribe(
        cls: Type[T],
        backend: "PubSubCapabilityMixin",
        *patterns: str,
        callback: RecordCallback,
        on_error: Optional[ErrorCallback] = None,
    ) -> None:
        """
        Subscribes to the model's channel plus any channels matching ``patterns``
        (backend glob syntax), invoking ``callback`` with each received record.

        :param patterns: Additional channel patterns to subscribe to.
        :param callback: Sync or async callable invoked with each deserialized record.
        :param on_error: Same as in ``subscribe``.
        """
        # Ordered de-duplication keeps the subscription order deterministic
        pattern_list = list(dict.fromkeys([await cls.get_schema_name(), *patterns]))
        await cls._consume(
            backend.psubscribe(*pattern_list, record_class=cls),
            callback,
            on_error,
        )

    @classmethod
    async def _consume(
        cls,
        subscription: "AsyncSubscriptionContext",
        callback: RecordCallback,
        on_error: Optional[ErrorCallback] = None,
    ) -> None:
        async with subscription as messages:
            async for message in messages:
                try:
                    await as_coroutine(callback, message.data)
                except Exception as exc:  # CancelledError is a BaseException
                    if on_error is None:
                        raise
                    logger.warning(
                        "Subscriber callback failed for %s: %s", cls.__name__, exc
                    )
                    await as_coroutine(on_error, exc, message.data)

    # Push/Pop queues
    #
    # A model has one route, so by default every record of the model lands in one
    # queue. A caller that needs a queue per entity (per job, per task) passes
    # ``route_override``: values for the route's ``{placeholders}``, as for streams.

    @classmethod
    @ensure_pushpop
    async def push(
        cls,
        backend: "PushPopCapabilityMixin",
        *instances: Any,
        side: QueueSide = QueueSide.RIGHT,
        ttl: Optional[float] = None,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Pushes one or more instances onto the model's queue.

        :param instances: One or more instances to be added to the queue.
        :param side: The end of the queue to push to (default: tail).
        :param ttl: Seconds after which the queue is dropped if nobody emptied it.
            Every push moves the deadline, so a producer that keeps pushing keeps
            the queue. A safety net for queues whose consumer may never come.
        :param route_override: Values for the route placeholders, to address another
            queue of the same model.
        :return: The length of the queue after the push.
        :raises ValueError: If ``ttl`` is not positive.
        :raises NotImplementedError: ``ttl`` on a backend that cannot expire a queue.
        """
        if not instances:
            raise ValueError("push() requires at least one instance")
        if ttl is not None and ttl <= 0:
            raise ValueError(f"ttl must be None or > 0, got {ttl!r}")
        key = await cls._route_key(route_override)
        if ttl is not None:
            # (refused before anything is pushed)
            _check_expiry_supported(backend)
        length = await backend.push(key, *instances, side=side)
        if ttl is not None:
            await backend.queue_expire(key, ttl)
        return length

    @classmethod
    @ensure_pushpop
    async def pop(
        cls: Type[T],
        backend: "PushPopCapabilityMixin",
        *,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
        route_override: Optional[Dict[str, str]] = None,
    ) -> Optional[T]:
        """
        Removes and returns an item from the model's queue.

        :param timeout: Seconds to wait for an item. ``None`` (default) does not wait and
            returns ``None`` immediately if the queue is empty; ``0`` waits indefinitely.
        :param side: The end of the queue to pop from (default: head).
        :param route_override: As for ``push``.
        :return: The item removed from the queue, or ``None`` if none was available.
        :raises ValueError: If ``timeout`` is negative.
        """
        _check_timeout(timeout)
        return await backend.pop(
            await cls._route_key(route_override),
            record_class=cls,
            timeout=timeout,
            side=side,
        )

    @classmethod
    @ensure_pushpop
    async def pop_many(
        cls: Type[T],
        backend: "PushPopCapabilityMixin",
        *,
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
        route_override: Optional[Dict[str, str]] = None,
    ) -> List[T]:
        """
        Waits for the first item, then removes a batch of items from the model's queue.

        :param limit: The maximum number of items to remove. ``None`` removes everything
            currently available.
        :param timeout: Seconds to wait for the FIRST item (same semantics as ``pop``).
        :param side: The end of the queue to pop from (default: head).
        :param route_override: As for ``push``.
        :return: The items removed, in pop order. Empty if no item arrived in time.
        :raises ValueError: If ``timeout`` is negative.
        """
        _check_timeout(timeout)
        return await backend.pop_many(
            await cls._route_key(route_override),
            record_class=cls,
            limit=limit,
            timeout=timeout,
            side=side,
        )

    @classmethod
    @ensure_pushpop
    async def enqueue(
        cls,
        backend: "PushPopCapabilityMixin",
        instance: Any,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        FIFO enqueue: pushes ``instance`` to the tail of the model's queue.

        :param instance: The instance to be enqueued.
        :param route_override: As for ``push``.
        :return: The length of the queue after the push.
        """
        return await backend.enqueue(await cls._route_key(route_override), instance)

    @classmethod
    @ensure_pushpop
    async def dequeue(
        cls: Type[T],
        backend: "PushPopCapabilityMixin",
        timeout: Optional[float] = None,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> Optional[T]:
        """
        FIFO dequeue: removes and returns the head of the model's queue.

        :param timeout: Same semantics as ``pop``: ``None`` does not wait, ``0`` waits
            indefinitely, ``> 0`` waits at most that many seconds.
        :param route_override: As for ``push``.
        :return: The dequeued item, or ``None`` if none was available.
        :raises ValueError: If ``timeout`` is negative.
        """
        _check_timeout(timeout)
        return await backend.dequeue(
            await cls._route_key(route_override),
            record_class=cls,
            timeout=timeout,
        )

    @classmethod
    @ensure_pushpop
    async def queue_length(
        cls,
        backend: "PushPopCapabilityMixin",
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """Return the number of items currently in the queue for this model identity."""
        return await backend.queue_length(await cls._route_key(route_override))

    @classmethod
    @ensure_pushpop
    async def queue_range(
        cls: Type[T],
        backend: "PushPopCapabilityMixin",
        start: int = 0,
        stop: int = -1,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> List[T]:
        """Return a slice of the queue (``stop`` inclusive) without removing items."""
        return await backend.queue_range(
            await cls._route_key(route_override),
            record_class=cls,
            start=start,
            stop=stop,
        )

    @classmethod
    @ensure_pushpop
    async def queue_peek(
        cls: Type[T],
        backend: "PushPopCapabilityMixin",
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> Optional[T]:
        """Return the head item as a model instance without removing it."""
        return await backend.queue_peek(
            await cls._route_key(route_override), record_class=cls
        )

    @classmethod
    @ensure_pushpop
    async def queue_delete(
        cls,
        backend: "PushPopCapabilityMixin",
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Delete the queue and everything in it.

        :return: True if the queue existed (a backend that cannot remove a queue
            returns False).
        """
        return await backend.queue_delete(await cls._route_key(route_override))

    # Append-only streams
    #
    # A model has one route, so by default every record of the model lands in one
    # stream. A caller that needs a stream per entity (per producing task, say)
    # passes ``route_override``: values for the route's ``{placeholders}``, resolved
    # exactly like the routing keys of the model itself.

    @classmethod
    async def _route_key(cls, route_override: Optional[Dict[str, str]] = None) -> str:
        """The channel / queue / stream key: the model's own, or with the route's
        placeholders filled from ``route_override``."""
        if not route_override:
            return await cls.get_schema_name()
        backend = await cls.get_backend()
        return cls.get_storage_route().resolve(backend, route_override)

    @classmethod
    async def _stream_key(cls, route_override: Optional[Dict[str, str]] = None) -> str:
        return await cls._route_key(route_override)

    @classmethod
    @ensure_streaming
    async def stream_append(
        cls,
        backend: "StreamCapabilityMixin",
        record: Any,
        max_len: Optional[int] = None,
        approximate_trim: bool = True,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> str:
        """Appends a record to the model's append-only stream log.

        :param route_override: Values for the route placeholders, to address another
            stream of the same model.
        :return: The backend-assigned entry ID.
        :raises ValueError: If ``max_len`` is negative.
        """
        if max_len is not None and max_len < 0:
            raise ValueError(f"max_len must be None or >= 0, got {max_len!r}")
        return await backend.stream_append(
            stream_key=await cls._stream_key(route_override),
            record=record,
            max_len=max_len,
            approximate_trim=approximate_trim,
        )

    @classmethod
    @ensure_streaming
    async def stream_ensure_consumer_group(
        cls,
        backend: "StreamCapabilityMixin",
        group_name: str,
        start_from: Union[StreamOffset, str] = StreamOffset.BEGINNING,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> None:
        """Ensures a consumer group namespace exists for this stream log."""
        await backend.stream_ensure_consumer_group(
            stream_key=await cls._stream_key(route_override),
            group_name=group_name,
            start_from=start_from,
        )

    @classmethod
    @ensure_streaming
    async def stream_read(
        cls: Type[T],
        backend: "StreamCapabilityMixin",
        group_name: str,
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> List[StreamEntry[T]]:
        """Reads undelivered entries from the stream for a consumer group.

        :raises ValueError: If ``count`` is below 1 or ``block_ms`` is negative.
        """
        _check_positive("count", count)
        _check_timeout(block_ms)
        return await backend.stream_read(
            stream_key=await cls._stream_key(route_override),
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
        backend: "StreamCapabilityMixin",
        group_name: str,
        *entry_ids: str,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """Acknowledges processed entries, advancing the consumer group offset."""
        return await backend.stream_ack(
            await cls._stream_key(route_override),
            group_name,
            *entry_ids,
        )

    @classmethod
    @ensure_streaming
    async def stream_claim_pending(
        cls: Type[T],
        backend: "StreamCapabilityMixin",
        group_name: str,
        consumer_id: str,
        min_idle_ms: int = 30_000,
        count: int = 100,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> List[StreamEntry[T]]:
        """Claims orphaned pending stream entries from crashed workers.

        :raises ValueError: If ``min_idle_ms`` is negative or ``count`` is below 1.
        """
        _check_timeout(min_idle_ms)
        _check_positive("count", count)
        return await backend.stream_claim_pending(
            stream_key=await cls._stream_key(route_override),
            group_name=group_name,
            consumer_id=consumer_id,
            record_class=cls,
            min_idle_ms=min_idle_ms,
            count=count,
        )

    @classmethod
    @ensure_streaming
    async def stream_length(
        cls,
        backend: "StreamCapabilityMixin",
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """Returns the total number of entries currently stored in the stream log."""
        return await backend.stream_length(await cls._stream_key(route_override))

    @classmethod
    @ensure_streaming
    async def stream_trim(
        cls,
        backend: "StreamCapabilityMixin",
        max_len: int,
        approximate: bool = True,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Trims the stream log to at most `max_len` entries, evicting the oldest entries first.

        :param max_len: Maximum entries to retain.
        :param approximate: If True, allows performance-optimized approximate trimming.
        :return: Total number of evicted entries.
        :raises ValueError: If ``max_len`` is negative.
        """
        if max_len < 0:
            raise ValueError(f"max_len must be >= 0, got {max_len!r}")
        return await backend.stream_trim(
            stream_key=await cls._stream_key(route_override),
            max_len=max_len,
            approximate=approximate,
        )

    @classmethod
    @ensure_streaming
    async def stream_delete(
        cls,
        backend: "StreamCapabilityMixin",
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Deletes the entire stream log and purges associated consumer group states."""
        return await backend.stream_delete(await cls._stream_key(route_override))

    @classmethod
    @ensure_streaming
    async def stream_read_and_ack(
        cls: Type[T],
        backend: "StreamCapabilityMixin",
        group_name: str,
        consumer_id: Optional[str] = None,
        count: int = 1,
        block_ms: Optional[int] = 5000,
        *,
        route_override: Optional[Dict[str, str]] = None,
    ) -> List[T]:
        """
        Reads and automatically acknowledges entries in a single call (At-Most-Once delivery).

        :param group_name: Consumer group tracking offset namespace.
        :param consumer_id: Specific worker instance identifier within the group.
        :param count: Maximum batch size to retrieve.
        :param block_ms: Block time in milliseconds.
        :return: List of deserialized model instances directly.
        :raises ValueError: If ``count`` is below 1 or ``block_ms`` is negative.
        """
        _check_positive("count", count)
        _check_timeout(block_ms)
        return await backend.stream_read_and_ack(
            stream_key=await cls._stream_key(route_override),
            group_name=group_name,
            record_class=cls,
            consumer_id=consumer_id,
            count=count,
            block_ms=block_ms,
        )
