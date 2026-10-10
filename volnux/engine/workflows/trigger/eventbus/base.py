import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Optional, Union

from ..event import Event

logger = logging.getLogger(__name__)

# A subscriber may be sync or async.
EventCallback = Callable[[Event], Union[Awaitable[None], None]]


class EventBusAdapterBase(ABC):
    """
    Abstract adapter for different event bus implementations.

    Supported backends (first-party):
      - In-memory (for testing / single-process deployments)
      - Redis Pub/Sub
      - RabbitMQ
      - Kafka, AWS SQS/SNS, etc. (write your own adapter)

    Lifecycle
    ---------
    Adapters MUST be safely idempotent for ``connect()`` / ``disconnect()``.
    Calling ``connect()`` twice is a no-op; publishing before ``connect()`` or
    after ``disconnect()`` raises :class:`RuntimeError`.

    Topic resolution
    ----------------
    When ``topic`` is omitted on :meth:`publish`, ``event.event_type`` is used.
    Wildcard support (e.g. ``"*"``) is *backend-specific*; consult the adapter.

    Usage
    -----
    Adapters can be used as async context managers::

        async with RedisEventBus(url) as bus:
            await bus.publish(event)
    """

    def __init__(self) -> None:
        self._connected: bool = False

    @property
    def is_connected(self) -> bool:
        """Whether the adapter currently holds an open connection."""
        return self._connected

    async def connect(self) -> None:
        """
        Open the underlying transport.

        Subclasses should override :meth:`_connect` rather than this method so
        the idempotency guard and connected-flag bookkeeping stay consistent.
        """
        if self._connected:
            logger.debug("%s already connected; skipping", type(self).__name__)
            return
        await self._connect()
        self._connected = True
        logger.info("%s connected", type(self).__name__)

    async def disconnect(self) -> None:
        """Close the underlying transport (idempotent)."""
        if not self._connected:
            return
        try:
            await self._disconnect()
        finally:
            self._connected = False
            logger.info("%s disconnected", type(self).__name__)

    @abstractmethod
    async def _connect(self) -> None:
        """Backend-specific connection logic."""

    @abstractmethod
    async def _disconnect(self) -> None:
        """Backend-specific teardown logic."""

    @abstractmethod
    async def publish(self, event: Event, topic: Optional[str] = None) -> None:
        """
        Publish ``event`` to ``topic``.

        If ``topic`` is ``None``, ``event.event_type`` is used.

        Raises:
            RuntimeError: If the adapter is not connected.
        """

    @abstractmethod
    async def subscribe(self, topic: str, callback: EventCallback) -> None:
        """
        Subscribe ``callback`` to ``topic``.

        Callbacks may be sync or async. Adapters invoke them via
        :meth:`_invoke_callback`.
        """

    @abstractmethod
    async def unsubscribe(self, topic: str, callback: EventCallback) -> None:
        """
        Remove a previously registered subscription.

        Implementations SHOULD be tolerant of unknown ``(topic, callback)``
        pairs: log a warning and return rather than raising.
        """

    async def __aenter__(self) -> "EventBusAdapterBase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.disconnect()

    @staticmethod
    def _resolve_topic(event: Event, topic: Optional[str]) -> str:
        """Return the effective topic name for ``event``."""
        return topic or event.event_type

    def _require_connected(self) -> None:
        """Raise if the adapter isn't currently connected."""
        if not self._connected:
            raise RuntimeError(
                f"{type(self).__name__} is not connected. "
                "Call `await bus.connect()` first."
            )

    @staticmethod
    async def _invoke_callback(callback: EventCallback, event: Event) -> None:
        """
        Invoke a subscriber callback, awaiting if it is a coroutine function.

        Errors raised by the callback are logged and swallowed so that a single
        faulty subscriber cannot break delivery to the rest.
        """
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)  # type: ignore[misc]
            else:
                result = callback(event)
                # Handle sync callables that still return an awaitable.
                if asyncio.iscoroutine(result):
                    await result
        except Exception:  # noqa: BLE001
            logger.exception(
                "Subscriber %r raised while handling event %s",
                getattr(callback, "__qualname__", callback),
                event.event_id,
            )
