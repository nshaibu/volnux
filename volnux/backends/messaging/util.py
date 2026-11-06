import logging
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    StreamCapabilityMixin,
)

logger = logging.getLogger(__name__)


# Dedicated thread pool for long-running blocking commands such as (BLPOP, SUBSCRIBE.listen)
# This prevents blocking the default asyncio.to_thread pool.
_BLOCKING_EXECUTOR = ThreadPoolExecutor(
    max_workers=100, thread_name_prefix="volnux-messaging-block"
)


def supports_pubsub(backend: Any) -> bool:
    """Return True if the backend implements PubSubCapabilityMixin."""
    return isinstance(backend, PubSubCapabilityMixin)


def supports_pushpop(backend: Any) -> bool:
    """Return True if the backend implements PushPopCapabilityMixin."""
    return isinstance(backend, PushPopCapabilityMixin)


def supports_streaming(backend: Any) -> bool:
    """Return True if the backend implements StreamCapabilityMixin."""
    return isinstance(backend, StreamCapabilityMixin)


def require_pubsub(backend: Any, feature: str) -> None:
    """
    Assert that backend supports pub/sub.
    Raises a clear error when a feature that needs pub/sub is used
    with a backend that does not implement it.

    Usage:
        require_pubsub(engine.checkpoint_backend, "RehydrationManager")
    """
    if not supports_pubsub(backend):
        raise TypeError(
            f"{feature} requires a backend that implements PubSubCapabilityMixin. "
            f"'{type(backend).__name__}' does not support pub/sub. "
            f"Configure a Redis or PostgreSQL backend for this feature."
        )


def require_pushpop(backend: Any, feature: str) -> None:
    """
    Assert that backend supports push/pop.

    Usage:
        require_pushpop(engine.checkpoint_backend, "HITLQueue")
    """
    if not supports_pushpop(backend):
        raise TypeError(
            f"{feature} requires a backend that implements PushPopCapabilityMixin. "
            f"'{type(backend).__name__}' does not support push/pop. "
            f"Configure a Redis backend for this feature."
        )


def require_streaming(backend: Any, feature: str) -> None:
    """
    Assert that backend supports streaming.

    Usage:
        require_streaming(engine.checkpoint_backend, "RehydrationManager")
    """

    if not supports_streaming(backend):
        raise TypeError(
            f"{feature} requires a backend that implements StreamCapabilityMixin. "
            f"'{type(backend).__name__}' does not support streaming. "
            f"Configure a Redis backend for this feature."
        )
