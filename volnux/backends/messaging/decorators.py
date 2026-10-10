import functools
from typing import Any, Awaitable, Callable, TypeVar

from .util import require_pubsub, require_pushpop, require_streaming


T = TypeVar("T")


def ensure_pubsub(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Documentational decorator tagging a method under the Pub/Sub messaging paradigm.

    Verifies that the model's backend implements PubSubCapabilityMixin
    before executing the method.
    """

    @functools.wraps(func)
    async def wrapper(cls: Any, *args: Any, **kwargs: Any) -> T:
        backend = cls.get_backend()
        require_pubsub(backend, cls.__name__)
        return await func(cls, backend, *args, **kwargs)

    return wrapper


def ensure_pushpop(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Documentational decorator tagging a method under the Queue (Push/Pop) paradigm.

    Verifies that the model's backend implements PushPopCapabilityMixin
    before executing the method.
    """

    @functools.wraps(func)
    async def wrapper(cls: Any, *args: Any, **kwargs: Any) -> T:
        backend = cls.get_backend()
        require_pushpop(backend, cls.__name__)
        return await func(cls, backend, *args, **kwargs)

    return wrapper


def ensure_streaming(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Documentational decorator tagging a method under the Durable Stream Log paradigm.

    Verifies that the model's backend implements StreamCapabilityMixin
    before executing the method.
    """

    @functools.wraps(func)
    async def wrapper(cls: Any, *args: Any, **kwargs: Any) -> T:
        backend = cls.get_backend()
        require_streaming(backend, cls.__name__)
        return await func(cls, backend, *args, **kwargs)

    return wrapper
