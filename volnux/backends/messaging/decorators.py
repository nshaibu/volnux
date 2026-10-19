import functools
from typing import Any, Awaitable, Callable, TypeVar

from .util import require_pubsub, require_pushpop, require_streaming


T = TypeVar("T")

_Method = Callable[..., Awaitable[T]]


def _ensure_capability(
    require: Callable[[Any, str], None],
) -> Callable[[_Method], _Method]:
    """Build a decorator that resolves the model's backend, checks it with
    ``require``, and passes it to the wrapped classmethod as its second argument.
    """

    def decorator(func: _Method) -> _Method:
        @functools.wraps(func)
        async def wrapper(cls: Any, *args: Any, **kwargs: Any) -> T:
            backend = await cls.get_backend()
            require(backend, cls.__name__)
            return await func(cls, backend, *args, **kwargs)

        return wrapper

    return decorator


ensure_pubsub = _ensure_capability(require_pubsub)
ensure_pubsub.__doc__ = """Tag a method under the Pub/Sub messaging paradigm.

Verifies that the model's backend implements PubSubCapabilityMixin
before executing the method.
"""

ensure_pushpop = _ensure_capability(require_pushpop)
ensure_pushpop.__doc__ = """Tag a method under the Queue (Push/Pop) paradigm.

Verifies that the model's backend implements PushPopCapabilityMixin
before executing the method.
"""

ensure_streaming = _ensure_capability(require_streaming)
ensure_streaming.__doc__ = """Tag a method under the Durable Stream Log paradigm.

Verifies that the model's backend implements StreamCapabilityMixin
before executing the method.
"""
