import types
import typing
from typing import Any, Tuple

from formax import ValidationError

# "no default given" (formax's MISSING cannot be a dataclass default)
UNSET: Any = object()


def formax_null_validator(instance, value):
    if value is None:
        raise ValidationError("Field cannot be null")


def unwrap_optional(annotation: Any) -> Tuple[Any, bool]:
    """
    Split ``Optional[X]`` into ``(X, True)``; anything else is ``(annotation, False)``.

    Works for forward references too (``Optional["Name"]``). A union of several
    types besides ``None`` keeps them together: ``Optional[Union[A, B]]`` gives
    ``(Union[A, B], True)``.
    """
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is getattr(types, "UnionType", None):
        arguments = typing.get_args(annotation)
        remaining = tuple(arg for arg in arguments if arg is not type(None))
        if len(remaining) != len(arguments):
            inner = remaining[0] if len(remaining) == 1 else typing.Union[remaining]
            return inner, True
    return annotation, False
