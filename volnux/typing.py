from enum import Enum
from typing import (
    Any,
    Callable,
    Collection,
    Deque,
    Generator,
    Iterator,
    List,
    Optional,
    TypeAlias,
    TypeVar,
    Union,
)


class ConfigState(Enum):
    """Configuration state indicators"""

    UNSET = "unset"


T = TypeVar("T")
ConfigurableValue: TypeAlias = Union[T, None, ConfigState]

BatchProcessType: TypeAlias = Callable[
    [
        Union[Collection[Any], Any],
        Optional[Union[int, float]],
    ],
    Union[Iterator[Any], Generator[Any, None, None]],
]
