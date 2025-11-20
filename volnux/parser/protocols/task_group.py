import typing
from enum import Enum, auto

from .mixin import TaskProtocolMixin

if typing.TYPE_CHECKING:
    from .task import TaskProtocol
    from .typing import TaskType


class GroupingStrategy(Enum):
    # Multiple chains executed in parallel: {A->B, C||D}
    # Results aggregated and sent to next task outside the grouping if any
    MULTIPATH_CHAINS = auto()

    # Single chain executed: {A->B}
    SINGLE_CHAIN = auto()


@typing.runtime_checkable
class TaskGroupingProtocol(TaskProtocolMixin, typing.Protocol):
    """Task group protocol."""

    # head of each task chain
    chains: typing.List["TaskType"]

    # strategy for this grouping
    strategy: GroupingStrategy

    def __init__(self, chains: typing.List["TaskType"]) -> None: ...
