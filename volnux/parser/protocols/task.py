import typing

from .mixin import TaskProtocolMixin

if typing.TYPE_CHECKING:
    from volnux.base import EventBase


@typing.runtime_checkable
class TaskProtocol(TaskProtocolMixin, typing.Protocol):
    """Individual task protocol."""

    event: typing.Union[typing.Type["EventBase"], str]

    def __init__(
        self,
        event: typing.Union[typing.Type["EventBase"], str],
    ) -> None: ...

    def get_event_class(self) -> typing.Type["EventBase"]:
        """Return event class of task"""
