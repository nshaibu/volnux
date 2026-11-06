import asyncio
import multiprocessing as mp
from typing import Optional, Union, TYPE_CHECKING, Type
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..base import TaskCommand, TaskMessage

QueueType = Union[asyncio.Queue, mp.Queue]


class CommandChannelBase(ABC):
    """
    Bidirectional channel for coordinator <-> task communication.

    Each side calls only its own half, possibly from a different thread, event
    loop or process than the other side:

    * engine side: ``send_command`` and ``receive_message``
    * event (task) side: ``receive_command`` and ``send_message``

    The ``receive_*`` methods wait up to ``timeout`` seconds and return ``None``
    when nothing arrived (``timeout=0`` only checks). A channel must be usable
    from any event loop and must not lose an item when a waiting receive is
    cancelled.
    """

    #: Seconds the event's command listener waits per ``receive_command`` call.
    #: A command wakes the waiting call immediately, so this only bounds how
    #: quickly the listener notices it was cancelled; a remote transport raises
    #: it to avoid a round trip per poll.
    poll_interval: float = 0.25

    def __init__(self, task_id: str):
        self.task_id = task_id

    @abstractmethod
    async def send_command(self, command: "TaskCommand") -> None:
        """Send a command from coordinator to task"""
        raise NotImplementedError("send_command must be implemented by subclasses")

    @abstractmethod
    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskCommand"]:
        """Receive command in a task"""
        raise NotImplementedError("receive_command must be implemented by subclasses")

    @abstractmethod
    async def send_message(self, message: "TaskMessage") -> None:
        """Send a message from a task to coordinator"""
        raise NotImplementedError("send_message must be implemented by subclasses")

    @abstractmethod
    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskMessage"]:
        """Receive a message in coordinator"""
        raise NotImplementedError("receive_message must be implemented by subclasses")

    def create_child(self, task_id: str) -> "CommandChannelBase":
        """A channel of the same kind for a sub-task (e.g. an agent's tool call),
        with its own queues and listener lifecycle."""
        return type(self)(task_id)

    async def close(self) -> None:
        """Release any transport resources. Safe to call more than once."""
