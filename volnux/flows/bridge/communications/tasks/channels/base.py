import asyncio
import multiprocessing as mp
from typing import Optional, Union, TYPE_CHECKING, Type
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..base import TaskCommand, TaskMessage

QueueType = Union[asyncio.Queue, mp.Queue]


class CommandChannelBase(ABC):
    """
    Bidirectional channel for coordinator ↔ task communication.
    """

    def __init__(self, task_id: str):
        self.task_id = task_id

        self._command_queue: Optional[QueueType] = None
        self._message_queue: Optional[QueueType] = None

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
