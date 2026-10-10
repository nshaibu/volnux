import asyncio
from typing import TYPE_CHECKING, Optional

from .base import CommandChannelBase

if TYPE_CHECKING:
    from ..base import TaskCommand, TaskMessage


class LocalCommandChannel(CommandChannelBase):
    """
    Provides a communication channel for exchanging commands and messages locally
    between a task and its coordinator.

    This class facilitates asynchronous sending and receiving of commands and
    messages using in-memory queues. It serves as a channel for communication
    in local execution environments where a task's coordinator and the task itself
    operate within the same process.

    :ivar task_id: The unique identifier for the task associated with this
        communication channel.
    :type task_id: str
    """

    def __init__(self, task_id: str):
        super().__init__(task_id)

        self._command_queue: asyncio.Queue["TaskCommand"] = asyncio.Queue()
        self._message_queue: asyncio.Queue["TaskMessage"] = asyncio.Queue()

    async def send_command(self, command: "TaskCommand") -> None:
        """Send command from coordinator to task"""
        await self._command_queue.put(command)

    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskCommand"]:
        """Receive command in a task"""

        try:
            return await asyncio.wait_for(self._command_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def send_message(self, message: "TaskMessage") -> None:
        """Send a message from a task to coordinator"""

        await self._message_queue.put(message)

    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskMessage"]:
        """Receive a message in coordinator"""

        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
