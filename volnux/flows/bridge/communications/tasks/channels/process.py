import multiprocessing as mp
from typing import TYPE_CHECKING, Optional

from volnux.concurrency.async_utils import to_thread
from .base import CommandChannelBase

if TYPE_CHECKING:
    from ..base import TaskCommand, TaskMessage


class ProcessCommandChannel(CommandChannelBase):
    """
    A communication channel implementation for task coordination, utilizing
    multiprocessing queues for commands and messages.

    This class is specifically designed to facilitate two-way asynchronous
    communication between a coordinator and a task. Commands can be sent
    from the coordinator to the task, and messages can be sent from the
    task to the coordinator. The class uses multiprocessing queues to
    ensure safe and process-isolated message passing.

    :ivar task_id: Unique identifier for the task associated with this
        communication channel.
    :type task_id: str
    """

    def __init__(self, task_id: str):
        super().__init__(task_id)

        ctx = mp.get_context("spawn")
        self._command_queue: mp.Queue["TaskCommand"] = ctx.Queue()
        self._message_queue: mp.Queue["TaskMessage"] = ctx.Queue()

    async def send_command(self, command: "TaskCommand") -> None:
        """Send command from coordinator to task"""

        await to_thread(self._command_queue.put, command)

    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskCommand"]:
        """Receive command in task"""

        try:
            return await to_thread(self._command_queue.get, True, timeout or 0.1)
        except Exception:
            return None

    async def send_message(self, message: "TaskMessage") -> None:
        """Send message from task to coordinator"""

        await to_thread(self._message_queue.put, message)

    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskMessage"]:
        """Receive message in coordinator"""

        try:
            return await to_thread(self._message_queue.get, True, timeout or 0.1)
        except Exception:
            return None
