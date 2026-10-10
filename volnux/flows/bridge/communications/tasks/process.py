import logging
import typing
import threading

from .base import TaskCommunicationBridge, TaskExecutionHandle
from .channels.process import ProcessCommandChannel
from volnux.concurrency.async_utils import to_thread

if typing.TYPE_CHECKING:
    from channels.base import CommandChannelBase
    from volnux.execution.context import ExecutionContext


logger = logging.getLogger(__name__)


class ProcessTaskCommunicationBridge(TaskCommunicationBridge):
    """
    Acts as a bridge for task communication by utilizing a process-based command channel.

    This class implements a task communication bridge specifically for handling tasks using
    processes. It facilitates task synchronization, registration, and communication while
    ensuring thread-safe access and operations through process-aware locks and events.

    :ivar channel_class: The command channel class to be used for process communication.
    :type channel_class: Type[ProcessCommandChannel]
    """

    channel_class = ProcessCommandChannel

    def __init__(self, context: "ExecutionContext"):
        super().__init__(context)

        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()

    def _register_task_sync(self, handle: TaskExecutionHandle) -> "CommandChannelBase":
        """Register a new task and return its command queue"""
        with self._lock:
            channel, full_id = self._register_task(handle)

        logger.debug(f"Registered execution: {handle.event_name} ({full_id})")

        return channel

    async def register_task(self, handle: TaskExecutionHandle) -> "CommandChannelBase":
        return await to_thread(self._register_task_sync, handle)
