import logging
import typing
import asyncio

from .base import TaskCommunicationBridge, TaskExecutionHandle

from .channels.local import LocalCommandChannel

if typing.TYPE_CHECKING:
    from .channels.base import CommandChannelBase
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class LocalTaskCommunicationBridge(TaskCommunicationBridge):
    """
    Facilitates communication between task execution and command channels within
    a local environment.

    This class serves as a bridge to manage the registration of tasks and ensure
    proper communication channels are allocated. It operates on an event-driven
    model, leveraging asynchronous locks and events to maintain consistency
    and thread safety.

    :ivar channel_class: The command channel class used by this bridge.
    :type channel_class: Type[LocalCommandChannel]
    """

    channel_class = LocalCommandChannel

    def __init__(self, context: "ExecutionContext"):
        super().__init__(context)

        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()

    async def register_task(self, handle: TaskExecutionHandle) -> "CommandChannelBase":
        """
        Register a task execution and return its communication channel.

        The channel should be injected into the event instance.
        """
        async with self._lock:
            channel, full_id = self._register_task(handle)

        logger.debug(f"Registered execution: {handle.event_name} ({full_id})")

        return channel
