import typing

from .base import TaskCommunicationBridge
from .channels.local import LocalCommandChannel

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


class LocalTaskCommunicationBridge(TaskCommunicationBridge):
    """
    Control plane for tasks running in *this* process: the default (async)
    executor and thread-pool executors.

    Commands and messages travel over in-memory thread-safe queues
    (``LocalCommandChannel``), so a task may consume them on a private event
    loop in another thread. Tasks running anywhere else (process pool, Celery)
    use ``MessagingTaskCommunicationBridge`` instead.

    :ivar channel_class: The command channel class used by this bridge.
    """

    channel_class = LocalCommandChannel

    def __init__(self, context: "ExecutionContext"):
        super().__init__(context)
