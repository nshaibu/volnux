from .base import TaskBase


class DeferredTask(TaskBase):
    """
    Represents a deferred task that is triggered by a specific event.

    This class serves as an extension of TaskBase, designed to manage tasks
    that are associated with delayed or event-driven execution. Each
    instance of this class binds an event to define the conditions under
    which the task will be executed.

    :ivar event: The name of the event that triggers the execution of the task.
    :type event: str
    """

    def __init__(self, event: str) -> None:
        super().__init__()

        self.event = event
