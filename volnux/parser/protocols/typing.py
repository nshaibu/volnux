import typing

from .task import TaskProtocol
from .task_group import TaskGroupingProtocol

# The type for all tasks
TaskType = typing.Union[TaskProtocol, TaskGroupingProtocol]
