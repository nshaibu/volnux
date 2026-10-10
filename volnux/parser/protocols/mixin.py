import typing

if typing.TYPE_CHECKING:
    from ..conditional import ConditionalNode
    from ..operator import PipeType
    from ..options import Options
    from .task import TaskProtocol
    from .task_group import TaskGroupingProtocol
    from .typing import TaskType


class TaskProtocolMixin(typing.Protocol):
    """Mixin for Task protocol"""

    # options specified in pointy scripts for tasks are kept here
    options: typing.Optional["Options"]

    sequence_number: typing.Optional[int]

    parent_node: typing.Optional["TaskType"]

    # sink event this is where the conditional events collapse
    # into after they are done executing
    sink_node: typing.Optional["TaskType"]
    sink_pipe: typing.Optional["PipeType"]

    condition_node: "ConditionalNode"

    def get_id(self) -> str: ...

    def get_event_name(self) -> str: ...

    @property
    def descriptor(self) -> int: ...

    @descriptor.setter
    def descriptor(self, value: int) -> None: ...

    @property
    def descriptor_pipe(self) -> "PipeType": ...

    @descriptor_pipe.setter
    def descriptor_pipe(self, value: "PipeType") -> None: ...

    @property
    def is_conditional(self) -> bool: ...

    @property
    def is_descriptor_task(self) -> bool: ...

    @property
    def is_sink(self) -> bool: ...

    @property
    def is_parallel_execution_node(self) -> bool: ...

    def get_root(self) -> "TaskType": ...

    def get_first_task_in_parallel_execution_mode(
        self,
    ) -> typing.Optional["TaskType"]: ...

    def get_last_task_in_parallel_execution_mode(
        self,
    ) -> typing.Optional["TaskType"]: ...

    def get_descriptor(self, descriptor: int) -> "TaskType": ...

    def get_children(self) -> typing.List["TaskType"]: ...

    def get_pointer_to_task(self) -> typing.Optional["PipeType"]: ...

    def get_dot_node_data(self) -> str: ...

    def get_parallel_nodes(
        self,
    ) -> typing.Deque["TaskType"]: ...

    @classmethod
    def bf_traversal(
        cls,
        root: "TaskType",
    ) -> typing.Generator["TaskType", None, None]: ...
