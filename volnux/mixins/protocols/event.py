import asyncio
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Protocol,
    Tuple,
    Type,
    Union,
    Optional,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from volnux.result import EventResult
    from volnux.execution.context import ExecutionContext
    from volnux.mixins.event import RetryPolicy
    from volnux.execution.rehydrator.event.snapshot import EventPhase
    from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
    from volnux.execution.rehydrator.event.snapshot import (
        EventCheckpointSnapshot,
        ResourceState,
    )
    from volnux.execution.rehydrator.event.resources import (
        ResourceProvider,
        ResourceMonitor,
    )
    from volnux.flows.bridge.communications.tasks import TaskCommand
    from volnux.flows.bridge.communications.tasks.base import CommandChannelBase


class BaseEvent(Protocol):
    """
    Represents the base interface for event processing, including retry mechanisms,
    resource management, and execution status tracking.

    This abstract class defines the contract for managing execution retries,
    handling resources, creating snapshots, and processing events through various
    stages. It serves as a template for building systems with event-oriented
    processing workflows. The class encapsulates utility methods for retrying
    tasks, registering and restoring resources, managing checkpoints, and handling
    execution outcomes.

    :ivar retry_policy: Defines the retry policy for the event.
    :type retry_policy: RetryPolicy
    :ivar exec_result: The result of the execution.
    :type exec_result: Any
    :ivar exec_status: Indicates the success or failure status of the execution.
    :type exec_status: bool
    :ivar checkpoint_manager: Manages checkpoints for event processing.
    :type checkpoint_manager: VolnuxCheckPointManager
    :ivar run_bypass_event_checks: Indicates whether event checks should be bypassed.
    :type run_bypass_event_checks: bool
    """

    # Retry configuration
    _retry_count: int
    retry_policy: "RetryPolicy"

    # state management
    _execution_context: "ExecutionContext"
    _task_id: str
    _sequence_number: int

    # Execution status
    _phase: "EventPhase"
    exec_result: Any
    exec_status: bool
    checkpoint_manager: "VolnuxCheckPointManager"

    # Resource management
    _init_args: dict
    _call_args: Union[dict, Any]
    _external_resources: Dict[str, "ResourceState"]
    _resource_instances: Dict[str, object] = {}
    _resource_monitor: "ResourceMonitor"

    # communication
    _pause_gate: asyncio.Event
    _preempted: bool = False
    _main_worker_task: Optional[asyncio.Task]
    _command_listener_task: Optional[asyncio.Task]
    _command_channel: "CommandChannelBase"

    def init_retry(self) -> Union["RetryPolicy", None]:
        """
        Initialize the retry policy for the given context.

        This method is used to configure the retry policy settings, if applicable.
        It can return a specific instance of a RetryPolicy, or None if no retries
        are to be applied.

        :return: An instance of RetryPolicy if a retry policy is defined,
            else returns None.
        :rtype: Union[RetryPolicy, None]
        """
        ...

    async def acquire_resource(
        self,
        name: str,
        provider: Union[str, Type["ResourceProvider"]],
        init_args: dict,
        init_func: Optional[Callable[[dict], Any]] = None,
    ) -> Any:
        """
        Acquires a resource by its name, provider, and initialization data. If the resource
        is already managed, it attempts to restore it. Otherwise, it initializes a new
        resource using the specified provider or an initialization function.
        """
        ...

    def _register_resource(
        self,
        resource_name: str,
        resource: Any,
        provider: Union[str, Type["ResourceProvider"]],
    ) -> None:
        """
        Registers a resource with a specified name, resource instance, and provider.
        This function associates a resource with a given provider, enabling the system
        to manage or utilize the resource effectively.

        :param resource_name: The name assigned to identify the resource.
        :type resource_name: str
        :param resource: The resource instance to be registered.
        :type resource: Any
        :param provider: The provider associated with the resource, which can be
            a string identifier or a type of ResourceProvider.
        :type provider: Union[str, Type[ResourceProvider]]
        :return: None
        """
        ...

    def _restore_resource(
        self, resource_name: str, resource_config: "ResourceState", bind: bool = True
    ) -> object:
        """
        Restores the state of a specified resource using the provided configuration.

        This method reinitializes or reconfigures a resource to a previous or desired
        state defined by the resource configuration.

        :param resource_name: The name of the resource to be restored.
        :param resource_config: The configuration object containing the specific state
            details for the resource.
        :param bind: Whether to bind the restored resource to the event context. Defaults to True.
        :return: object
        """
        ...

    @staticmethod
    def _process_result_tuple(
        result: Any, method_name="process"
    ) -> Tuple[bool, Any]: ...

    async def bypass(self) -> Optional[Tuple[bool, Any]]: ...

    def is_exhausted(self) -> bool: ...

    def is_retryable(self, exception: Exception) -> bool: ...

    async def _sleep_for_backoff(self) -> float: ...

    async def _retry(
        self,
        func: Callable[[Tuple[Any, ...], Dict[str, Any]], Awaitable[Tuple[bool, Any]]],
        /,
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any],
    ) -> Tuple[bool, Any]: ...

    # Event Hooks
    async def communicate(
        self,
        /,
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any],
    ) -> None:
        """

        :param args:
        :return:
        """
        ...

    async def process(self, *args, **kwargs) -> Tuple[bool, Any]:
        """
        Asynchronous method to process given arguments and return a tuple containing a boolean
        status and additional data. The specific logic of processing is determined by the
        implementation of this method.

        :param args: Positional arguments required for processing. Usage depends on the specific
                     implementation.
        :param kwargs: Keyword arguments required for processing. Usage depends on the specific
                       implementation.
        :return: A tuple where the first element is a boolean indicating whether the processing
                 was successful, and the second element contains additional data from the
                 process.
        :rtype: Tuple[bool, Any]
        """
        ...

    async def cleanup(self, *args, **kwargs) -> None:
        """
        Cleans up resources or performs necessary teardown actions.

        This method is meant to be implemented for releasing resources,
        closing connections, or any other cleanup tasks required within
        the system. It should handle any necessary logic to gracefully
        finalize operations.

        :param args: Positional arguments passed to the cleanup method.
        :type args: tuple
        :param kwargs: Keyword arguments passed to the cleanup method.
        :type kwargs: dict
        :return: This method does not return any value.
        :rtype: None
        """
        ...

    def on_success(self, execution_result: Any) -> "EventResult": ...

    def on_failure(self, execution_result: Any) -> "EventResult": ...

    # Checkpoint Management
    async def create_snapshot(self) -> "EventCheckpointSnapshot": ...

    async def enqueue_checkpoint(self) -> None: ...

    async def _process_wrapper(
        self, *args: Any, **kwargs: Any
    ) -> Union["EventResult", Tuple[bool, Any]]: ...

    def _get_steps(self) -> List[Callable[..., Any]]: ...

    # Task Communication
    async def _send_update(
        self,
        msg_type: "MessageType",
        state: "TaskState",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None: ...

    async def _handle_command(self, command: "TaskCommand") -> None:
        """
        Handles the given command by processing the task specified.

        :param command: The task command to be processed.
        :type command: TaskCommand
        :return: None
        :rtype: None
        """
        ...

    async def _command_listener(self) -> None:
        """
        Listens for incoming commands and processes them asynchronously.

        :return: None
        :rtype: None
        """
        ...

    async def _run_step(self, step, *args, **kwargs) -> Any:
        """
        Executes a single step in an asynchronous context, allowing for additional
        arguments to be passed dynamically. This method is intended to handle
        execution logic for configurable steps while supporting asynchronous
        operations.

        :param step: The step to be executed.
        :type step: Any
        :param args: Additional positional arguments required for the step execution.
        :type args: tuple
        :param kwargs: Additional keyword arguments required for the step execution.
        :type kwargs: dict
        :return: The result after executing the step.
        :rtype: Any
        """
        ...

    async def steps_runner(self, *args, **kwargs) -> "EventResult":
        """
        Executes a sequence of steps asynchronously and returns the result.

        The method is designed to handle a list of steps or tasks executed in a
        specific order, allowing for dynamic handling of arguments provided at
        runtime. It ensures the final result is an instance of `EventResult`.

        :param args: Positional arguments passed to the step runner dynamically.
                     These arguments can vary depending on the implementation of
                     the steps.
        :param kwargs: Keyword arguments passed to the step runner dynamically.
                       These arguments are used for additional flexibility in
                       configuring or controlling the steps being executed.
        :return: An instance of `EventResult`, which contains the outcome of the
                 executed steps sequence.
        :rtype: EventResult
        """
        ...
