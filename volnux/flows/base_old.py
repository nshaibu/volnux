import asyncio
import logging
import inspect
from collections import deque
from typing import (
    Optional,
    Deque,
    Dict,
    Any,
    Union,
    Tuple,
    cast,
    Set,
    Type,
    TYPE_CHECKING,
)
from dataclasses import dataclass, InitVar

from volnux.constants import EMPTY
from volnux.execution.context import ExecutionContext
from volnux.executors import BaseExecutor, ProcessPoolExecutor
from volnux.mixins import ObjectIdentityMixin
from volnux.parser.operator import PipeType
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.parser.protocols import TaskGroupingProtocol, TaskProtocol, TaskType
from volnux.signal import SoftSignal
from volnux.signal.signals import event_execution_end, event_execution_start
from volnux.utils import build_event_arguments_from_pipeline, get_function_call_args
from .bridge.communications.tasks import (
    create_communication_bridge,
    TaskCommunicationBridge,
    TaskCommand,
    CommandType,
)
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.concurrency.async_utils import to_thread

if TYPE_CHECKING:
    from volnux import Event


logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


def attach_signal_emitter(signal: SoftSignal, **signal_kwargs: Dict[str, Any]) -> None:
    """Attaches a signal emitter to the execution context."""
    signal.emit(**signal_kwargs)


def format_task_profiles(
    task_profiles: Any,
) -> Set[TaskType]:
    if isinstance(task_profiles, (TaskProtocol, TaskGroupingProtocol)):
        return {
            task_profiles,
        }
    return cast(Set[TaskType], task_profiles)


@dataclass()
class BaseFlow(ObjectIdentityMixin):
    """
    BaseFlow class.

    A class designed to handle the execution flow of tasks within a pipeline. It manages
    task profiles, event initialization, configuration, and communication bridging. This
    class is intended as a base class and requires specific methods to be implemented by
    subclasses.

    :ivar context: The execution context for this flow.
    :type context: ExecutionContext
    :ivar task_profiles: The profile of the tasks to be executed.
    :type task_profiles: Optional[Deque[TaskType]]
    """

    # The execution context for this flow
    context: ExecutionContext

    #  The profile of the tasks to be executed
    task_profiles: Optional[Deque[TaskType]] = None

    # Task communication bridge
    _comm_bridge: Optional[TaskCommunicationBridge] = None
    enable_communication: InitVar[bool] = True

    # class Config:
    #     init_strategy = InitStrategy.DATACLASS
    #     validation = ValidationFlags.NONE

    def __post_init__(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        enable_communication = kwargs.pop("enable_communication", True)
        self.task_profiles = cast(Deque[TaskType], self.context.task_profiles)

        if enable_communication:
            self._comm_bridge = create_communication_bridge(
                self.context, "executor_type"
            )

        super().__init__(*args, **kwargs)  # type: ignore

    def add_task_profile(self, task_profile: TaskType) -> None:
        """
        Add a task profile to this flow.
        Args:
            task_profile: The task profile to add.
        """
        if self.task_profiles is None:
            self.task_profiles = deque([task_profile])
        self.task_profiles.append(task_profile)

    def configure_event(self, event: "Event", task_profile: TaskProtocol) -> None:
        """
        Configure event for this flow.
        Args:
            event: The event to configure.
            task_profile: The task profile that this event is configured for.
        """
        options_retries = (
            task_profile.options.retry_attempts if task_profile.options else 0
        )
        total_retries = options_retries
        if total_retries > 1:
            event_retry_policy = event.init_retry()
            if event_retry_policy:
                event_retry_policy.max_attempts = total_retries
            else:
                event.config_retry_policy(max_attempts=total_retries)

    def get_initialized_event(
        self, task_profile: TaskType
    ) -> Tuple["Event", Dict[str, Any]]:
        """
        Initialized and configure event
        :param task_profile: The task profile to initialize
        :return: A tuple of the initialized event and the event call arguments
        """
        event_klass = task_profile.get_event_class()

        logger.info(f"Initializing '{task_profile.event}'")

        event_init_args, event_call_ars = build_event_arguments_from_pipeline(
            event_klass, self.context.pipeline
        )

        event_init_args = event_init_args or {}
        event_call_args = event_call_ars or {}

        event_init_args["execution_context"] = self.context
        event_init_args["task_id"] = task_profile.get_id()
        event_init_args["sequence_number"] = task_profile.sequence_number

        # Let's pass the options given in the pointy script to the event
        if task_profile.options:
            event_init_args["options"] = task_profile.options

        if task_profile.is_parallel_execution_node:
            parent = task_profile.get_first_task_in_parallel_execution_mode()
            pointer_type = parent.get_pointer_to_task()
        else:
            pointer_type = task_profile.get_pointer_to_task()

        if pointer_type == PipeType.PIPE_POINTER:
            # TODO: get result from context state
            if self.context.previous_context:
                event_init_args["previous_result"] = (
                    self.context.previous_context.state.results
                )
            else:
                event_init_args["previous_result"] = EMPTY

        event = event_klass(**event_init_args)

        # configure the event
        self.configure_event(event, task_profile)

        return event, event_call_args

    @staticmethod
    def get_task_executor_from_options(
        task_profile: Union[TaskProtocol, TaskGroupingProtocol],
    ) -> Optional[Union[Type[BaseExecutor], BaseExecutor]]:
        """
        Get the executor class from the task profile options if available.
        Args:
            task_profile: The task profile to get the executor class from.
        Returns:
            The executor class or None if not found or invalid.
        """
        if task_profile.options:
            executor_str: str = task_profile.options.executor  # type: ignore
            if executor_str is not None:
                return _GLOBAL_EXECUTOR_REGISTRY.get(executor_str)
        return None

    @staticmethod
    def parse_executor_initialisation_configuration(
        executor: Type[BaseExecutor], execution_config: "ExecutorInitializerConfig"
    ) -> Dict[str, Any]:
        """
        Parse the executor initialization configuration
        Args:
            executor: The executor to initialize.
            execution_config: The execution configuration to parse.
        Returns:
            The parsed executor initialization configuration.
        """
        return get_function_call_args(executor.__init__, execution_config.to_dict())

    async def get_flow_executor(
        self, *args: Any, **kwargs: Dict[str, Any]
    ) -> Type[BaseExecutor]:
        raise NotImplementedError(
            "get_flow_executor() must be implemented by subclasses"
        )

    async def get_flow_executor_config(
        self, task_profile: TaskType
    ) -> "ExecutorInitializerConfig":
        """
        Get the init configuration for executor
        Args:
            task_profile: The task profile to get the executor config from.
        Returns:
              ExecutorInitializerConfig: The init configuration for executor
        """
        options_config = None

        # get config from options
        if task_profile.options:
            options_config = task_profile.options.executor_config

        if isinstance(task_profile, TaskProtocol):
            event, _ = self.get_initialized_event(task_profile)
            execution_config = event.get_executor_initializer_config()
            if options_config:
                new_config = execution_config.update(options_config)  # type: ignore
                return new_config
            return execution_config

        if options_config:
            return options_config  # type: ignore
        return ExecutorInitializerConfig()

    async def _submit_event_to_executor(
        self,
        executor: BaseExecutor,
        event: "Event",
        event_call_kwargs: Dict[str, Any],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> asyncio.Future:
        """
        Submit event for execution via the provided executor.

        Args:
            executor (BaseExecutor): The executor responsible for running the event task.
                This could be a ThreadPoolExecutor, ProcessPoolExecutor, or any other
                executor implementing the 'Executor' interface.
            event (Event): The event to submit.
            event_call_kwargs (Dict): A dictionary containing data for the event.
            loop (asyncio.AbstractEventLoop): The event loop to use.
        Returns:
            Future
        """
        logger.info(
            f"Submitting event {event} to executor {executor.__class__.__name__}"
        )

        await event_execution_start.emit_async(
            sender=self.context.__class__,
            event=event,
            execution_context=self.context,
        )

        if loop is None:
            loop = asyncio.get_event_loop()

        event_args = (event_call_kwargs,)

        future = loop.run_in_executor(executor, event, *event_args)
        future.add_done_callback(
            lambda fut: attach_signal_emitter(
                signal=event_execution_end,
                sender=self.context.__class__,  # type: ignore
                event=event,  # type: ignore
                execution_context=self.context,  # type: ignore
            )
        )
        logger.debug(f"Event submitted successfully; future: {future}")
        return future

    async def _map_events_to_executor(
        self,
        executor: BaseExecutor,
        event_execution_config: Dict["Event", Any],
    ) -> asyncio.Future:
        """
        Submit events to the provided executor class.

        Args:
            executor (Type[BaseExecutor]): The executor class to use for executing the events.
            event_execution_config (Dict): A dictionary containing data for the events.

        Returns:
            Future
        """
        loop = asyncio.get_event_loop()
        futures = []
        for event, event_call_kwargs in event_execution_config.items():
            future = await self._submit_event_to_executor(
                executor, event, event_call_kwargs, loop=loop
            )
            futures.append(future)

        return asyncio.gather(*futures, return_exceptions=True)

    @staticmethod
    def validate_executor_class_and_config(
        executor_class: Type[BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> None:
        if isinstance(executor_class, Exception):
            raise RuntimeError(
                f"Failed to get executor class: {executor_class.__class__.__name__}: {executor_class}"
            )
        if isinstance(executor_config, Exception):
            raise ValueError(
                f"Invalid executor config: {executor_config.__class__.__name__}: {executor_config}"
            )

    def _initialize_executor(
        self,
        executor_class: Union[Type[BaseExecutor], BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> BaseExecutor:
        """
        Initialize an executor instance.

        Args:
            executor_class: Executor class or instance
            executor_config: Configuration for initialization

        Returns:
            Executor instance

        Note:
            For shared executors (ProcessPoolExecutor), this returns a reference
            to the workflow-managed instance. For per-flow executors, creates
            a new instance that should be shut down after use.
        """
        executor_instance = executor_class

        if inspect.isclass(executor_instance):
            config = self.parse_executor_initialisation_configuration(
                executor_class, executor_config
            )
            executor_instance = executor_class(**config)

            # Mark as created by this flow
            if hasattr(executor_instance, "__dict__"):
                executor_instance._created_by_flow = True

        return executor_instance

    def _should_shutdown_executor(self, executor: BaseExecutor) -> bool:
        """
        Determines if the provided executor should be shut down.

        Checks the type of the executor and whether it has been marked as
        created by a specific flow to decide if it qualifies for shutdown.

        :param executor: The executor instance being evaluated.
        :type executor: BaseExecutor

        :return: Returns True if the executor is marked for shutdown,
            otherwise False.
        :rtype: bool
        """
        if isinstance(executor, ProcessPoolExecutor):
            return False
        if hasattr(executor, "_created_by_flow"):
            return executor._created_by_flow
        return False

    async def run(self) -> asyncio.Future:
        """
        Run the flow.
        Raises:
            ValueError: If the flow cannot be run.
            RuntimeError: If the flow encounters an error during execution.
            asyncio.TimeoutError: If the flow times out during execution.
            Exception: For any other exceptions that may occur.
        """
        raise NotImplementedError("run() must be implemented by subclasses")

    @staticmethod
    async def shutdown_executor(executor: BaseExecutor) -> None:
        if not isinstance(executor, BaseExecutor):
            logger.warning(f"Executor is not an instance of BaseExecutor: {executor}")
            return

        try:
            logger.debug(
                f"Shutting down executor {executor.__class__.__name__} "
                "(non-blocking)"
            )
            await to_thread(executor.shutdown, wait=True)
            logger.debug(f"Executor {executor.__class__.__name__} shut down")
        except Exception as e:
            logger.warning(
                f"Error shutting down executor {executor.__class__.__name__}: {e}"
            )

    async def close(self) -> None:
        """Cleanup communication bridge and other resources."""
        if self._comm_bridge:
            try:
                await self._comm_bridge.shutdown()
            except Exception as e:
                logger.warning(
                    f"Error shutting down communication bridge: {e}", exc_info=True
                )
            finally:
                self._comm_bridge = None

    async def cancel(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        """
        Cancel the flow execution.
        """
        if self._comm_bridge:
            for task_profile in self.context.task_profiles:
                task_id = task_profile.get_id()
                command = TaskCommand(task_id=task_id, command_type=CommandType.CANCEL)
                try:
                    await self._comm_bridge.send_command(task_id, command)
                except Exception as e:
                    logger.warning(
                        f"Error cancelling task {task_id}: {e}", exc_info=True
                    )

    async def get_task_status(self, task_id: str):
        """Get the current status of a specific task"""
        if self._comm_bridge:
            try:
                return await self._comm_bridge.get_status(task_id)
            except Exception as e:
                logger.warning(
                    f"Error getting status for task {task_id}: {e}", exc_info=True
                )
        return None

    async def pause_task(self, task_id: str) -> bool:
        """Pause a running task"""
        if self._comm_bridge:
            command = TaskCommand(task_id=task_id, command_type=CommandType.PAUSE)
            try:
                return await self._comm_bridge.send_command(task_id, command)
            except Exception as e:
                logger.warning(f"Error pausing task {task_id}: {e}", exc_info=True)
        return False

    async def resume_task(self, task_id: str) -> bool:
        """Resume a paused task"""
        if self._comm_bridge:
            command = TaskCommand(task_id=task_id, command_type=CommandType.RESUME)
            try:
                return await self._comm_bridge.send_command(task_id, command)
            except Exception as e:
                logger.warning(f"Error resuming task {task_id}: {e}", exc_info=True)
        return False
