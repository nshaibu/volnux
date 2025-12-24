import asyncio
import logging
import typing
from abc import abstractmethod
from collections import deque

from pydantic_mini import BaseModel

from volnux.base import ExecutorInitializerConfig
from volnux.constants import EMPTY
from volnux.execution.context import ExecutionContext
from volnux.executors import BaseExecutor
from volnux.import_utils import import_string
from volnux.mixins import ObjectIdentityMixin
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskGroupingProtocol, TaskProtocol, TaskType
from volnux.signal import SoftSignal
from volnux.signal.signals import event_execution_end, event_execution_start
from volnux.utils import build_event_arguments_from_pipeline, get_function_call_args

if typing.TYPE_CHECKING:
    from volnux import Event


logger = logging.getLogger(__name__)


def attach_signal_emitter(
    signal: SoftSignal, **signal_kwargs: typing.Dict[str, typing.Any]
) -> None:
    """Attaches a signal emitter to the execution context."""
    signal.emit(**signal_kwargs)


def format_task_profiles(
    task_profiles: typing.Any,
) -> typing.Set[TaskType]:
    if isinstance(task_profiles, (TaskProtocol, TaskGroupingProtocol)):
        return {
            task_profiles,
        }
    return typing.cast(typing.Set[TaskType], task_profiles)


class BaseFlow(BaseModel, ObjectIdentityMixin):
    # The execution context for this flow
    context: ExecutionContext

    #  The profile of the tasks to be executed
    task_profiles: typing.Optional[typing.Deque[TaskType]]  # type: ignore

    class Config:
        disable_type_check = False
        disable_all_validations = False

    def __model_init__(
        self, *args: typing.Any, **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        self.task_profiles = typing.cast(
            typing.Deque[TaskType], self.context.task_profiles
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
            task_profile.options and task_profile.options.retry_attempts or 0
        )
        total_retries = options_retries
        if total_retries > 1:  # type: ignore
            event_retry_policy = event.get_retry_policy()
            if event_retry_policy:
                event_retry_policy.max_attempts = total_retries  # type: ignore
            else:
                event.config_retry_policy(max_attempts=total_retries)  # type: ignore

    def get_initialized_event(
        self, task_profile: TaskType
    ) -> typing.Tuple["Event", typing.Dict[str, typing.Any]]:
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
    async def get_task_executor_from_options(
        task_profile: typing.Union[TaskProtocol, TaskGroupingProtocol],
    ) -> typing.Optional[typing.Type[BaseExecutor]]:
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
                try:
                    instance = typing.cast(
                        typing.Type[BaseExecutor], import_string(executor_str)
                    )
                    if not issubclass(instance, BaseExecutor):
                        raise ValueError(f"Unsupported executor type {executor_str}")
                    return instance
                except ImportError:
                    logger.warning("Could not import executor '%s'", executor_str)
                except ValueError as e:
                    logger.warning(str(e))
        return None

    @staticmethod
    def parse_executor_initialisation_configuration(
        executor: typing.Type[BaseExecutor], execution_config: ExecutorInitializerConfig
    ) -> typing.Dict[str, typing.Any]:
        """
        Parse the executor initialization configuration
        Args:
            executor: The executor to initialise.
            execution_config: The execution configuration to parse.
        Returns:
            The parsed executor initialization configuration.
        """
        return get_function_call_args(executor.__init__, execution_config.to_dict())

    @abstractmethod
    async def get_flow_executor(
        self, *args: typing.Any, **kwargs: typing.Dict[str, typing.Any]
    ) -> typing.Type[BaseExecutor]:
        raise NotImplementedError()

    async def get_flow_executor_config(
        self, task_profile: TaskType
    ) -> ExecutorInitializerConfig:
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
        event_call_kwargs: typing.Dict[str, typing.Any],
        *,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
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
        event_execution_config: typing.Dict["Event", typing.Any],
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
        executor_class: typing.Type[BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> None:
        if isinstance(executor_class, Exception):
            raise RuntimeError(f"Failed to get executor class: {executor_class}")
        if isinstance(executor_config, Exception):
            raise ValueError(f"Invalid executor config: {executor_config}")

    @abstractmethod
    async def run(self) -> asyncio.Future:
        """
        Run the flow.
        Raises:
            ValueError: If the flow cannot be run.
            RuntimeError: If the flow encounters an error during execution.
            asyncio.TimeoutError: If the flow times out during execution.
            Exception: For any other exceptions that may occur.
        """

    async def cancel(
        self, *args: typing.Any, **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        """
        Cancel the flow execution.
        """
        pass
