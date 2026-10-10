import asyncio
import inspect
import logging
from collections import deque
from copy import deepcopy
from typing import (
    Any,
    Deque,
    Dict,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    TYPE_CHECKING,
)
from dataclasses import dataclass, field, InitVar

from volnux.constants import EMPTY
from volnux.concurrency.async_utils import to_thread
from volnux.execution.context import ExecutionContext
from volnux.executors import BaseExecutor, ProcessPoolExecutor
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.mixins import ObjectIdentityMixin

# from volnux.mixins.event.checkpointing import SubPhaseSuspension
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskGroupingProtocol, TaskProtocol, TaskType
from volnux.signal import SoftSignal
from volnux.signal.signals import event_execution_end, event_execution_start
from volnux.utils import build_event_arguments_from_pipeline, get_function_call_args
from .bridge.communications.tasks import (
    TaskCommand,
    TaskCommunicationBridge,
    CommandType,
    create_communication_bridge,
)

if TYPE_CHECKING:
    from volnux import Event

logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


def _emit_signal_sync(signal: SoftSignal, **kwargs: Any) -> None:
    """Safe synchronous signal emission for done-callbacks."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(signal.emit_async(**kwargs))
    except RuntimeError:
        # No running loop; fall back to sync emit if available
        signal.emit(**kwargs)


def format_task_profiles(task_profiles: Any) -> Set[TaskType]:
    if isinstance(task_profiles, (TaskProtocol, TaskGroupingProtocol)):
        return {task_profiles}
    return cast(Set[TaskType], task_profiles)


@dataclass
class BaseFlow(ObjectIdentityMixin):
    """Base execution flow for Volnux pipelines.

    Manages task profiles, event initialization (with instance caching),
    executor lifecycle, communication bridging, and sub-phase suspension
    propagation. Subclasses must implement ``run()`` and
    ``get_flow_executor()``.
    """

    context: ExecutionContext
    task_profiles: Optional[Deque[TaskType]] = None
    enable_communication: InitVar[bool] = True

    # Internal state — not part of the public dataclass interface
    _comm_bridge: Optional[TaskCommunicationBridge] = field(
        default=None, init=False, repr=False
    )
    _initialized_events: Dict[str, Tuple["Event", Dict[str, Any]]] = field(
        default_factory=dict, init=False, repr=False
    )
    _flow_executors: list[BaseExecutor] = field(
        default_factory=list, init=False, repr=False
    )

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        enable_communication = kwargs.pop("enable_communication", True)
        self.task_profiles = cast(Deque[TaskType], self.context.task_profiles)

        if enable_communication:
            self._comm_bridge = create_communication_bridge(
                self.context, "executor_type"
            )

        super().__init__(*args, **kwargs)  # type: ignore[misc]

    def add_task_profile(self, task_profile: TaskType) -> None:
        if self.task_profiles is None:
            self.task_profiles = deque([task_profile])
        else:
            self.task_profiles.append(task_profile)

    def get_initialized_event(
        self, task_profile: TaskType
    ) -> Tuple["Event", Dict[str, Any]]:
        """Return a cached, fully configured event instance for the given task.

        Events are cached by task ID so that sub-phase cache, resource state,
        and retry policy survive across multiple access points within a single
        flow execution.
        """
        task_id = task_profile.get_id()
        if task_id in self._initialized_events:
            return self._initialized_events[task_id]

        event_klass = task_profile.get_event_class()
        logger.info("Initializing '%s' (task_id=%s)", task_profile.event, task_id)

        event_init_args, event_call_args = build_event_arguments_from_pipeline(
            event_klass, self.context.pipeline
        )
        event_init_args = event_init_args or {}
        event_call_args = event_call_args or {}

        # Core context injection
        event_init_args["execution_context"] = self.context
        event_init_args["task_id"] = task_id
        event_init_args["sequence_number"] = task_profile.sequence_number

        if task_profile.options:
            event_init_args["options"] = task_profile.options

        # Data flow routing based on pipe type
        pointer_type = self._resolve_pointer_type(task_profile)

        if pointer_type == PipeType.POINTER:
            event_init_args["previous_result"] = (
                self.context.previous_context.results
                if self.context.previous_context
                else EMPTY
            )
        elif pointer_type == PipeType.PIPE_POINTER:
            # Stream integration for |-> operator
            stream_key = task_profile.get_stream_key()
            group_name = f"{self.context.workflow_id}:{task_id}"
            event_init_args["stream_key"] = stream_key
            event_init_args["consumer_group"] = group_name
            event_init_args["checkpoint_manager"] = self.context.checkpoint_manager

        event = event_klass(**event_init_args)
        self._configure_event(event, task_profile)

        result = (event, event_call_args)
        self._initialized_events[task_id] = result
        return result

    @staticmethod
    def _resolve_pointer_type(task_profile: TaskType) -> PipeType:
        if task_profile.is_parallel_execution_node:
            parent = task_profile.get_first_task_in_parallel_execution_mode()
            return parent.get_pointer_to_task()
        return task_profile.get_pointer_to_task()

    def _configure_event(self, event: "Event", task_profile: TaskProtocol) -> None:
        """Configure retry policy without mutating shared state."""
        if not task_profile.options:
            return

        total_retries = task_profile.options.retry_attempts or 0
        if total_retries <= 1:
            return

        event_retry_policy = event.init_retry()
        if event_retry_policy:
            # Deep-copy to prevent cross-task policy leakage
            new_policy = deepcopy(event_retry_policy)
            new_policy.max_attempts = total_retries
            event.config_retry_policy(policy=new_policy)
        else:
            event.config_retry_policy(max_attempts=total_retries)

    @staticmethod
    def get_task_executor_from_options(
        task_profile: Union[TaskProtocol, TaskGroupingProtocol],
    ) -> Optional[Union[Type[BaseExecutor], BaseExecutor]]:
        if task_profile.options and task_profile.options.executor:
            return _GLOBAL_EXECUTOR_REGISTRY.get(task_profile.options.executor)
        return None

    @staticmethod
    def parse_executor_initialisation_configuration(
        executor: Type[BaseExecutor], execution_config: ExecutorInitializerConfig
    ) -> Dict[str, Any]:
        return get_function_call_args(executor.__init__, execution_config.to_dict())

    async def get_flow_executor(self, *args: Any, **kwargs: Any) -> Type[BaseExecutor]:
        raise NotImplementedError(
            "get_flow_executor() must be implemented by subclasses"
        )

    async def get_flow_executor_config(
        self, task_profile: TaskType
    ) -> ExecutorInitializerConfig:
        options_config = (
            task_profile.options.executor_config if task_profile.options else None
        )

        if isinstance(task_profile, TaskProtocol):
            # Uses cached event — no duplicate initialization
            event, _ = self.get_initialized_event(task_profile)
            execution_config = event.get_executor_initializer_config()
            if options_config:
                return execution_config.update(options_config)  # type: ignore[union-attr]
            return execution_config

        return options_config or ExecutorInitializerConfig()

    def _initialize_executor(
        self,
        executor_class: Union[Type[BaseExecutor], BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> BaseExecutor:
        if inspect.isclass(executor_class):
            config = self.parse_executor_initialisation_configuration(
                executor_class, executor_config
            )
            instance = executor_class(**config)
            instance._created_by_flow = True  # type: ignore[attr-defined]
            self._flow_executors.append(instance)
            return instance

        # Shared executor instance (e.g., ProcessPoolExecutor singleton)
        return executor_class

    @staticmethod
    def _should_shutdown_executor(executor: BaseExecutor) -> bool:
        if isinstance(executor, ProcessPoolExecutor):
            return False
        return getattr(executor, "_created_by_flow", False)

    @staticmethod
    async def shutdown_executor(executor: BaseExecutor) -> None:
        if not isinstance(executor, BaseExecutor):
            logger.warning("Executor is not a BaseExecutor instance: %s", executor)
            return
        try:
            logger.debug("Shutting down executor %s", executor.__class__.__name__)
            await to_thread(executor.shutdown, wait=True)
            logger.debug("Executor %s shut down", executor.__class__.__name__)
        except Exception as e:
            logger.warning(
                "Error shutting down executor %s: %s", executor.__class__.__name__, e
            )

    async def _submit_event_to_executor(
        self,
        executor: BaseExecutor,
        event: "Event",
        event_call_kwargs: Dict[str, Any],
    ) -> asyncio.Future:
        logger.info(
            "Submitting event %s to executor %s", event, executor.__class__.__name__
        )

        await event_execution_start.emit_async(
            sender=self.context.__class__,
            event=event,
            execution_context=self.context,
        )

        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(executor, event, event_call_kwargs)

        # Async-safe signal emission on completion
        future.add_done_callback(
            lambda fut: _emit_signal_sync(
                signal=event_execution_end,
                sender=self.context.__class__,
                event=event,
                execution_context=self.context,
            )
        )

        logger.debug("Event submitted successfully; future: %s", future)
        return future

    async def _map_events_to_executor(
        self,
        executor: BaseExecutor,
        event_execution_config: Dict["Event", Any],
    ) -> list[asyncio.Future]:
        """Submit events and return individual futures for exception inspection.

        Callers MUST inspect each future for SubPhaseSuspension before
        treating it as a generic error.
        """
        futures: list[asyncio.Future] = []
        for event, event_call_kwargs in event_execution_config.items():
            future = await self._submit_event_to_executor(
                executor, event, event_call_kwargs
            )
            futures.append(future)
        return futures

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

    async def run(self) -> asyncio.Future:
        raise NotImplementedError("run() must be implemented by subclasses")

    async def close(self) -> None:
        """Clean up executors, communication bridge, and event cache."""
        # Shutdown flow-owned executors concurrently
        shutdown_tasks = [
            self.shutdown_executor(ex)
            for ex in self._flow_executors
            if self._should_shutdown_executor(ex)
        ]
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        self._flow_executors.clear()

        # Clear event cache to release references
        self._initialized_events.clear()

        # Shutdown communication bridge
        if self._comm_bridge:
            try:
                await self._comm_bridge.shutdown()
            except Exception as e:
                logger.warning(
                    "Error shutting down communication bridge: %s", e, exc_info=True
                )
            finally:
                self._comm_bridge = None

    async def cancel(self, *args: Any, **kwargs: Any) -> None:
        if not self._comm_bridge:
            return
        for task_profile in self.context.task_profiles:
            task_id = task_profile.get_id()
            command = TaskCommand(task_id=task_id, command_type=CommandType.CANCEL)
            try:
                await self._comm_bridge.send_command(task_id, command)
            except Exception as e:
                logger.warning(
                    "Error cancelling task %s: %s", task_id, e, exc_info=True
                )

    async def get_task_status(self, task_id: str) -> Any:
        if not self._comm_bridge:
            return None
        try:
            return await self._comm_bridge.get_status(task_id)
        except Exception as e:
            logger.warning(
                "Error getting status for task %s: %s", task_id, e, exc_info=True
            )
            return None

    async def pause_task(self, task_id: str) -> bool:
        if not self._comm_bridge:
            return False
        try:
            return await self._comm_bridge.send_command(
                task_id, TaskCommand(task_id=task_id, command_type=CommandType.PAUSE)
            )
        except Exception as e:
            logger.warning("Error pausing task %s: %s", task_id, e, exc_info=True)
            return False

    async def resume_task(self, task_id: str) -> bool:
        if not self._comm_bridge:
            return False
        try:
            return await self._comm_bridge.send_command(
                task_id, TaskCommand(task_id=task_id, command_type=CommandType.RESUME)
            )
        except Exception as e:
            logger.warning("Error resuming task %s: %s", task_id, e, exc_info=True)
            return False
