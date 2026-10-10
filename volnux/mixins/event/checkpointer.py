import asyncio
import logging
import inspect
import time
from typing import (
    List,
    Dict,
    Tuple,
    Any,
    cast,
    Callable,
    Union,
    Type,
    Optional,
    TYPE_CHECKING,
)
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

from volnux.import_utils import import_string
from volnux.constants import EMPTY
from volnux.config import VolnuxConfig
from volnux.result import EventResult
from volnux.mixins._phase_decorator import phase_step
from volnux.signal.signals import event_init
from volnux.concurrency.async_utils import to_thread
from volnux.parser.options import Options, StopCondition
from volnux.exceptions import (
    MaxRetryError,
    SuspendTask,
    SwitchTask,
    SkipExecutionError,
    EmptyResultError,
)
from volnux.utils import get_function_call_args, get_obj_klass_import_str
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    ResourceState,
)
from volnux.execution.rehydrator.event.builder import SnapshotBuilder
from volnux.execution.rehydrator.event.resources import (
    ResourceProvider,
    ResourceMonitor,
)
from volnux.mixins.protocols.event import BaseEvent as _BaseEvent

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext
    from volnux.flows.bridge.communications.tasks.base import CommandChannelBase


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


# Dedicated executor for sync sub_phase init_funcs with timeouts.
# NEVER use the default asyncio.to_thread pool.
_SUB_PHASE_EXECUTOR = ThreadPoolExecutor(
    max_workers=64, thread_name_prefix="volnux-subphase"
)


class SubPhaseSuspension(Exception):
    """Internal signal raised by sub_phase to suspend the event.

    Carries the sub-phase name so the engine can:
      1. Update ExecutionRecord.status to SUSPENDED
      2. Record which sub-phase is awaiting external input
      3. Route webhook/HITL responses to the correct resume point
    """

    def __init__(self, sub_phase_name: str, pending_value: Any):
        self.sub_phase_name = sub_phase_name
        self.pending_value = pending_value
        super().__init__(f"Sub-phase '{sub_phase_name}' suspended")


@dataclass
class StopConditionProcessor:
    """
    Processor for handling stop conditions with improved error handling and flexibility.
    Attributes:
        stop_condition: The condition that determines when to stop processing
        exception: Any exception that occurred during processing
        message: Optional message for logging or debugging
    """

    stop_condition: Union[StopCondition, List[StopCondition]]
    exception: Optional[Exception] = None
    message: Optional[str] = None

    def should_stop(self, success: bool = True) -> bool:
        """
        Determine if processing should stop based on the current state.
        Args:
            success: Whether the operation was successful
        Returns:
            bool: True if processing should stop, False otherwise
        """
        if self.stop_condition == StopCondition.NEVER:
            return False

        if isinstance(self.stop_condition, (list, tuple)):
            return any(
                self._evaluate_single_condition(cond, success)
                for cond in self.stop_condition
            )

        return self._evaluate_single_condition(self.stop_condition, success)

    def _evaluate_single_condition(
        self, condition: StopCondition, success: bool
    ) -> bool:
        """Evaluate a single stop condition."""
        if condition == StopCondition.NEVER:
            return False
        elif condition == StopCondition.ON_SUCCESS:
            return success and self.exception is None
        elif condition == StopCondition.ON_ERROR:
            return not success or self.exception is not None
        elif condition == StopCondition.ON_ANY:
            return True
        else:
            logger.warning(f"Unknown stop condition: {condition}")
            return False

    def on_success(self) -> bool:
        """
        Handle successful operation completion.
        Returns:
            bool: True if processing should stop
        """
        self.exception = None
        should_stop = self.should_stop(success=True)

        if should_stop:
            self._log_stop_decision("success")

        return should_stop

    def on_error(self, exception: Exception, message: Optional[str] = None) -> bool:
        """
        Handle error during operation.
        Args:
            exception: The exception that occurred
            message: Optional additional message
        Returns:
            bool: True if processing should stop
        """
        self.exception = exception
        if message:
            self.message = message

        should_stop = self.should_stop(success=False)

        if should_stop:
            self._log_stop_decision("error")
        return should_stop

    def reset(self) -> None:
        """Reset the processor state for reuse."""
        self.exception = None
        self.message = None

    def _log_stop_decision(self, event_type: str) -> None:
        """Log the stop decision with context."""
        context = {
            "event_type": event_type,
            "stop_condition": self.stop_condition,
            "has_exception": self.exception is not None,
            "message": self.message,
        }

        if self.exception:
            logger.info(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )
        else:
            logger.debug(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )

    def get_status(self) -> Dict[str, Any]:
        """Get the current processor status for debugging."""
        return {
            "stop_condition": self.stop_condition,
            "has_exception": self.exception is not None,
            "exception_type": type(self.exception).__name__ if self.exception else None,
            "message": self.message,
        }


class EventCheckpointingMixin:

    @staticmethod
    def _process_result_tuple(result: Any, method_name="process") -> Tuple[bool, Any]:
        is_event_result = isinstance(result, EventResult)
        is_tuple = isinstance(result, tuple)

        if not is_event_result and not is_tuple:
            raise ValueError(
                f"{method_name} result must be a tuple with two elements or an instance of EventResult"
            )
        if is_tuple and not isinstance(result[0], bool):
            raise ValueError(f"First element of {method_name} result must be a boolean")

        if is_event_result:
            return result.success, result

        return result

    @phase_step(EventPhase.INITIALIZED)
    def _setup_event(
        self: _BaseEvent,
        execution_context: "ExecutionContext",
        task_id: str,
        *args: Tuple[Any],
        checkpoint_manager: Optional[VolnuxCheckPointManager] = None,
        previous_result: Union[List[EventResult], EMPTY] = EMPTY,
        stop_condition: StopCondition = StopCondition.NEVER,
        run_bypass_event_checks: bool = False,
        options: Optional["Options"] = None,
        sequence_number: Optional[int] = None,
        command_channel: Optional["CommandChannelBase"] = None,
    ):
        """
        Initialize resource tracking and set up the event's required configurations. This method
        prepares the event for its execution by setting various attributes such as task-related
        details, previous results, stop conditions, and optional configurations.

        :param execution_context: The execution context within which the event operates.
        :type execution_context: ExecutionContext
        :param task_id: Unique identifier for the task associated with this event.
        :type task_id: str
        :param args: Additional positional arguments passed to the event setup.
        :type args: Tuple[Any]
        :param checkpoint_manager: Optional checkpoint manager for managing event states. If not
            provided, no checkpointing will be performed.
        :type checkpoint_manager: Optional[VolnuxCheckPointManager]
        :param previous_result: A list of previous event results or EMPTY if no prior results
            exist.
        :type previous_result: Union[List[EventResult], EMPTY]
        :param stop_condition: Condition under which event execution is terminated. Defaults
            to `StopCondition.NEVER`.
        :type stop_condition: StopCondition
        :param run_bypass_event_checks: Flag indicating if event checks should be bypassed.
            Defaults to False.
        :type run_bypass_event_checks: bool
        :param options: Optional configuration or settings for the event.
        :type options: Optional[Options]
        :param sequence_number: Optional sequence number indicating the order of the event.
        :type sequence_number: Optional[int]
        :return: None
        """

        # checkpointing resources
        self._external_resources: Dict[str, ResourceState] = {}
        self._resource_instances: Dict[str, object] = {}
        self._resource_monitor: ResourceMonitor = ResourceMonitor()

        # checkpointing sub-phase
        self._sub_phase_cache: Dict[str, Any] = {}

        self._execution_context = execution_context

        # Task ID
        self._task_id = task_id
        self._sequence_number = sequence_number

        # Configurations
        self.options = options

        # The previous result of the event, if any.
        self.previous_result = previous_result
        self.stop_condition = StopConditionProcessor(stop_condition=stop_condition)
        self.run_bypass_event_checks = run_bypass_event_checks

        # Retry configuration
        self._retry_count = 0
        self.init_retry()

        # The executor used to execute the event.
        self.exec_status: bool = False
        self.exec_result: Any = None

        self._init_args = get_function_call_args(self.__class__.__init__, locals())  # type: ignore
        self._init_args.pop("checkpoint_manager", None)
        self._call_args = EMPTY

        self._phase: EventPhase = EventPhase.INITIALIZED
        self.checkpoint_manager = checkpoint_manager

        # communication
        self._pause_gate = asyncio.Event()
        self._pause_gate.set()  # Default to 'Running'
        self._preempted: bool = False
        self._main_worker_task: Optional[asyncio.Task] = None
        self._command_channel: Optional["CommandChannelBase"] = command_channel

        event_init.emit(sender=self.__class__, event=self, init_kwargs=self._init_args)

    @phase_step(EventPhase.COMMUNICATING)
    async def _communicate(self: _BaseEvent, *args, **kwargs) -> None:
        """
        Execute the communicate() hook and merge its return value
        into the kwargs passed to subsequent steps.

        If communicate() suspends via request_human_input() or
        wait_for_event(), this phase is checkpointed. On resumption,
        _communicate runs again from the top of communicate() — which
        is correct because communicate() has no complex internal state.
        It is purely I/O: it requests something and waits. On the second
        run it finds the response already in previous_result and returns
        immediately without re-requesting.
        """
        await self._run_step(self.communicate, *args, **kwargs)

    @phase_step(EventPhase.PRE_PROCESS)
    async def _pre_process(self: _BaseEvent, *args, **kwargs):
        """
        Handles the pre-processing phase of an event within the event lifecycle. This method
        is executed with the intention of determining whether the event execution should proceed
        or be bypassed, based on custom conditions.

        The behavior of this method can be controlled by the `run_bypass_event_checks` attribute
        of the event instance. If this attribute is set to `True`, it attempts to validate event
        bypass prerequisites by calling the `can_bypass_current_event` method. If these checks
        indicate that the event can be skipped, the event execution completes successfully without
        proceeding further, and the bypass conditions are logged.

        :param args: Positional arguments are passed to the pre-processing handler.
        :type args: tuple
        :param kwargs: Keyword arguments passed to the pre-processing handler.
        :type kwargs: dict
        :return: If event bypass checks indicate skipping the event, returns the success
            result containing a dictionary with the bypass status, data, and status flag.
        :rtype: dict, optional
        """
        try:
            result = self.bypass()
            if asyncio.iscoroutine(result):
                result = await result
        except NotImplementedError:
            return
        except Exception as e:
            logger.error("Error in event bypass check: %s", str(e), exc_info=e)
            raise

        if result is None:
            raise EmptyResultError(self.__class__.__name__, method_name="bypass")

        success, data = self._process_result_tuple(result)

        self.exec_status = success
        self.exec_result = data

        logger.debug(
            f"Event '{self.__class__.__name__}' bypassed with " f"status={success}"
        )
        raise SkipExecutionError("skipped")

    @phase_step(EventPhase.PROCESSING)
    async def _process(self: _BaseEvent, *args, **kwargs):
        if self.retry_policy is None:
            self.exec_status, self.exec_result = await self._process_wrapper(
                *args, **kwargs
            )
        else:
            try:
                self.exec_status, exec_result = await self._retry(
                    self._process_wrapper, *args, **kwargs
                )
            except MaxRetryError as e:
                logger.error(str(e), exc_info=e.exception)
                self.exec_status, self.exec_result = False, e.exception
            except Exception as e:
                if not isinstance(e, SwitchTask):
                    logger.error(str(e), exc_info=e)
                self.exec_status, self.exec_result = False, e

    @phase_step(EventPhase.POST_PROCESS)
    def _post_process(self: _BaseEvent, *args, **kwargs) -> EventResult:
        result = (
            self.on_success(self.exec_result)
            if self.exec_status
            else self.on_failure(self.exec_result)
        )
        return result

    @phase_step(EventPhase.COMPLETED)
    async def _completed(self: _BaseEvent, *args, **kwargs):
        """Cleanup resources after completion."""
        for resource_name, resource_config in self._external_resources.items():
            provider_path = resource_config.get("provider_path")
            if provider_path:
                try:
                    provider_class = import_string(provider_path)
                    if issubclass(provider_class, ResourceProvider):
                        resource = self._resource_instances.get(resource_name)
                        if resource:
                            if not inspect.iscoroutinefunction(provider_class.cleanup):
                                await to_thread(provider_class.cleanup, resource)
                            else:
                                await provider_class.cleanup(resource)
                            logger.debug(f"Cleaned up resource '{resource_name}'")
                except Exception as e:
                    logger.warning(f"Failed to cleanup resource '{resource_name}': {e}")

        try:
            await self.cleanup(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=e)

        return None

    def _get_steps(self) -> List[Callable]:
        """
        Retrieves the list of internal processing steps for the object's lifecycle.

        :return: List of method references representing the sequence of internal steps.
        :rtype: list[Callable]
        """
        return [
            self._setup_event,
            self._pre_process,
            self._communicate,
            self._process,
            self._post_process,
            self._completed,
        ]

    async def _process_wrapper(
        self: _BaseEvent, *args, **kwargs
    ) -> Union[EventResult, Tuple[bool, Any]]:
        """
        Execute the process method of the event with provided arguments and handle
        results returned by the process method. Ensures that the result adheres
        to the required format.

        The process method can be either asynchronous or synchronous. If it's
        synchronous, it will be executed in a different thread using the to_thread
        utility.

        Validation of the process result ensures:
          - The result is not None.
          - The result is either an instance of EventResult or a tuple type.
          - If it's a tuple, the first element of the tuple must be a boolean.

        :param args: Positional arguments to be passed to the event's process method.
        :type args: Any
        :param kwargs: Keyword arguments to be passed to the event's process method.
        :type kwargs: Any
        :return: The result returned by the process method. This could be an instance
                 of EventResult or a tuple.
        :rtype: Union[EventResult, tuple]
        :raises ValueError: If the process result is None, does not adhere to the
                            required format, or if a tuple does not have a boolean
                            as its first element.
        """
        if inspect.iscoroutinefunction(self.process):
            result = await self.process(*args, **kwargs)
        else:
            result = await to_thread(self.process, *args, **kwargs)

        if result is None:
            raise EmptyResultError(event_class=self.__class__.__name__)

        return self._process_result_tuple(result)

    async def acquire_resource(
        self,
        name: str,
        provider: Union[str, Type[ResourceProvider]],
        init_args: dict,
        init_func: Optional[Callable[[dict], Any]] = None,
    ) -> Any:
        """
        Acquires a resource by its name, provider, and initialization data. If the resource
        is already managed, it attempts to restore it. Otherwise, it initializes a new
        resource using the specified provider or an initialization function.

        Example:
            >>> my_resource = await self.acquire_resource("my_resource", MyResourceProvider, {"param": "value"})
            >>> # Or using a function
            >>> my_resource = await self.acquire_resource("my_resource", MyResourceProvider, {"param": "value"}, init_func=lambda args: MyResource(args))

        :param name: The name of the resource to be acquired.
        :type name: str
        :param provider: Specifies the resource provider for the resource. It can be a string
            representing the provider path or a class of type ResourceProvider.
        :type provider: Union[str, Type[ResourceProvider]]
        :param init_args: A dictionary containing initialization parameters for the resource.
        :type init_args: dict
        :param init_func: Optional callable that initializes the resource. If provided,
            the resource will be created using this function.
        :type init_func: Optional[Callable[[dict], Any]]
        :return: The acquired resource instance.
        :rtype: Any
        """

        if name in self._external_resources:
            saved_state = self._external_resources[name]
            provider_path = saved_state.get("provider_path")
            if provider_path:
                try:
                    provider = cast(
                        Type[ResourceProvider], import_string(provider_path)
                    )
                    saved_state["provider_path"] = get_obj_klass_import_str(provider)
                except ImportError:
                    pass
            return await self._restore_resource(name, saved_state, bind=False)

        if init_func is not None:
            resource = init_func(init_args)
            if asyncio.iscoroutine(resource):
                resource = await resource

            await self._register_resource(name, resource, provider)
        else:
            saved_state: ResourceState = {
                "resource_name": name,
                "provider_path": (
                    get_obj_klass_import_str(provider)
                    if not isinstance(provider, str)
                    else provider
                ),
                "data": init_args,
            }
            resource = await self._restore_resource(name, saved_state, bind=False)

            self._external_resources[name] = saved_state

        # Start resource monitoring
        if not self._resource_monitor.is_started():
            await self._resource_monitor.start(self)

        return resource

    async def sub_phase(
        self,
        name: str,
        init_func: Callable[[], Any],
        *,
        suspend_on: Optional[Callable[[Any], bool]] = None,
        serialise: Optional[Callable[[Any], Any]] = None,
        deserialise: Optional[Callable[[Any], Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Execute a named computation block with caching, checkpointing, and optional suspension.

        Suspension Lifecycle
        --------------------
        1. First call: init_func runs → suspend_on(result) == True → cache pending value,
           checkpoint, raise SubPhaseSuspension.
        2. Resume (after external response updates cache via deliver_sub_phase_response):
           init_func is NOT re-executed → cached value found → suspend_on(cached) == False →
           return deserialised result.
        3. Resume (no external response yet): cached value found → suspend_on(cached) == True →
           re-suspend immediately without re-executing init_func.

        Parameters
        ----------
        name : str
            Stable, unique identifier for this computation block within the event.
        init_func : Callable[[], Any]
            The computation to execute. Sync or async.
        suspend_on : Callable[[Any], bool], optional
            Predicate evaluated against the init_func result (or cached value on resume).
            Return True to suspend; False/None to proceed normally.
            When provided, the sub-phase becomes a durable suspension point.
        serialise : Callable[[Any], Any], optional
            Transform result before caching/checkpointing. Required if result is not
            JSON-primitive. Paired with deserialise.
        deserialise : Callable[[Any], Any], optional
            Transform cached value back to domain object on retrieval.
        timeout : float, optional
            Max seconds for init_func execution. Sync functions are offloaded to
            _SUB_PHASE_EXECUTOR so asyncio.wait_for can interrupt them.

        Returns
        -------
        Any
            The computed (and optionally deserialised) result.

        Raises
        ------
        SubPhaseSuspension
            When suspend_on returns True. Caught by the engine to transition
            ExecutionRecord to SUSPENDED state.
        asyncio.TimeoutError
            When init_func exceeds timeout.
        """

        if name in self._sub_phase_cache:
            cached = self._sub_phase_cache[name]
            result = deserialise(cached) if deserialise is not None else cached

            if suspend_on is not None and suspend_on(result):
                # Still waiting — re-suspend WITHOUT re-executing init_func
                logger.debug(
                    "sub_phase %r: re-suspending from cache (event=%s)",
                    name,
                    self._task_id,
                )
                raise SubPhaseSuspension(name, result)

            # Cached and resolved — return immediately
            logger.debug(
                "sub_phase %r: cache hit — skipping init_func (event=%s)",
                name,
                self._task_id,
            )
            return result

        logger.debug(
            "sub_phase %r: executing init_func (event=%s)",
            name,
            self._task_id,
        )
        start_time = time.monotonic()

        try:
            raw = await self._execute_sub_phase_func(init_func, timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "sub_phase %r: timed out after %.1fs (event=%s)",
                name,
                timeout,
                self._task_id,
            )
            raise
        except Exception:
            # Failed executions are NEVER cached — will re-execute on next attempt
            raise

        elapsed = time.monotonic() - start_time
        logger.debug(
            "sub_phase %r: completed in %.2fs (event=%s)",
            name,
            elapsed,
            self._task_id,
        )

        if suspend_on is not None and suspend_on(raw):
            stored = serialise(raw) if serialise is not None else raw
            self._sub_phase_cache[name] = stored
            await self.enqueue_checkpoint()

            logger.info(
                "sub_phase %r: suspending — awaiting external resolution (event=%s)",
                name,
                self._task_id,
            )
            raise SubPhaseSuspension(name, raw)

        stored = serialise(raw) if serialise is not None else raw
        self._sub_phase_cache[name] = stored
        await self.enqueue_checkpoint()

        if serialise is not None and deserialise is not None:
            return deserialise(stored)
        return raw

    async def _execute_sub_phase_func(
        self, init_func: Callable[[], Any], timeout: Optional[float]
    ) -> Any:
        """Execute init_func with correct sync/async handling and timeout support."""
        is_async = inspect.iscoroutinefunction(
            init_func
        ) or inspect.iscoroutinefunction(getattr(init_func, "__wrapped__", None))

        if is_async:
            coro = init_func()
        elif timeout is not None:
            # Sync + timeout → offload to dedicated executor so wait_for can interrupt
            loop = asyncio.get_running_loop()
            coro = loop.run_in_executor(_SUB_PHASE_EXECUTOR, init_func)
        else:
            # Sync + no timeout → safe to run directly (fast path)
            return init_func()

        if timeout is not None:
            return await asyncio.wait_for(coro, timeout=timeout)
        return await coro

    # async def sub_phase(
    #     self,
    #     name: str,
    #     init_func: Callable[[], Any],
    #     *,
    #     serialise: Optional[Callable[[Any], Any]] = None,
    #     deserialise: Optional[Callable[[Any], Any]] = None,
    #     timeout: Optional[float] = None,
    # ) -> Any:
    #     """
    #     Execute a named computation block with automatic sub-phase checkpointing.
    #
    #     This function ensures efficient checkpointing and replay in a computation
    #     process. It can save execution states to avoid redundant computations on
    #     retries or process restarts. It supports both synchronous and asynchronous
    #     execution, with optional timeout enforcement and custom serialization.
    #
    #     Parameters:
    #     -----------
    #     :param name: The unique name of the computation block. It must be stable
    #         across code revisions and unique within a single execution of the
    #         parent process.
    #     :type name: str
    #     :param init_func: The initialization function for the computation.
    #         Can be synchronous or asynchronous.
    #     :type init_func: Callable[[], Any]
    #     :param serialise: Optional function for serializing the result of
    #         the computation block. Used for mutation consistency.
    #     :type serialise: Optional[Callable[[Any], Any]]
    #     :param deserialise: Optional function for deserializing the
    #         serialized result. Used for mutation consistency, typically paired
    #         with `serialise`.
    #     :type deserialise: Optional[Callable[[Any], Any]]
    #     :param timeout: Optional timeout in seconds. If provided, the computation
    #         will be interrupted if it exceeds the specified timeout.
    #     :type timeout: Optional[float]
    #
    #     Returns:
    #     --------
    #     :return: The result of the computation, either retrieved from the cache
    #         or freshly computed. If both `serialise` and `deserialise` are provided,
    #         the deserialized value is returned for consistent object identity.
    #     :rtype: Any
    #
    #     Raises:
    #     -------
    #     :raises asyncio.TimeoutError: If the computation exceeds the given timeout.
    #     :raises Exception: If there is an exception during the execution of
    #         `init_func`. Such exceptions will not cache the result and the
    #         computation will retry the next time it's invoked.
    #     """
    #     cache = self._sub_phase_cache.get(name, {})
    #
    #     if name in cache:
    #         cached = cache[name]
    #         result = deserialise(cached) if deserialise is not None else cached
    #         logger.debug(
    #             "sub_phase %r: cache hit — skipping init_func (event=%s)",
    #             name,
    #             self._task_id,
    #         )
    #         return result
    #
    #     logger.debug(
    #         "sub_phase %r: executing init_func (event=%s)", name, self._task_id
    #     )
    #     start_time = time.monotonic()
    #
    #     try:
    #         # Determine whether to call on event loop or offload to thread.
    #         # Async functions and lambdas returning coroutines are called directly.
    #         # Sync functions with a timeout are offloaded, so wait_for can interrupt.
    #         is_async_fn = inspect.iscoroutinefunction(
    #             init_func
    #         ) or inspect.iscoroutinefunction(getattr(init_func, "__wrapped__", None))
    #
    #         if is_async_fn or timeout is None:
    #             raw = init_func()
    #         else:
    #             loop = asyncio.get_running_loop()
    #             raw = loop.run_in_executor(None, init_func)
    #
    #         if inspect.isawaitable(raw):
    #             if timeout is not None:
    #                 raw = await asyncio.wait_for(raw, timeout=timeout)
    #             else:
    #                 raw = await raw
    #
    #     except asyncio.TimeoutError:
    #         logger.warning(
    #             "sub_phase %r: timed out after %.1fs (event=%s)",
    #             name,
    #             timeout,
    #             self._task_id,
    #         )
    #         raise
    #
    #     except Exception:
    #         # Failed executions are never cached — re-execute on retry
    #         raise
    #
    #     elapsed = time.monotonic() - start_time
    #     logger.debug(
    #         "sub_phase %r: completed in %.2fs (event=%s)", name, elapsed, self._task_id
    #     )
    #
    #     stored = serialise(raw) if serialise is not None else raw
    #     self._sub_phase_cache[name] = stored
    #     await self.enqueue_checkpoint()
    #
    #     if serialise is not None and deserialise is not None:
    #         return deserialise(stored)
    #     return raw

    async def _register_resource(
        self,
        resource_name: str,
        resource: Any,
        provider: Union[str, Type[ResourceProvider]],
    ) -> None:
        """
        Register an external resource for checkpointing.

        The provider must be a subclass of ResourceProvider implementing:
        - save_state(resource) -> dict: Serialize resource state
        - restore_state(data: dict) -> resource: Recreate resource from state
        - cleanup(resource) (optional): Clean up resource

        Args:
            resource_name: Unique identifier for this resource
            resource: The resource object to checkpoint
            provider: Either:
                - Import path string (e.g., "myapp.FileProvider")
                - ResourceProvider class directly

        Raises:
            TypeError: If the provider is not a ResourceProvider subclass
            ValueError: If the provider doesn't implement required methods
        """
        try:
            if isinstance(provider, str):
                provider_class = import_string(provider)
                provider_path = provider
            else:
                provider_class = provider
                provider_path = f"{provider.__module__}.{provider.__name__}"

            if not issubclass(provider_class, ResourceProvider):
                raise TypeError(
                    f"Provider must be a subclass of ResourceProvider, "
                    f"got {type(provider_class)}"
                )

            if not hasattr(provider_class, "save_state"):
                raise ValueError(
                    f"Provider {provider_class.__name__} must implement save_state() method"
                )

            if not hasattr(provider_class, "restore_state"):
                raise ValueError(
                    f"Provider {provider_class.__name__} must implement restore_state() method"
                )

            resource_data = provider_class.save_state(resource)
            if asyncio.iscoroutine(resource_data):
                resource_data = await resource_data

            if not isinstance(resource_data, dict):
                raise ValueError(
                    f"Provider save_state() must return a dict, "
                    f"got {type(resource_data)}"
                )

            self._external_resources[resource_name] = {
                "resource_name": resource_name,
                "data": resource_data,
                "provider_path": provider_path,
            }
            self._resource_instances[resource_name] = resource

            logger.debug(
                f"Registered resource '{resource_name}' with provider {provider_class.__name__}"
            )

        except ImportError as e:
            logger.error(f"Failed to import provider {provider}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to register resource '{resource_name}': {e}")
            raise

    async def _restore_resource(
        self, resource_name: str, resource_config: "ResourceState", bind: bool = True
    ) -> object:
        """
        Restore an external resource from checkpoint data.

        This method is called automatically during event resumption.

        Args:
            resource_name: Name of the resource to restore
            resource_config: ResourceState dict with restoration data
            bind: Whether to bind the restored resource to the event
        Return:
            Restored resource object
        """
        try:
            provider_path = resource_config.get("provider_path")
            if not provider_path:
                logger.warning(
                    f"No provider_path for resource '{resource_name}', skipping restoration"
                )
                return None

            provider_class = import_string(provider_path)

            if not issubclass(provider_class, ResourceProvider):
                raise TypeError(
                    f"Provider {provider_path} is not a ResourceProvider subclass"
                )

            resource_data = resource_config.get("data", {})
            restored_resource = provider_class.restore_state(resource_data)

            if asyncio.iscoroutine(restored_resource):
                restored_resource = await restored_resource

            if bind:
                setattr(self, f"{resource_name}", restored_resource)

            logger.info(
                f"Restored resource '{resource_name}' using provider {provider_class.__name__}"
            )

            self._resource_instances[resource_name] = restored_resource

            return restored_resource

        except ImportError as e:
            logger.error(
                f"Failed to import provider for resource '{resource_name}': {e}",
                exc_info=True,
            )
            # Don't raise - allow event to continue without this resource
        except Exception as e:
            logger.error(
                f"Failed to restore resource '{resource_name}': {e}", exc_info=True
            )

    def get_phase(self) -> EventPhase:
        return self._phase

    async def enqueue_checkpoint(self) -> None:
        if self.checkpoint_manager is None:
            # logger.warning("Checkpoint manager is not available. Skipping enqueue.")
            # Enforce checkpointing if configured
            if conf.get("CHECKPOINT_REQUIRED", default=False):
                raise RuntimeError(
                    "Checkpointing is required but manager is unavailable"
                )
            return

        try:
            self.checkpoint_manager.enqueue(await self.create_snapshot())
        except Exception as e:
            logger.error(
                "Failed to create or enqueue checkpoint for task_id=%s: %s",
                self._task_id,
                str(e),
                exc_info=e,
            )
            # Re-raise if checkpointing is critical
            if conf.get("CHECKPOINT_REQUIRED", default=False):
                raise

    async def create_snapshot(self) -> EventCheckpointSnapshot:
        return await SnapshotBuilder().build(self)
