import abc
import logging
import inspect
import multiprocessing as mp
import time
import typing
from asgiref.sync import async_to_sync
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum

from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.parser.options import Options, StopCondition
from volnux.result_evaluators import (
    EventEvaluator,
    ExecutionResultEvaluationStrategyBase,
    ResultEvaluationStrategies,
)
from volnux.signal.signals import (
    event_called,
    event_execution_retry,
    event_execution_retry_done,
    event_init,
)
from volnux.versioning.handler import VersionHandler
from volnux.versioning import BaseVersioning, NoVersioning, DeprecationInfo, VersionInfo

from .conf import ConfigLoader
from .constants import EMPTY, MAX_BACKOFF, MAX_BACKOFF_FACTOR, MAX_RETRIES
from .exceptions import (
    ImproperlyConfigured,
    MaxRetryError,
    StopProcessingError,
    SwitchTask,
)
from .executors.default_executor import DefaultExecutor
from .executors.remote_executor import RemoteExecutor
from .registry import Registry
from .result import EventResult, ResultSet
from .utils import get_function_call_args

__all__ = [
    "EventBase",
    "RetryPolicy",
    "ExecutorInitializerConfig",
    "EventType",
    "get_event_registry",
]


logger = logging.getLogger(__name__)

conf = ConfigLoader.get_lazily_loaded_config()

_event_registry = Registry()


if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext
    from volnux.signal.handlers.event_initialiser import ExtraEventInitKwargs


def get_event_registry():
    """Singleton for event registry"""
    return _event_registry


class EventType(Enum):
    SYSTEM = "system"  # internal system events
    META = "meta"  # meta events
    OTHER = "other"


class EventMeta(abc.ABCMeta):
    """
    Metaclass that registers event classes at creation time.
    """

    def __new__(mcs, name, bases, namespace, **kwargs):
        """
        Called when a new class is created.
        Automatically registers the class with the global registry.
        """
        cls = super().__new__(mcs, name, bases, namespace)

        # Register it if it's not the base EventBase class
        if name not in ["EventBase", "ControlFlowEvent"] and any(
            isinstance(base, EventMeta) for base in bases
        ):
            try:
                # Get version info using the versioning scheme
                event_class = typing.cast(typing.Type[EventBase], cls)
                versioning = event_class.get_version_handler()
                version_info = versioning.get_info()

                event_name = versioning.scheme.get_event_name(cls)

                _event_registry.register(
                    event_class,
                    name=event_name,
                    namespace=version_info['namespace'],
                    version=version_info['version'],
                    changelog=version_info.get('changelog'),
                    deprecated=version_info.get('deprecated', False),
                    deprecation_info=version_info.get('deprecation_info'),
                    scheme_handler=versioning.scheme,
                    event_type=getattr(cls, "event_type", EventType.OTHER),
                )

                # Log registration
                status = "DEPRECATED" if version_info.get('deprecated') else "active"
                logger.debug(
                    f"Registered: {cls.__module__}.{name} as "
                    f"{version_info['namespace']}::{event_name}@{version_info['version']} "
                    f"[{status}]"
                )
            except RuntimeError as e:
                logger.warning(str(e))

        return cls


class RetryConfigDict(typing.TypedDict, total=False):
    max_attempts: int
    backoff_factor: float
    max_backoff: float
    retry_on_exceptions: typing.List[typing.Type[Exception]]


@dataclass
class RetryPolicy:
    max_attempts: int = field(
        init=True, default=conf.get("MAX_EVENT_RETRIES", default=MAX_RETRIES)
    )
    backoff_factor: float = field(
        init=True,
        default=conf.get("MAX_EVENT_BACKOFF_FACTOR", default=MAX_BACKOFF_FACTOR),
    )
    max_backoff: float = field(
        init=True, default=conf.get("MAX_EVENT_BACKOFF", default=MAX_BACKOFF)
    )
    retry_on_exceptions: typing.List[typing.Type[Exception]] = field(
        default_factory=list
    )


class _RetryMixin:
    retry_policy: typing.Union[
        typing.Optional[RetryPolicy], typing.Dict[str, typing.Any], None
    ] = None

    def __init__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        self._retry_count = 0
        super().__init__(*args, **kwargs)

    def get_retry_policy(self) -> typing.Union[RetryPolicy, None]:
        if isinstance(self.retry_policy, dict):
            self.retry_policy = RetryPolicy(**self.retry_policy)
        return self.retry_policy

    def config_retry_policy(
        self,
        max_attempts: int,
        backoff_factor: float = MAX_BACKOFF_FACTOR,
        max_backoff: float = MAX_BACKOFF,
        retry_on_exceptions: typing.Union[
            typing.List[typing.Type[Exception]], typing.Type[Exception], None
        ] = None,
    ) -> None:
        """
        Configures the retry policy for the event.
        Args:
            max_attempts (int): Maximum number of retry attempts.
            backoff_factor (float): Factor for calculating backoff time.
            max_backoff (float): Maximum backoff time.
            retry_on_exceptions (Union[Tuple[Type[Exception]], Type[Exception], None]): Exceptions that trigger a retry.
        Returns:
            None
        """
        config: RetryConfigDict = {
            "max_attempts": max_attempts,
            "backoff_factor": backoff_factor,
            "max_backoff": max_backoff,
            "retry_on_exceptions": [],
        }
        if retry_on_exceptions:
            retry_exceptions: typing.Sequence[typing.Type[Exception]] = (
                retry_on_exceptions
                if isinstance(retry_on_exceptions, (tuple, list))
                else [retry_on_exceptions]
            )
            config["retry_on_exceptions"].extend(retry_exceptions)

        self.retry_policy = RetryPolicy(**config)

    def get_backoff_time(self) -> float:
        if self.retry_policy is None or self._retry_count <= 1:
            return 0

        backoff_value = self.retry_policy.backoff_factor * (
            2 ** (self._retry_count - 1)
        )

        return typing.cast(float, min(backoff_value, self.retry_policy.max_backoff))

    def _sleep_for_backoff(self) -> float:
        backoff = self.get_backoff_time()
        if backoff <= 0:
            return 0
        time.sleep(backoff)
        return backoff

    def is_retryable(self, exception: Exception) -> bool:
        if self.retry_policy is None:
            return False
        exception_evaluation = not self.retry_policy.retry_on_exceptions or any(
            [
                isinstance(exception, exc)
                and exception.__class__.__name__ == exc.__name__
                for exc in self.retry_policy.retry_on_exceptions
                if exc
            ]
        )
        return isinstance(exception, Exception) and exception_evaluation

    def is_exhausted(self) -> bool:
        return (
            self.retry_policy is None
            or self._retry_count >= self.retry_policy.max_attempts
        )

    def retry(
        self,
        func: typing.Callable[[typing.Any], typing.Tuple[bool, typing.Any]],
        /,
        *args: typing.Tuple[typing.Any],
        **kwargs: typing.Dict[str, typing.Any],
    ) -> typing.Tuple[bool, typing.Any]:
        if self.retry_policy is None:
            return func(*args, **kwargs)

        exception_causing_retry = None

        while True:
            if self.is_exhausted():
                event_execution_retry_done.emit(
                    sender=self._execution_context.__class__,
                    event=self,
                    execution_context=self._execution_context,
                    task_id=self._task_id,
                    max_attempts=self.retry_policy.max_attempts,
                )

                raise MaxRetryError(
                    attempt=self._retry_count,
                    exception=exception_causing_retry,
                    reason="Retryable event is already exhausted: actual error:{reason}".format(
                        reason=str(exception_causing_retry)
                    ),
                )

            logger.info(
                "Retrying event {}, attempt {}...".format(
                    self.__class__.__name__, self._retry_count
                )
            )

            try:
                self._retry_count += 1
                if inspect.iscoroutinefunction(func):
                    return async_to_sync(func)(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except MaxRetryError:
                # ignore this
                break
            except Exception as exc:
                if self.is_retryable(exc):
                    if exception_causing_retry is None:
                        exception_causing_retry = exc
                    back_off = self._sleep_for_backoff()

                    event_execution_retry.emit(
                        sender=self._execution_context.__class__,
                        event=self,
                        backoff=back_off,
                        retry_count=self._retry_count,
                        max_attempts=self.retry_policy.max_attempts,
                        execution_context=self._execution_context,
                        task_id=self._task_id,
                    )
                    continue
                raise

        return False, None


class _ExecutorInitializerMixin:
    executor: typing.Type[Executor] = DefaultExecutor

    executor_config: typing.Optional[ExecutorInitializerConfig] = None

    def __init__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        super().__init__(*args, **kwargs)

        self.get_executor_initializer_config()

    @classmethod
    def get_executor_class(cls) -> typing.Type[Executor]:
        return cls.executor

    def get_executor_initializer_config(self) -> ExecutorInitializerConfig:
        if self.executor_config:
            if isinstance(self.executor_config, dict):
                self.executor_config = ExecutorInitializerConfig.from_dict(
                    self.executor_config
                )
        else:
            self.executor_config = ExecutorInitializerConfig()
        return self.executor_config

    def is_multiprocessing_executor(self) -> bool:
        """Check if using multiprocessing or remote executor"""
        return (
            self.get_executor_class() == ProcessPoolExecutor
            or self.get_executor_class() == RemoteExecutor
        )

    def get_executor_context(
        self, ctx: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> typing.Dict[str, typing.Any]:
        """
        Retrieves the execution context for the event's executor.

        This method determines the appropriate execution context (e.g., multiprocessing context)
        based on the executor class used for the event. If the executor is configured to use
        multiprocessing, the context is set to "spawn". Additionally, any parameters required
        for the executor's initialization are fetched and added to the context.

        The resulting context dictionary is used to configure the executor for the event execution.

        Returns:
            dict: A dictionary containing the execution context for the event's executor,
                  including any necessary parameters for initialization and multiprocessing context.

        """
        executor = self.get_executor_class()
        context = dict()
        if self.is_multiprocessing_executor():
            context["mp_context"] = mp.get_context("spawn")
        elif hasattr(executor, "get_context"):
            context["mp_context"] = executor.get_context("spawn")  # type: ignore
        params = get_function_call_args(
            executor.__init__, self.get_executor_initializer_config()
        )
        context.update(params)
        if ctx and isinstance(ctx, dict):
            context.update(ctx)
        return context


@dataclass
class StopConditionProcessor:
    """
    Processor for handling stop conditions with improved error handling and flexibility.
    Attributes:
        stop_condition: The condition that determines when to stop processing
        exception: Any exception that occurred during processing
        message: Optional message for logging or debugging
        logger: Optional logger instance for structured logging
    """

    stop_condition: typing.Union[StopCondition, typing.List[StopCondition]]
    exception: typing.Optional[Exception] = None
    message: typing.Optional[str] = None
    logger: typing.Optional[logging.Logger] = None

    def __post_init__(self) -> None:
        """Validate initialization parameters."""
        if not self.logger:
            self.logger = logging.getLogger(__name__)

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
            self.logger.warning(f"Unknown stop condition: {condition}")
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

    def on_error(
        self, exception: Exception, message: typing.Optional[str] = None
    ) -> bool:
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
            self.logger.info(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )
        else:
            self.logger.debug(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )

    def get_status(self) -> typing.Dict[str, typing.Any]:
        """Get current processor status for debugging."""
        return {
            "stop_condition": self.stop_condition,
            "has_exception": self.exception is not None,
            "exception_type": type(self.exception).__name__ if self.exception else None,
            "message": self.message,
        }


class EventBase(_RetryMixin, _ExecutorInitializerMixin, metaclass=EventMeta):
    """
    Abstract base class for event in the pipeline system.

    This class serves as a base for event-related tasks and defines common
    properties for event execution, which can be customized in subclasses.

    Class Attributes:
        executor (Type[Executor]): The executor type used to handle event execution.
                                    Defaults to DefaultExecutor.
        executor_config (ExecutorInitializerConfig): Configuration settings for the executor.
                                                    Defaults to None.
         result_evaluation_strategy(ExecutionResultEvaluationStrategyBase): The strategy to use in evaluating the
                                    results of the execution of this event in the pipeline. This will inform
                                    the pipeline as to the next execution path to take.

    Result Evaluation Strategies:
        ALL_MUST_SUCCEED/AllTasksMustSucceedStrategy: The event is considered successful only if all the tasks within the event
                                        succeeded. If any task fails, the evaluation should be marked as a failure.

        NO_FAILURES_ALLOWED/NoFailuresAllowedStrategy: The event is considered a failure if any of the tasks fail. Even if some tasks
                                    succeed, a failure in any one task results in the event being considered a failure.

        ANY_MUST_SUCCEED/AnyTaskMustSucceedStrategy: The event is considered successful if at least one of the tasks succeeded.
                                    This means that if any task succeeds, the event will be considered successful,
                                    even if others fail.

        MAJORITY_MUST_SUCCEED: Event succeeds if a majority of tasks succeed

    Subclasses must implement the `process` method to define the logic for
    processing pipeline data.
    """

    # Version configuration
    versioning_class: typing.Type[BaseVersioning] = NoVersioning
    version: str = "1.0.0"
    changelog: typing.Optional[str] = None
    deprecated: bool = False
    deprecation_info: typing.Optional[DeprecationInfo] = None

    _version_handler: typing.Optional[VersionHandler] = None

    # Custom name and namespace
    namespace: str = "local"
    name: typing.Optional[str] = None

    # The event types
    event_type: EventType = EventType.OTHER

    # how we want the execution results of this event to be evaluated by the pipeline
    result_evaluation_strategy: ExecutionResultEvaluationStrategyBase = (
        ResultEvaluationStrategies.ALL_MUST_SUCCEED
    )

    # The schema for adding extra event initialization arguments without modify the __init__
    # It uses event signal 'event_init' to hook an initializer function.
    # That will handle the setting and validation of the extra event init args
    EXTRA_INIT_PARAMS_SCHEMA: typing.Dict[str, "ExtraEventInitKwargs"] = {}

    def __init_subclass__(cls, **kwargs: typing.Dict[str, typing.Any]) -> None:
        # prevent the overriding of __init__
        if cls.__name__ != "EventBase":
            for attr_name in ["__init__"]:
                if attr_name in cls.__dict__:
                    raise PermissionError(
                        f"Model '{cls.__name__}' cannot override {attr_name!r}. "
                        f"Consider registering a 'listener' function for the signal 'event_init' "
                        f"for all your custom initialization. Also, you can configure the EXTRA_INIT_PARAMS_SCHEMA "
                        f"class variable and the runtime will take care of the initialisation of your custom arguments"
                        f"You can also handle initialisation within the 'process' method"
                    )

        super().__init_subclass__(**kwargs)

    def __init__(
        self,
        execution_context: "ExecutionContext",
        task_id: str,
        *args: typing.Tuple[typing.Any],
        previous_result: typing.Union[typing.List[EventResult], EMPTY] = EMPTY,
        stop_condition: StopCondition = StopCondition.NEVER,
        run_bypass_event_checks: bool = False,
        options: typing.Optional["Options"] = None,
        sequence_number: typing.Optional[int] = None,
        **kwargs: typing.Dict[str, typing.Any],
    ) -> None:
        """
        Initializes an EventBase instance with the provided execution context and configuration.

        This constructor is used to set up the event with the necessary context for execution,
        as well as optional configuration for handling previous results and exceptions.

        Args:
            execution_context (EventExecutionContext): The context in which the event will be executed,
                                                      providing access to execution-related data.
            task_id (str): The PipelineTask for this event.
            previous_result (Any, optional): The result of the previous event execution.
                                              Default to `EMPTY` if not provided.
            stop_on_exception (bool, optional): Flag to indicate whether the event should stop execution
                                                 if an exception occurs. Defaults to `False`.
            stop_on_success (bool, optional): Flag to indicate whether the event should stop execution
                                          if it is successful. Defaults to `False`.
            stop_on_error (bool, optional): Flag to indicate whether the event should stop execution
                                        if an error occurs. Defaults to `False`.
            options (Options, optional): Additional options to pass to the event constructor.
                                        This is automatically assigned at run time
            sequence_number (int, optional): Sequence number of the event. Defaults to `None`.

        """
        super().__init__(*args, **kwargs)

        self._execution_context = execution_context
        self._task_id = task_id
        self._sequence_number = sequence_number
        self.options = options
        self.previous_result = previous_result
        self.stop_condition = StopConditionProcessor(
            stop_condition=stop_condition, logger=logger
        )
        self.run_bypass_event_checks = run_bypass_event_checks

        self.get_retry_policy()  # config retry if error

        self._init_args = get_function_call_args(self.__class__.__init__, locals())  # type: ignore
        self._call_args = EMPTY

        event_init.emit(sender=self.__class__, event=self, init_kwargs=self._init_args)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} executor={self.executor.__name__}>"

    def get_init_args(self) -> typing.Dict[str, typing.Any]:
        return self._init_args

    def get_call_args(self) -> typing.Dict[str, typing.Any]:
        return self._call_args  # type: ignore

    @classmethod
    def get_version_handler(cls) -> VersionHandler:
        """Get version handler for this class"""
        if cls._version_handler is None:
            cls._version_handler = VersionHandler.from_class(
                cls, config_key="DEFAULT_EVENT_VERSIONING"
            )
        return cls._version_handler

    @classmethod
    def get_version_info(cls) -> VersionInfo:
        """Get version information by delegating to handler."""
        return cls.get_version_handler().get_info()

    @classmethod
    def is_deprecated(cls) -> bool:
        """Check if deprecated by delegating to handler."""
        return cls.get_version_handler().is_deprecated()

    @classmethod
    def get_all_versions(cls, name: typing.Optional[str] = None) -> typing.List[str]:
        """Get all versions from registry."""
        handler = cls.get_version_handler()
        event_name = name or handler.class_name
        return _event_registry.list_versions(event_name, handler.namespace)

    @classmethod
    def get_latest_version(
        cls, name: typing.Optional[str] = None
    ) -> typing.Optional[str]:
        """Get the latest version from registry."""
        handler = cls.get_version_handler()
        event_name = name or handler.class_name
        return _event_registry.get_latest_version(event_name, handler.namespace)

    def goto(
        self,
        descriptor: int,
        result_success: bool,
        result: typing.Any,
        reason: typing.Literal["manual"] = "manual",
        execute_on_event_method: bool = True,
    ) -> None:
        """
        Transitions to the new sub-child of a parent task with the given descriptor
        while optionally processing the result.
        Args:
            descriptor (int): The identifier of the next task to switch to.
            result_success (bool): Indicates if the current task succeeded or failed.
            result (typing.Any): The result data to pass to the next task.
            reason (str, optional): Reason for the task switch. Defaults to "manual".
            execute_on_event_method (bool, optional): If True, processes the result via
                success/failure handlers; otherwise, wraps it in `EventResult`.
        Raises:
            ValueError: If the descriptor is not an integer between 0 and 9.
            SwitchTask: Always raised to signal the task switch.
        """
        if not isinstance(descriptor, int):
            raise ValueError("Descriptor must be an integer between 0 to 9")

        if execute_on_event_method:
            if result_success:
                res = self.on_success(result)
            else:
                res = self.on_failure(result)
        else:
            res = EventResult(
                error=not result_success, # type: ignore
                content=result, # type: ignore
                task_id=self._task_id, # type: ignore
                event_name=self.__class__.__name__, # type: ignore
                call_params=self._call_args, # type: ignore
                init_params=self._init_args,
            )
        raise SwitchTask(
            current_task_id=self._task_id,
            next_task_descriptor=descriptor,
            result=res,
            reason=reason,
        )

    @classmethod
    def evaluator(cls) -> EventEvaluator:
        """
        Get the event evaluator for the current task.
        Return:
            Evaluator for the current task.
        Raises:
            ImproperlyConfigured: If no valid result evaluation strategy is specified.
        """
        if cls.result_evaluation_strategy is None:
            raise ImproperlyConfigured("No result evaluation strategy specified")
        if not isinstance(
            cls.result_evaluation_strategy, ExecutionResultEvaluationStrategyBase
        ):
            raise ImproperlyConfigured(
                f"'{cls.__name__}' is not a valid result evaluation strategy"
            )
        return EventEvaluator(cls.result_evaluation_strategy)

    def can_bypass_current_event(self) -> typing.Tuple[bool, typing.Any]:
        """
        Determines if the current event execution can be bypassed, allowing pipeline
        processing to continue to the next event regardless of validation or execution failures.

        This method evaluates custom bypass conditions defined for this specific event.
        When it returns True, the pipeline will skip the current event's execution
        and proceed to the next event in the sequence. When False, normal execution
        and error handling will occur.

        The bypass decision is typically based on business rules such as:
        - Event is optional in certain contexts
        - Alternative processing path exists
        - Specific data conditions make this event unnecessary

        Returns (Tuple):
            bool: True if the event can be bypassed, False if normal execution should occur
            data: Result data to pass to the next event.
        Example:
            In a shipping pipeline, certain validation steps might be bypassed
            for internal transfers while being required for external shipments.
        """
        return False, None

    @abc.abstractmethod
    def process(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> typing.Tuple[bool, typing.Any]:
        """
        Processes pipeline data and executes the associated logic.

        This method must be implemented by any class inheriting from EventBase.
        It defines the logic for processing pipeline data, taking in any necessary
        arguments, and returning a tuple containing:
            - A boolean indicating the success or failure of the processing.
            - The result of the processing, which could vary based on the event logic.

        Returns:
            A tuple (success_flag, result), where:
                - success_flag (bool): True if processing is successful, False otherwise.
                - result (Any): The output or result of the processing, which can vary.
        """
        raise NotImplementedError()

    def event_result(
        self, error: bool, content: typing.Dict[str, typing.Any]
    ) -> EventResult:
        return EventResult(
            error=error,  # type: ignore
            task_id=self._task_id,  # type: ignore
            order=self._sequence_number,  # type: ignore
            event_name=self.__class__.__name__,  # type: ignore
            content=content,
            call_params=self.get_call_args(),
            init_params=self.get_init_args(),
        )  # type: ignore

    def on_success(self, execution_result: typing.Any) -> EventResult:
        self.stop_condition.message = execution_result

        event_called.emit(
            sender=self.__class__,
            event=self,
            init_args=self.get_init_args(),
            call_args=self.get_call_args(),
            hook_type="on_success",
            result=execution_result,
        )

        if self.stop_condition.on_success():
            raise StopProcessingError(
                message=execution_result,
                exception=None,
                stop_condition=self.stop_condition,
                params={
                    "init_args": self._init_args,
                    "call_args": self._call_args,
                    "sequence_number": self._sequence_number,
                    "event_name": self.__class__.__name__,
                    "task_id": self._task_id,
                },
            )

        return self.event_result(False, execution_result)

    def on_failure(self, execution_result: typing.Any) -> EventResult:
        """
        Handles failure scenarios during event execution.
        Args:
            execution_result: The result or exception from the failed execution.
        Returns:
            EventResult: The wrapped result indicating failure.
        Raises:
            StopProcessingError: If the stop condition dictates to halt processing.
        """
        event_called.emit(
            sender=self.__class__,
            event=self,
            init_args=self.get_init_args(),
            call_args=self.get_call_args(),
            hook_type="on_failure",
            result=execution_result,
        )

        if isinstance(execution_result, Exception):
            execution_result = (
                getattr(execution_result, "exception", execution_result)
                if execution_result.__class__ == MaxRetryError
                else execution_result
            )

        if self.stop_condition.on_error(
            exception=execution_result,
            message=f"Error occurred while processing event '{self.__class__.__name__}'",
        ):
            raise StopProcessingError(
                message=self.stop_condition.message,  # type: ignore
                exception=execution_result,
                params={
                    "init_args": self._init_args,
                    "call_args": self._call_args,
                    "event_name": self.__class__.__name__,
                    "task_id": self._task_id,
                },
            )

        return self.event_result(True, execution_result)

    @classmethod
    def get_all_event_classes(cls) -> typing.FrozenSet[typing.Type["EventBase"]]:
        """
        return all registered event classes.
        """
        return _event_registry.list_all_classes()  # type:ignore

    @classmethod
    def get_direct_subclasses(cls) -> typing.Set[typing.Type["EventBase"]]:
        """Get only direct subclasses (one level down)"""
        return set(cls.__subclasses__())

    @classmethod
    def clear_class_cache(cls) -> None:
        """Clear the cached subclass registry"""
        _event_registry.clear()

    def __call__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> EventResult:
        self._call_args = get_function_call_args(self.__class__.__call__, locals())  # type: ignore

        if self.run_bypass_event_checks:
            try:
                should_skip, data = self.can_bypass_current_event()
            except Exception as e:
                logger.error(
                    "Error in event setup status checks: %s", str(e), exc_info=e
                )
                raise

            if should_skip:
                execution_result = {
                    "status": 1,
                    "skip_event_execution": should_skip,
                    "data": data,
                }
                return self.on_success(execution_result)

        try:
            self._execution_status, execution_result = self.retry(
                self.process, *args, **kwargs
            )
        except MaxRetryError as e:
            logger.error(str(e), exc_info=e.exception)
            return self.on_failure(e)
        except Exception as e:
            if not isinstance(e, SwitchTask):
                logger.error(str(e), exc_info=e)
            return self.on_failure(e)
        if self._execution_status:
            return self.on_success(execution_result)
        return self.on_failure(execution_result)
