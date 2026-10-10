import abc
import logging
import typing
import asyncio
from enum import Enum

from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.parser.options import Options, StopCondition
from volnux.result_evaluators import (
    EventEvaluator,
    ExecutionResultEvaluationStrategyBase,
    ResultEvaluationStrategies,
)
from volnux.signal.signals import event_called
from volnux.versioning.handler import VersionHandler
from volnux.versioning import BaseVersioning, NoVersioning, DeprecationInfo, VersionInfo

from volnux.config import VolnuxConfig
from volnux.constants import EMPTY
from volnux.exceptions import (
    ImproperlyConfigured,
    MaxRetryError,
    StopProcessingError,
    SwitchTask,
)
from volnux.event.registry import Registry
from volnux.result import EventResult
from volnux.mixins.event import (
    RetryMixin,
    RetryPolicy,
    ExecutorInitializerMixin,
    ExecutorInitializerConfig,
    EventCheckpointingMixin,
    EventCommandMixin,
    ExternalCommunicationMixin,
)
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager

__all__ = [
    "EventBase",
    "RetryPolicy",
    "ExecutorInitializerConfig",
    "EventType",
    "get_event_registry",
]


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()

_event_registry = Registry()


if typing.TYPE_CHECKING:
    from volnux.flows.bridge.communications.tasks.channels.base import (
        CommandChannelBase,
    )
    from volnux.execution.context import ExecutionContext
    from volnux.signal.handlers.event_initialiser import EventInitKwargs


def get_event_registry():
    """Singleton for event registry"""
    return _event_registry


class EventType(Enum):
    """
    Enumeration of event types.

    :ivar SYSTEM: Internal system events.
    :type SYSTEM: EventType
    :ivar META: Metadata-related events.
    :type META: EventType
    :ivar OTHER: General events do not fall into specific predefined categories.
    :type OTHER: EventType
    """

    SYSTEM = "system"  # internal system events
    META = "meta"  # meta events
    OTHER = "other"


class EventCategory(str, Enum):
    """
        Semantic classification for :class:`EventBase` subclasses.

        Set ``category`` on your event class to make it discoverable in the
        registry, CLI tooling, and generated documentation::

            class MyEvent(EventBase):
                category = EventCategory.DATABASE

        Choose the category that best describes the *domain* of the event, not
        its execution role (that is :class:`EventType`).

        Members
        -------
        EXTRACT
            Pulling data from an external source (files, APIs, databases, streams).
        TRANSFORM
            Reshaping, enriching, or converting data.
        LOAD
            Writing processed data to a destination (warehouse, database, storage).
        INGEST
            Streaming or continuous data ingestion (Kafka, CDC, webhooks, etc.).
            Prefer ``EXTRACT`` for one-shot batch reads.
            ETL categories do not cover DATABASE
    Relational / NoSQL database operations.
        HTTP
            Outbound HTTP / REST / GraphQL calls.
        MESSAGING
            Pub/sub and queue consumers (Redis Streams, RabbitMQ, SQS, etc.).
        FILE
            Local or remote file-system operations (read, write, upload, download).
        CACHE
            Cache reads, writes, and invalidation events.
        VALIDATE
            Data validation, schema checks, and quality-gate steps.
        ANALYZE
            Statistical analysis, reporting, and aggregations.
        MONITOR
            Health checks, metrics collection, and alerting.
        NOTIFICATION
            Sending emails, SMS, push notifications, Slack messages, etc.
        CLEANUP
            Temporary resource removal, archiving, and data-retention enforcement.
        AI
            Machine-learning inference, model training, and embedding generation.
        AGENT
            Agentic orchestration, tool-calling, and multi-step LLM workflows.
        OTHER
            Any event that does not fit a specific category above.
    """

    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    INGEST = "INGEST"
    DATABASE = "DATABASE"
    HTTP = "HTTP"
    MESSAGING = "MESSAGING"
    FILE = "FILE"
    CACHE = "CACHE"
    VALIDATE = "VALIDATE"
    ANALYZE = "ANALYZE"
    MONITOR = "MONITOR"
    AUTOMATION = "AUTOMATION"
    NOTIFICATION = "NOTIFICATION"
    CLEANUP = "CLEANUP"
    AI = "AI"
    AGENT = "AGENT"
    OTHER = "OTHER"


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

                raw_categories = getattr(
                    cls, "categories", frozenset({EventCategory.OTHER})
                )

                # Normalise: allow a bare EventCategory (common mistake) or a frozenset.
                if isinstance(raw_categories, EventCategory):
                    raw_categories = frozenset({raw_categories})

                if not isinstance(raw_categories, frozenset) or not all(
                    isinstance(c, EventCategory) for c in raw_categories
                ):
                    raise ImproperlyConfigured(
                        f"'{name}.categories' must be a frozenset of EventCategory members. "
                        f"Got: {raw_categories!r}"
                    )

                _event_registry.register(
                    event_class,
                    name=event_name,
                    namespace=version_info["namespace"],
                    version=version_info["version"],
                    changelog=version_info.get("changelog"),
                    deprecated=version_info.get("deprecated", False),
                    deprecation_info=version_info.get("deprecation_info"),
                    scheme_handler=versioning.scheme,
                    event_type=getattr(cls, "event_type", EventType.OTHER),
                    categories=raw_categories,
                )

                # Log registration
                status = "DEPRECATED" if version_info.get("deprecated") else "active"
                logger.debug(
                    f"Registered: {cls.__module__}.{name} as "
                    f"{version_info['namespace']}::{event_name}@{version_info['version']} "
                    f"[{status}]"
                )
            except RuntimeError as e:
                logger.warning(str(e))

        return cls


class EventBase(
    RetryMixin,
    ExecutorInitializerMixin,
    EventCheckpointingMixin,
    EventCommandMixin,
    ExternalCommunicationMixin,
    metaclass=EventMeta,
):
    """
    Abstract base class for event in the pipeline system.

    This class serves as a base for event-related tasks and defines common
    properties for event execution, which can be customized in subclasses.

    Class Attributes:
        executor (Type[Executor]): The executor type used to handle event execution.
                                    Defaults to DefaultExecutor.
        executor_config (ExecutorInitializerConfig): Configuration settings for the executor.
                                                    Defaults to None.
        result_evaluation_strategy (ExecutionResultEvaluationStrategyBase): The strategy to use in evaluating the
                                    results of the execution of this event in the pipeline. This will inform
                                    the pipeline as to the next execution path to take.

    Result Evaluation Strategies:
        ALL_MUST_SUCCEED/AllTasksMustSucceedStrategy: The event is considered successful only if all the tasks within the
            event succeeded. If any task fails, the evaluation should be marked as a failure.

        NO_FAILURES_ALLOWED/NoFailuresAllowedStrategy: The event is considered a failure if any of the tasks fail. Even if
            some tasks succeed, a failure in any one task results in the event being considered a failure.

        ANY_MUST_SUCCEED/AnyTaskMustSucceedStrategy: The event is considered successful if at least one of the tasks
            succeeded. This means that if any task succeeds, the event will be considered successful, even if others fail.

        MAJORITY_MUST_SUCCEED: Event succeeds if a majority of tasks succeed.

    Subclasses must implement the `process` method to define the logic for
    processing pipeline data.

    :ivar versioning_class: Versioning strategy used for this event.
    :type versioning_class: typing.Type[BaseVersioning]
    :ivar version: Current version of the event class.
    :type version: str
    :ivar changelog: Optional changelog describing changes in this version.
    :type changelog: Optional[str]
    :ivar deprecated: Flag to indicate whether the event is deprecated.
    :type deprecated: bool
    :ivar deprecation_info: Optional deprecation details if deprecated.
    :type deprecation_info: typing.Optional[DeprecationInfo]
    :ivar namespace: Namespace associated with the event.
    :type namespace: str
    :ivar name: Custom name for the event.
    :type name: typing.Optional[str]
    :ivar event_type: Type of the event, categorized by usage.
    :type event_type: EventType
    :ivar result_evaluation_strategy: Strategy to evaluate execution results.
    :type result_evaluation_strategy: ExecutionResultEvaluationStrategyBase
    :ivar INIT_PARAMS_SCHEMA: Schema for defining extra event initialization
        arguments.
    :type INIT_PARAMS_SCHEMA: typing.Dict[str, "ExtraEventInitKwargs"]
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

    # Semantic domain categories — one event may belong to multiple.
    # Always a frozenset; use frozenset({EventCategory.X, ...}) in subclasses.
    categories: typing.FrozenSet[EventCategory] = frozenset({EventCategory.OTHER})

    # how we want the execution results of this event to be evaluated by the pipeline
    result_evaluation_strategy: ExecutionResultEvaluationStrategyBase = (
        ResultEvaluationStrategies.ALL_MUST_SUCCEED
    )

    # The schema for adding extra event initialization arguments without modify the __init__
    # It uses event signal 'event_init' to hook an initializer function.
    # That will handle the setting and validation of the extra event init args
    INIT_PARAMS_SCHEMA: typing.Dict[str, "EventInitKwargs"] = {}

    def __init_subclass__(cls, **kwargs: typing.Dict[str, typing.Any]) -> None:
        # prevent the overriding of __init__
        if "__init__" in cls.__dict__:
            raise ImproperlyConfigured(
                f"'{cls.__name__}' must not override '__init__'. "
                "Use INIT_PARAMS_SCHEMA for custom init arguments, or handle "
                "initialisation inside the 'process' method."
            )
        super().__init_subclass__(**kwargs)

    def __init__(
        self,
        execution_context: "ExecutionContext",
        task_id: str,
        checkpoint_manager: typing.Optional[VolnuxCheckPointManager] = None,
        command_channel: typing.Optional["CommandChannelBase"] = None,
        previous_result: typing.Union[typing.List[EventResult], EMPTY] = EMPTY,
        stop_condition: StopCondition = StopCondition.NEVER,
        run_bypass_event_checks: bool = False,
        options: typing.Optional["Options"] = None,
        sequence_number: typing.Optional[int] = None,
    ) -> None:
        """
        Initializes an EventBase instance with the provided execution context and configuration.

        This constructor sets up the event with required inputs such as the execution context,
        task identifier, optional previous results, stop conditions, and configurable options.

        :param execution_context: The context in which the event operates, providing access
            to execution-related data.
        :type execution_context: ExecutionContext
        :param task_id: Unique identifier for the task associated with this event.
        :type task_id: str
        :param checkpoint_manager: The checkpoint manager instance to use for event
            checkpointing. Defaults to None.
        :type checkpoint_manager: Optional[VolnuxCheckPointManager]
        :param previous_result: The result of the preceding event execution. Defaults to
            `EMPTY` when not specified.
        :type previous_result: Union[List[EventResult], EMPTY]
        :param stop_condition: Determines the condition under which execution should stop.
        :type stop_condition: StopCondition
        :param run_bypass_event_checks: A flag indicating whether to bypass event checks.
        :type run_bypass_event_checks: bool
        :param options: Additional options to configure the event. Passed additional runtime
            configuration if provided.
        :type options: Optional[Options]
        :param sequence_number: Specifies the sequence number associated with the event,
            if applicable.
        :type sequence_number: Optional[int]
        :param command_channel: Key-value arguments to provide additional flexibility for configuration.
        :type command_channel: CommandChannelBase
        """
        super().__init__()

        self._setup_event(
            execution_context=execution_context,
            task_id=task_id,
            checkpoint_manager=checkpoint_manager,
            previous_result=previous_result,
            stop_condition=stop_condition,
            run_bypass_event_checks=run_bypass_event_checks,
            options=options,
            sequence_number=sequence_number,
            command_channel=command_channel,
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} executor={self.executor.__name__}>"

    def get_init_args(self) -> typing.Dict[str, typing.Any]:
        return self._init_args

    def get_call_args(self) -> typing.Dict[str, typing.Any]:
        return self._call_args  # type: ignore

    @classmethod
    def get_version_handler(cls) -> VersionHandler:
        """Get a version handler for this class"""
        if cls._version_handler is None:
            cls._version_handler = VersionHandler.from_class(
                cls, config_key="DEFAULT_EVENT_VERSIONING"
            )
        return cls._version_handler

    @classmethod
    def get_version_info(cls) -> VersionInfo:
        """Get version information by delegating to the handler."""
        return cls.get_version_handler().get_info()

    @classmethod
    def is_deprecated(cls) -> bool:
        """Check if deprecated by delegating to the handler."""
        return cls.get_version_handler().is_deprecated()

    @classmethod
    def get_all_versions(cls, name: typing.Optional[str] = None) -> typing.List[str]:
        """Get all versions from the registry."""
        handler = cls.get_version_handler()
        event_name = name or handler.class_name
        return _event_registry.list_versions(event_name, handler.namespace)

    @classmethod
    def get_latest_version(
        cls, name: typing.Optional[str] = None
    ) -> typing.Optional[str]:
        """Get the latest version from the registry."""
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
        if not isinstance(descriptor, int) or not (0 <= descriptor <= 9):
            raise ValueError("Descriptor must be an integer between 0 to 9")

        if execute_on_event_method:
            if result_success:
                res = self.on_success(result)
            else:
                res = self.on_failure(result)
        else:
            res = EventResult(
                error=not result_success,
                content=result,
                task_id=self._task_id,
                event_name=self.__class__.__name__,
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

    async def bypass(self) -> typing.Tuple[bool, typing.Any]:
        """
        Determines if the current event execution can be bypassed, allowing pipeline
        processing to continue to the next event regardless of validation or execution failures.

        This method evaluates custom bypass conditions defined for this specific event.
        When it returns True, the pipeline will skip the current event's execution
        and proceed to the next event in the sequence. When False, normal execution
        and error handling will occur.

        The bypass decision is typically based on business rules such as
        - Event is optional in certain contexts
        - an Alternative processing path exists
        - Specific data conditions make this event unnecessary

        Returns (Tuple):
            bool: True if the event can be bypassed, False if normal execution should occur
            data: Result data to pass to the next event.
        Example:
            In a shipping pipeline, certain validation steps might be bypassed
            for internal transfers while being required for external shipments.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support bypass")

    @abc.abstractmethod
    async def process(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Tuple[bool, typing.Any]:
        """
        Processes pipeline data and executes the associated logic.

        This method must be implemented by any class inheriting from EventBase.
        It defines the logic for processing pipeline data, taking in any necessary
         arguments and returning a tuple containing:
            - A boolean indicating the success or failure of the processing.
            - The result of the processing, which could vary based on the event logic.

        Returns:
            A tuple (success_flag, result), where:
                - success_flag (bool): True if processing is successful, False otherwise.
                - result (Any): The output or result of the processing, which can vary.

        Note:
            The result must be serializable (primitives, dicts, lists).
            Avoid returning file handles, sockets, or other non-serializable objects.
            For complex state, use external_resources with custom serialization.
        """
        raise NotImplementedError(
            "Event processing logic must be implemented by subclasses"
        )

    async def cleanup(self, *args, **kwargs) -> None:
        """
        Asynchronously performs cleanup operations.

        This method is designed to handle and perform various cleanup tasks,
        ensuring that resources are properly released and any necessary
        finalization operations are completed. It accepts arbitrary
        positional and keyword arguments for flexibility in handling
        specific cleanup requirements.

        :param args: Positional arguments that might be needed for
            the cleanup process.
        :param kwargs: Keyword arguments that might be necessary to
            fine-tune the cleanup behavior.
        :return: None
        """
        pass

    def event_result(
        self, error: bool, content: typing.Dict[str, typing.Any]
    ) -> EventResult:
        return EventResult(
            error=error,
            task_id=self._task_id,
            order=self._sequence_number,
            event_name=self.__class__.__name__,
            content=content,
        )

    def on_success(self, execution_result: typing.Any) -> EventResult:
        self.stop_condition.message = (
            execution_result
            if execution_result is None or isinstance(execution_result, str)
            else str(execution_result)
        )

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
    def clear_class_cache(cls) -> None:
        """Clear the cached subclass registry"""
        _event_registry.clear()

    async def __call__(self, *args, **kwargs) -> EventResult:
        """
        Asynchronously invokes the callable instance, executing the steps_runner method with the
        provided arguments. Additionally, if a command channel is present, it starts a command
        listener task that listens for commands during execution. The listener task is properly
        cleaned up upon completion or cancellation.

        :param args: Positional arguments to be passed to the steps_runner method.
        :param kwargs: Keyword arguments to be passed to the steps_runner method.
        :return: The result of the steps_runner method execution.
        :rtype: EventResult
        """
        self._main_worker_task = asyncio.current_task()
        self._command_listener_task: typing.Optional[asyncio.Task] = None

        if self._command_channel:
            self._command_listener_task = asyncio.create_task(
                self._command_listener(),
                name=f"{self.__class__.__name__}_command_listener",
            )

        result = None

        try:
            result = await self.steps_runner(*args, **kwargs)
        finally:
            if self._command_listener_task:
                self._command_listener_task.cancel()
                try:
                    await self._command_listener_task
                except asyncio.CancelledError:
                    pass

            if self._resource_monitor.is_started():
                await self._resource_monitor.stop()

            # Must run even if steps_runner() raised (e.g. StopProcessingError/
            # SwitchTask, both routine control-flow signals) — otherwise
            # external resource cleanup and the cleanup() hook never fire.
            if self._phase != EventPhase.COMPLETED:
                try:
                    await self._completed(*args, **kwargs)
                except Exception as e:
                    logger.exception(e)

        return result
