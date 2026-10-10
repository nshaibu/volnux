import enum
from datetime import datetime, timezone
from dataclasses import dataclass, field, InitVar
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from volnux.event import EventBase
    from volnux.result import EventResult
    from volnux.mixins.protocols.event import BaseEvent
    from volnux.mixins.event.external import ExternalCommunicationType


class ImproperlyConfigured(Exception):
    """Raised when the system is improperly configured."""

    pass


class SerializationError(Exception):
    """Raised when serialization or deserialization fails."""

    pass


class PipelineError(Exception):
    """Base class for all pipeline errors."""

    def __init__(self, message: str, code: Any = None, params: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.params = params

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_class": self.__class__.__name__,
            "message": self.message,
            "code": self.code,
            "params": self.params,
        }


class TaskError(PipelineError):
    """Raised when a task fails."""

    pass


class EventDoesNotExist(PipelineError, ValueError):
    """Raised when an event does not exist."""

    pass


class StateError(PipelineError, ValueError):
    """Raised when an event state is invalid."""

    pass


class EventDone(PipelineError):
    """Raised when an event is already done."""

    pass


class EventNotConfigured(ImproperlyConfigured):
    """Raised when an event is not configured."""

    pass


class BadPipelineError(ImproperlyConfigured, PipelineError):
    """Raised when a pipeline is improperly configured."""

    def __init__(
        self,
        *args: Any,
        exception: Optional[Exception] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
        self.exception = exception


class MultiValueError(PipelineError, KeyError):
    """Raised when multiple values are found for a given key."""

    pass


class StopProcessingError(PipelineError, RuntimeError):
    """Raised when processing should be stopped due to a specific condition."""

    def __init__(
        self,
        *args: Any,
        exception: Optional[Exception] = None,
        stop_condition: Optional[Any] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        self.exception = exception
        self.stop_condition = stop_condition
        super().__init__(*args, **kwargs)  # type: ignore


class MaxRetryError(Exception):
    """
    Raised when the maximum number of retries is exceeded.
    """

    def __init__(
        self, attempt: int, exception: Exception, reason: Optional[str] = None
    ) -> None:
        self.reason = reason
        self.attempt = attempt
        self.exception = exception
        message = f"Max retries exceeded: {attempt} (Caused by {reason!r})"
        super().__init__(message)


class ValidationError(PipelineError, ValueError):
    """ValidationError raised when validation fails."""

    def __init__(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)  # type: ignore


class EmptyResultError(Exception):
    """Raised when an event's process() method returns a None or no result."""

    def __init__(self, event_class: str, method_name: Optional[str] = None):
        super().__init__(
            f"The {method_name or 'process'}() method of '{event_class}' did not return a result.\n\n"
            f"Every {method_name or 'process'}() method must return a tuple of (success: bool, result: Any).\n"
            f"Example:\n"
            f"    async def {method_name or 'process'}(self, *args, **kwargs):\n"
            f"        data = await self.previous_result.first()\n"
            f"        processed = transform(data)\n"
            f"        return True, processed\n\n"
            f"Check your implementation and ensure:\n"
            f"  1. The method returns a value (not None)\n"
            f"  2. The first element is a boolean (True/False)\n"
            f"  3. The second element is your result data"
        )


class ObjectExistError(ValueError):
    """ObjectExistError raised when an object already exists."""

    pass


class ObjectDoesNotExist(ValueError):
    """ObjectDoesNotExist raised when an object does not exist."""


class ObjectProtectedError(ValueError):
    """ObjectProtectedError raised when an object cannot be deleted or modified."""

    pass


class SwitchTask(Exception):
    """SwitchTask raised to indicate a task switch is required."""

    def __init__(
        self,
        current_task_id: str,
        next_task_descriptor: int,
        result: "EventResult",
        reason: str = "Manual",
    ) -> None:
        self.current_task_id = current_task_id
        self.next_task_descriptor = next_task_descriptor
        self.result = result
        self.reason = reason
        self.descriptor_configured: bool = False
        message = "Task switched to %s (Caused by %r)" % (
            self.next_task_descriptor,
            reason,
        )
        super().__init__(message)


class SuspendTask(Exception):
    """
    Represents an exception that is raised when a task is suspended.

    This exception is intended to be used in systems where task
    prioritization, cancellation, or external coordination requires a
    task to pause and be resumed or re-queued later. It carries
    information about the specific task instance that has been
    suspended, why it was suspended, and any data needed to resume it.

    :ivar task_instance: The task instance associated with the suspension.
    :type task_instance: BaseEvent
    :ivar suspension_type: The reason the task was suspended.
    :type suspension_type: Optional[SuspensionType]
    :ivar suspension_data: Additional data needed to resume the task.
    :type suspension_data: object
    """

    class SuspensionType(enum.Enum):
        """Reason a task was suspended via SuspendTask"""

        PREEMPTION = "preemption"
        CANCELLATION = "cancellation"
        HITL = "hitl"
        EXTERNAL_EVENT = "external_event"
        CONDITION = "condition"

    def __init__(
        self,
        task_instance: "BaseEvent",
        *,
        suspension_type: Optional[SuspensionType] = None,
        suspension_data: object = None,
        message: str = "Task suspended for higher priority execution",
    ):
        self.task_instance = task_instance
        self.suspension_type = suspension_type  # type: ignore[misc]
        self.suspension_data = suspension_data
        super().__init__(message)

    def get_phase(self):
        return getattr(self.task_instance, "_phase", "Unknown")


class SkipExecutionError(Exception):
    """Raised internally to skip remaining lifecycle phases."""

    pass


@dataclass
class ExternalCommunicationSuspensionRequest(SuspendTask):
    """
    Represents a request to suspend a task waiting for external communication
    such as human-in-the-loop (HITL) or other async events.

    This class extends the `SuspendTask` class and is used to encapsulate details of a suspension request for
    an external communication task. It includes attributes that define the request details,
    payload, options, timeout duration, and metadata regarding when the request was created.

    :ivar request_id: Unique identifier for the suspension request.
    :type request_id: str
    :ivar request_type: Type of the request (e.g., "HITL suspension").
    :type request_type: ExternalCommunicationType
    :ivar title: Title of the suspension request.
    :type title: str
    :ivar description: A detailed description explaining the purpose of the suspension request.
    :type description: str
    :ivar payload: A dictionary containing additional data relevant to the suspension request.
    :type payload: Dict[str, Any]
    :ivar options: Optional list of string-based options relevant to the request.
    :type options: Optional[List[str]]
    :ivar timeout_hours: Optional duration in hours indicating how long the suspension request should stay active.
    :type timeout_hours: Optional[int]
    :ivar task: The task instance associated with the suspension request.
    :type task: EventBase
    :ivar event_type: Optional string indicating the type of event to listen for.
    :type event_type: Optional[str]
    :ivar event_filter: Dictionary containing filters for the event listener.
    :type event_filter: Dict[str, Any]
    :ivar message: Message to be displayed when the suspension request is raised.
    :type message: str
    :ivar created_at: ISO 8601 formatted string representing the timestamp of when the request was created.
    :type created_at: str
    """

    request_id: str
    request_type: "ExternalCommunicationType"
    task_id: str
    title: str
    description: str
    payload: Dict[str, Any]
    title: str
    description: str
    payload: Dict[str, Any]
    options: Optional[List[str]]
    timeout_hours: Optional[int]
    task: InitVar["BaseEvent"]
    event_type: Optional[str] = None
    event_filter: Dict[str, Any] = field(default_factory=dict)
    message: str = "Task suspended for higher priority execution"
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def __post_init__(self, task: "BaseEvent") -> None:
        # request_type is an ExternalCommunicationType, imported only under
        # TYPE_CHECKING to avoid a circular import with volnux.mixins.event.external
        suspension_type = (
            SuspensionType.HITL
            if getattr(self.request_type, "value", None) == "hitl"
            else SuspensionType.EXTERNAL_EVENT
        )
        super().__init__(task, suspension_type=suspension_type, message=self.message)


class TaskSwitchingError(PipelineError):
    """TaskSwitchingError raised when a task switch fails."""


class SqlOperationError(ValueError):
    """SqlOperationError raised when a SQL operation fails."""


class PipelineExecutionError(PipelineError):
    """Exception raised when pipeline execution fails."""


class PipelineConfigurationError(PipelineError):
    """Exception raised for configuration errors."""


class ExecutorNotFound(IndexError):
    """Exception raised when an executor does not exist."""


class PointyNotExecutable(Exception):
    """Exception raised when a pointy script is not executable."""


# Meta Event Errors
class NestedMetaEventError(Exception):
    """Raised when nested meta-events are detected"""

    pass


class MetaEventConfigurationError(Exception):
    """Raised when a meta-event is misconfigured"""

    pass


class MetaEventExecutionError(Exception):
    """Raised when meta-event execution fails"""

    pass


# AI Agent
class ToolPermissionError(PermissionError):
    """
    Raised when an agent calls an undeclared tool or hands off to an
    undeclared target. This is a governance violation — not a runtime
    error — and is emitted as a security event via soft signal.
    """


class MaxReasoningStepsExceeded(RuntimeError):
    """
    Raised when the agent exceeds ``max_reasoning_steps`` without a
    terminal action. Prevents runaway API consumption.
    """


class LLMProviderError(RuntimeError):
    """
    Raised when the LLM provider is unavailable, returns an error, or
    produces a structurally invalid response.
    """


class HallucinationDetected(RuntimeError):
    """
    Raised when domain-specific validation rejects the LLM response.
    Subclasses override ``detect_hallucination()`` with domain rules.
    """


# Sage errors
class SagaCompensationError(Exception):
    """One or more compensation steps failed after a saga step failure.

    This indicates the system may be in a partially inconsistent state
    and likely requires manual intervention.
    """

    def __init__(
        self,
        message: str,
        original_error: Exception,
        compensation_errors: List[tuple[int, Exception]],
    ):
        super().__init__(message)
        self.original_error = original_error
        # List of (step_index, exception) for each failed compensation.
        self.compensation_errors = compensation_errors


class SagaError(Exception):
    """A saga step failed; all completed steps were successfully compensated."""

    def __init__(self, message: str, original_error: Exception):
        super().__init__(message)
        self.original_error = original_error


# Command line errors
class CommandError(Exception):
    """Exception raised for command errors."""

    pass


class SubprocessTimeoutError(Exception):
    """
    Raised by ``run_command`` when the subprocess exceeds its timeout.

    Wraps ``subprocess.TimeoutExpired`` with the timeout value already
    converted to seconds, so callers do not need to re-derive it.
    """

    def __init__(self, cmd: List[str], timeout_seconds: float):
        self.cmd = cmd
        self.timeout_seconds = timeout_seconds
        super().__init__(f"Command timed out after {timeout_seconds:.1f}s: {cmd[0]!r}")
