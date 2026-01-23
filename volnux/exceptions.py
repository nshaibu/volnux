import typing

if typing.TYPE_CHECKING:
    from volnux.result import EventResult


class ImproperlyConfigured(Exception):
    pass


class SerializationError(Exception):
    """Raised when serialization or deserialization fails."""

    pass


class PipelineError(Exception):
    def __init__(
        self, message: str, code: typing.Any = None, params: typing.Any = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.params = params

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "error_class": self.__class__.__name__,
            "message": self.message,
            "code": self.code,
            "params": self.params,
        }


class TaskError(PipelineError):
    pass


class EventDoesNotExist(PipelineError, ValueError):
    pass


class StateError(PipelineError, ValueError):
    pass


class EventDone(PipelineError):
    pass


class EventNotConfigured(ImproperlyConfigured):
    pass


class BadPipelineError(ImproperlyConfigured, PipelineError):
    def __init__(
        self,
        *args: typing.Any,
        exception: typing.Optional[Exception] = None,
        **kwargs: typing.Dict[str, typing.Any],
    ) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
        self.exception = exception


class MultiValueError(PipelineError, KeyError):
    pass


class StopProcessingError(PipelineError, RuntimeError):
    def __init__(
        self,
        *args: typing.Any,
        exception: typing.Optional[Exception] = None,
        stop_condition: typing.Optional[typing.Any] = None,
        **kwargs: typing.Dict[str, typing.Any],
    ) -> None:
        self.exception = exception
        self.stop_condition = stop_condition
        super().__init__(*args, **kwargs)  # type: ignore


class MaxRetryError(Exception):
    """
    Raised when the maximum number of retries is exceeded.
    """

    def __init__(
        self, attempt: int, exception: Exception, reason: typing.Optional[str] = None
    ) -> None:
        self.reason = reason
        self.attempt = attempt
        self.exception = exception
        message = "Max retries exceeded: %s (Caused by %r)" % (
            self.attempt,
            self.reason,
        )
        super().__init__(message)


class ValidationError(PipelineError, ValueError):
    """ValidationError raised when validation fails."""

    def __init__(
        self, *args: typing.Any, **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        super().__init__(*args, **kwargs)  # type: ignore


class ObjectExistError(ValueError):
    """ObjectExistError raised when an object already exists."""

    pass


class ObjectDoesNotExist(ValueError):
    """ObjectDoesNotExist raised when an object does not exist."""


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


## Meta Event Errors
class NestedMetaEventError(Exception):
    """Raised when nested meta events are detected"""

    pass


class MetaEventConfigurationError(Exception):
    """Raised when meta event is misconfigured"""

    pass


class MetaEventExecutionError(Exception):
    """Raised when meta event execution fails"""

    pass
