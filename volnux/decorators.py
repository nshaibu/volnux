import functools
import typing
from concurrent.futures import Executor

from .base import EventBase, ExecutorInitializerConfig, RetryPolicy
from .executors.default_executor import DefaultExecutor
from .result_evaluators import (ExecutionResultEvaluationStrategyBase,
                                ResultEvaluationStrategies)
from .utils import validate_event_process_method

if typing.TYPE_CHECKING:
    from .signal import SoftSignal


F = typing.TypeVar("F", bound=typing.Callable[..., typing.Any])
T = typing.TypeVar("T")


def event(
    name: typing.Optional[str] = None,
    executor: typing.Optional[typing.Type[Executor]] = None,
    retry_policy: typing.Optional[RetryPolicy] = None,
    executor_config: typing.Optional[ExecutorInitializerConfig] = None,
    result_evaluation_strategy: ExecutionResultEvaluationStrategyBase = ResultEvaluationStrategies.ALL_MUST_SUCCEED,
) -> typing.Callable[[F], typing.Type[EventBase]]:
    """
    Decorator to create an Event class from a function.

    This decorator transforms a function into a fully-featured Event class
    that inherits from EventBase, with configurable execution and retry behavior.

    Args:
        name: Custom name for the event. If not provided, uses the function name.
        executor: Executor class to use for event execution. Defaults to DefaultExecutor.
        retry_policy: Retry configuration for failed executions. Defaults to RetryPolicy().
        executor_config: Executor initialization configuration. Defaults to ExecutorInitializerConfig().
        result_evaluation_strategy: Strategy to use in evaluating the executing results of event.
         Defaults to ALL_MUST_SUCCEED

    Returns:
        A decorator function that transforms the input function into an Event class.

    Example:
        @event(
            name="DataProcessor",
            retry_policy=RetryPolicy(max_attempts=5, backoff_factor=2.0)
        )
        def process_data(self, data: dict) -> Tuple[bool, Any]:
            '''Process incoming data and return success status with result.'''
            processed = {"result": data.get("value", 0) * 2}
            return True, processed
    """

    def decorator(func: F) -> typing.Type[EventBase]:
        event_name = name or func.__name__
        executor_class = executor or DefaultExecutor
        _retry_policy = retry_policy or RetryPolicy()
        _executor_config = executor_config or ExecutorInitializerConfig()
        _result_evaluation_strategy = (
            result_evaluation_strategy or ResultEvaluationStrategies.ALL_MUST_SUCCEED
        )

        # Validate that func has the correct signature
        validate_event_process_method(func)

        def process(
            self,
            *args: typing.Any,
            **kwargs: typing.Any,
        ) -> typing.Tuple[bool, typing.Any]:
            """
            Execute the wrapped function with provided arguments.

            Returns:
                Tuple of (success: bool, result: Any)
            """
            return func(self, *args, **kwargs)

        # Create the class dynamically with the desired name
        generated_event = type(
            event_name,
            (EventBase,),  # Base classes
            {
                "__module__": func.__module__,
                "__doc__": func.__doc__ or f"Dynamically generated event: {event_name}",
                "__qualname__": event_name,
                "executor": executor_class,
                "executor_config": _executor_config,
                "retry_policy": _retry_policy,
                "result_evaluation_strategy": _result_evaluation_strategy,
                "process": process,
                "_original_func": func,
                "_event_name": event_name,
            },
        )

        generated_event = typing.cast(typing.Type[EventBase], generated_event)
        generated_event._original_func = func  # type: ignore[attr-defined]
        generated_event._event_name = event_name  # type: ignore[attr-defined]

        # Copy function annotations for better IDE support
        if hasattr(func, "__annotations__"):
            generated_event.process.__annotations__ = func.__annotations__.copy()

        # Use functools.wraps equivalent for the class
        functools.update_wrapper(
            generated_event, func, assigned=("__module__", "__doc__"), updated=()
        )

        return generated_event

    return decorator


def listener(
    signal: typing.Union["SoftSignal", typing.Iterable["SoftSignal"]],
    sender: typing.Type[typing.Any] = None,
) -> typing.Any:
    """
    A decorator to connect a callback function to a specified signal or signals.

    This function allows you to easily connect a callback to one or more signals, enabling
    it to be invoked when the signal is emitted.

    Usage:

        @listener(task_submit, sender=MyModel)
        def callback(sender, signal, **kwargs):
            # This callback will be executed when the post_save signal is emitted
            ...

        @listener([task_submit, pipeline_init], sender=MyModel)
        def callback(sender, signal, **kwargs):
            # This callback will be executed for both post_save and post_delete signals
            pass

    Args:
        signal (Union[Signal, List[Signal]]): A single signal or a list of signals to which the
                                               callback function will be connected.
        sender: The sender of this event. Additional keyword arguments that can be
                passed to the signal's connect method.

    Returns:
        function: The original callback function wrapped with the connection logic.
    """

    def wrapper(
        func: typing.Callable[[typing.Any], typing.Any],
    ) -> typing.Callable[[typing.Any], typing.Any]:
        if isinstance(signal, (list, tuple)):
            for s in signal:
                s.connect(listener=func, sender=sender)
        else:
            signal.connect(listener=func, sender=sender)
        return func

    return wrapper
