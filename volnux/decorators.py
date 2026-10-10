import functools
import typing
import logging
import hashlib
import json
import asyncio
from concurrent.futures import Executor

from .event import EventBase
from .mixins.event import RetryPolicy
from volnux.parser.executor_config import ExecutorInitializerConfig
from .executors.default import DefaultExecutor
from .result_evaluators import (
    ExecutionResultEvaluationStrategyBase,
    ResultEvaluationStrategies,
)
from .utils import validate_event_process_method
from volnux.asset import (
    AssetKey,
    AssetMaterialisation,
    get_asset_catalog,
    FreshnessPolicy,
)

if typing.TYPE_CHECKING:
    from .signal import SoftSignal


logger = logging.getLogger(__name__)


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
        >>> @event(
        ...    name="DataProcessor",
        ...    retry_policy=RetryPolicy(max_attempts=5, backoff_factor=2.0)
        ...)
        ...def process_data(self, data: dict) -> Tuple[bool, Any]:
        ...    '''Process incoming data and return success status with result.'''
        ...    processed = {"result": data.get("value", 0) * 2}
        ...    return True, processed
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
    signal: typing.Union["SoftSignal", typing.Sequence["SoftSignal"]],
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


def asset(
    key: typing.Optional[AssetKey] = None,
    *,
    description: typing.Optional[str] = None,
    group: typing.Optional[str] = None,
    freshness_policy: typing.Optional[FreshnessPolicy] = None,
    upstream_keys: typing.Optional[typing.List[AssetKey]] = None,
    auto_register: bool = True,
):
    """
    Decorator that declares an EventBase subclass produces a named asset.

    Apply this to any EventBase subclass whose output should be tracked
    in the asset catalog. The decorator registers the asset and links
    it to the event class.

    Args:
        key: The asset key. Defaults to the event class name.
        description: Human-readable description of the asset.
        group: Logical grouping for organization in the UI.
        freshness_policy: How fresh this asset should be kept.
            If None, the asset has no freshness requirement.
        upstream_keys: Assets this asset depends on.
        auto_register: If True (default), register the asset in the
            global catalog at decoration time.

    Returns:
        A decorator that can be applied to an EventBase subclass.

    Example:
        >>> @asset(
        ...     key=AssetKey("cleaned_orders"),
        ...     description="Orders with nulls removed",
        ...     group="order_processing",
        ...     freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
        ... )
        ... class CleanOrdersEvent(EventBase):
        ...     async def process(self, **kwargs):
        ...         return True, [r for r in kwargs['orders'] if r]
    """

    def decorator(cls: typing.Type[EventBase]) -> typing.Type[EventBase]:
        # Determine the asset key
        asset_key = key or AssetKey(cls.__name__)

        # Set asset metadata on the class
        cls._volnux_asset = {
            "key": asset_key,
            "description": description,
            "group": group,
            "freshness_policy": freshness_policy,
            "upstream_keys": upstream_keys or [],
        }

        # Mark the class as an asset producer
        cls._is_asset = True

        # Override the event's process wrapper to handle asset tracking
        original_process = cls.process

        async def asset_tracking_process(self, **kwargs):
            """Wrapper that records materialisation on successful completion."""
            result = await original_process(self, **kwargs)

            # If the event completed successfully, record materialisation
            if isinstance(result, tuple) and len(result) == 2:
                success, data = result
                if success and getattr(self, "_is_asset", False):
                    await self._record_asset_materialisation(data)

            return result

        cls.process = asset_tracking_process

        # Add materialisation recording method
        async def _record_asset_materialisation(
            self, data: typing.Any
        ) -> typing.Optional[AssetMaterialisation]:
            """Record that this event produced its asset."""
            asset_meta = getattr(self, "_volnux_asset", None)
            if asset_meta is None:
                return None

            catalog = get_asset_catalog()

            # Collect upstream versions
            upstream_versions = {}
            for uk in asset_meta.get("upstream_keys", []):
                mat = await catalog.get_materialisation(uk)
                if mat:
                    upstream_versions[str(uk)] = mat.asset_version

            # Determine asset version
            # Use a hash of the data and event version for versioning
            data_hash = hashlib.md5(
                json.dumps(data, sort_keys=True, default=str).encode()
            ).hexdigest()[:12]

            event_version = getattr(self, "_version", "0.1.0")
            asset_version = f"{event_version}+{data_hash}"

            # TODO: update workflow_id and execution_id to use the ones generated by the trigger
            return await catalog.record_materialisation(
                key=asset_meta["key"],
                asset_version=asset_version,
                producing_event=cls.__name__,
                producing_event_version=event_version,
                workflow_id=getattr(self, "_workflow_id", "unknown"),
                execution_id=getattr(self, "_execution_id", "unknown"),
                task_id=getattr(self, "_task_id", "unknown"),
                upstream_versions=upstream_versions,
                metadata={
                    "description": asset_meta.get("description"),
                    "group": asset_meta.get("group"),
                },
            )

        cls._record_asset_materialisation = _record_asset_materialisation

        # Register in the global catalog
        if auto_register:
            catalog = get_asset_catalog()

            try:
                loop = asyncio.get_running_loop()
                # We're in an async context — schedule registration
                loop.create_task(
                    catalog.register(
                        key=asset_key,
                        producing_event=f"{cls.__module__}.{cls.__name__}",
                        description=description,
                        group=group,
                        freshness_policy=freshness_policy,
                        upstream_keys=upstream_keys or [],
                    )
                )
            except RuntimeError:
                # No running event loop — register synchronously
                asyncio.run(
                    catalog.register(
                        key=asset_key,
                        producing_event=f"{cls.__module__}.{cls.__name__}",
                        description=description,
                        group=group,
                        freshness_policy=freshness_policy,
                        upstream_keys=upstream_keys or [],
                    )
                )

        return cls

    return decorator
