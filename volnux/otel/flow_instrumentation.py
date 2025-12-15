"""
OpenTelemetry Instrumentation for Flow Layer with Metrics Collection

This module instruments the BaseFlow and its subclasses to provide:
1. Span tracing for flow execution
2. Event-level spans for individual event execution
3. Executor tracking
4. Metrics collection (duration, counts, errors)
"""

import logging
import typing
import functools
import time
from opentelemetry import trace, metrics
from opentelemetry.trace import Status, StatusCode

from volnux.otel.tracer_setup import get_tracer
from volnux.otel.context_manager import OTelContextManager, SpanHelper

if typing.TYPE_CHECKING:
    from volnux.flows.base import BaseFlow
    from volnux import Event

logger = logging.getLogger(__name__)


# ============================================================================
# Metrics Setup
# ============================================================================


class FlowMetrics:
    """
    Centralized metrics collection for Flow execution.

    Provides OpenTelemetry metrics for:
    - Flow execution duration
    - Event execution duration
    - Task counts
    - Error rates
    - Executor usage
    """

    _meter = None
    _initialized = False

    # Metric instruments
    flow_duration = None
    event_duration = None
    task_counter = None
    error_counter = None
    executor_usage = None
    parallel_task_gauge = None

    @classmethod
    def initialize(cls):
        """Initialize metrics instruments"""
        if cls._initialized:
            return

        try:
            meter_provider = metrics.get_meter_provider()
            cls._meter = meter_provider.get_meter("volnux.flows", version="1.0.0")

            # Flow duration histogram
            cls.flow_duration = cls._meter.create_histogram(
                name="flow.duration",
                description="Duration of flow execution in milliseconds",
                unit="ms",
            )

            # Event duration histogram
            cls.event_duration = cls._meter.create_histogram(
                name="event.duration",
                description="Duration of event execution in milliseconds",
                unit="ms",
            )

            # Task counter
            cls.task_counter = cls._meter.create_counter(
                name="task.count", description="Number of tasks executed", unit="1"
            )

            # Error counter
            cls.error_counter = cls._meter.create_counter(
                name="error.count", description="Number of errors encountered", unit="1"
            )

            # Executor usage counter
            cls.executor_usage = cls._meter.create_counter(
                name="executor.usage",
                description="Number of times each executor type is used",
                unit="1",
            )

            # Parallel task gauge
            cls.parallel_task_gauge = cls._meter.create_up_down_counter(
                name="parallel_tasks.active",
                description="Number of parallel tasks currently executing",
                unit="1",
            )

            cls._initialized = True
            logger.info("Flow metrics initialized")

        except Exception as e:
            logger.warning(f"Failed to initialize flow metrics: {e}")
            cls._initialized = False

    @classmethod
    def record_flow_duration(
        cls, duration_ms: float, flow_type: str, status: str, task_count: int
    ):
        """Record flow execution duration"""
        if cls.flow_duration:
            cls.flow_duration.record(
                duration_ms,
                attributes={
                    "flow.type": flow_type,
                    "flow.status": status,
                    "flow.task_count": str(task_count),
                },
            )

    @classmethod
    def record_event_duration(
        cls, duration_ms: float, event_name: str, status: str, executor_type: str
    ):
        """Record event execution duration"""
        if cls.event_duration:
            cls.event_duration.record(
                duration_ms,
                attributes={
                    "event.name": event_name,
                    "event.status": status,
                    "executor.type": executor_type,
                },
            )

    @classmethod
    def increment_task_count(cls, task_type: str, flow_type: str):
        """Increment task execution counter"""
        if cls.task_counter:
            cls.task_counter.add(
                1, attributes={"task.type": task_type, "flow.type": flow_type}
            )

    @classmethod
    def increment_error_count(
        cls, error_type: str, flow_type: str, event_name: str = None
    ):
        """Increment error counter"""
        if cls.error_counter:
            attributes = {"error.type": error_type, "flow.type": flow_type}
            if event_name:
                attributes["event.name"] = event_name

            cls.error_counter.add(1, attributes=attributes)

    @classmethod
    def record_executor_usage(cls, executor_type: str, flow_type: str):
        """Record executor usage"""
        if cls.executor_usage:
            cls.executor_usage.add(
                1, attributes={"executor.type": executor_type, "flow.type": flow_type}
            )

    @classmethod
    def increment_parallel_tasks(cls, count: int = 1):
        """Increment parallel task counter"""
        if cls.parallel_task_gauge:
            cls.parallel_task_gauge.add(count)

    @classmethod
    def decrement_parallel_tasks(cls, count: int = 1):
        """Decrement parallel task counter"""
        if cls.parallel_task_gauge:
            cls.parallel_task_gauge.add(-count)


# ============================================================================
# Flow Instrumentation
# ============================================================================


def instrument_flow_run(original_run):
    """
    Decorator to instrument BaseFlow.run() method.

    Creates spans for flow execution and tracks metrics.
    """

    @functools.wraps(original_run)
    async def instrumented_run(self: "BaseFlow"):
        tracer = get_tracer()

        if not tracer:
            return await original_run(self)

        # Initialize metrics if not done
        FlowMetrics.initialize()

        # Get parent context from ExecutionContext
        parent_context = OTelContextManager.get_async_context()

        flow_type = self.__class__.__name__
        task_count = len(self.task_profiles) if self.task_profiles else 0

        start_time = time.time()

        with tracer.start_as_current_span(
            f"flow.run.{flow_type}",
            context=parent_context,
            kind=trace.SpanKind.INTERNAL,
            attributes={
                "flow.type": flow_type,
                "flow.task_count": task_count,
                "flow.context_id": self.context.id,
            },
        ) as span:
            try:
                # Add task profile information
                if self.task_profiles:
                    task_names = [task.get_event_name() for task in self.task_profiles]
                    span.set_attribute("flow.tasks", ",".join(task_names))

                    # Check for parallel execution
                    parallel_count = sum(
                        1
                        for task in self.task_profiles
                        if getattr(task, "is_parallel_execution_node", False)
                    )
                    if parallel_count > 0:
                        span.set_attribute("flow.parallel_tasks", parallel_count)
                        FlowMetrics.increment_parallel_tasks(parallel_count)

                span.add_event("flow_execution_started")

                # Execute original run method
                result = await original_run(self)

                # Calculate duration
                duration_ms = (time.time() - start_time) * 1000

                span.add_event("flow_execution_completed", {"duration_ms": duration_ms})

                span.set_status(Status(StatusCode.OK))

                # Record metrics
                FlowMetrics.record_flow_duration(
                    duration_ms=duration_ms,
                    flow_type=flow_type,
                    status="success",
                    task_count=task_count,
                )

                # Decrement parallel task counter
                if self.task_profiles:
                    parallel_count = sum(
                        1
                        for task in self.task_profiles
                        if getattr(task, "is_parallel_execution_node", False)
                    )
                    if parallel_count > 0:
                        FlowMetrics.decrement_parallel_tasks(parallel_count)

                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000

                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                # Record error metrics
                FlowMetrics.record_flow_duration(
                    duration_ms=duration_ms,
                    flow_type=flow_type,
                    status="error",
                    task_count=task_count,
                )

                FlowMetrics.increment_error_count(
                    error_type=type(e).__name__, flow_type=flow_type
                )

                raise

    return instrumented_run


def instrument_get_initialized_event(original_get_initialized_event):
    """
    Decorator to instrument BaseFlow.get_initialized_event() method.

    Tracks event initialization with spans and metrics.
    """

    @functools.wraps(original_get_initialized_event)
    def instrumented_get_initialized_event(self: "BaseFlow", task_profile):
        tracer = get_tracer()

        if not tracer:
            return original_get_initialized_event(self, task_profile)

        with tracer.start_as_current_span(
            "flow.initialize_event", kind=trace.SpanKind.INTERNAL
        ) as span:
            try:
                event_name = task_profile.get_event_name()

                span.set_attribute("event.name", event_name)
                span.set_attribute("task.id", str(task_profile.get_id()))
                SpanHelper.add_task_attributes(span, task_profile)

                # Track if this is a parallel task
                if getattr(task_profile, "is_parallel_execution_node", False):
                    span.set_attribute("task.parallel_execution", True)

                # Track retry configuration
                if task_profile.options and task_profile.options.retry_attempts:
                    span.set_attribute(
                        "event.retry_attempts", task_profile.options.retry_attempts
                    )

                span.add_event("event_initialization_started")

                # Call original method
                event, event_call_args = original_get_initialized_event(
                    self, task_profile
                )

                span.add_event(
                    "event_initialization_completed",
                    {"event.class": event.__class__.__name__},
                )

                # Record task count metric
                FlowMetrics.increment_task_count(
                    task_type=event_name, flow_type=self.__class__.__name__
                )

                return event, event_call_args

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                FlowMetrics.increment_error_count(
                    error_type=type(e).__name__,
                    flow_type=self.__class__.__name__,
                    event_name=task_profile.get_event_name(),
                )

                raise

    return instrumented_get_initialized_event


def instrument_submit_event_to_executor(original_submit):
    """
    Decorator to instrument BaseFlow._submit_event_to_executor() method.

    Tracks event submission and execution with detailed spans.
    """

    @functools.wraps(original_submit)
    async def instrumented_submit(
        self: "BaseFlow", executor, event: "Event", event_call_kwargs, **kwargs
    ):
        tracer = get_tracer()

        if not tracer:
            return await original_submit(
                self, executor, event, event_call_kwargs, **kwargs
            )

        executor_type = executor.__class__.__name__
        event_name = event.__class__.__name__

        with tracer.start_as_current_span(
            f"event.execute.{event_name}",
            kind=trace.SpanKind.INTERNAL,
            attributes={
                "event.name": event_name,
                "event.task_id": (
                    str(event.task_id) if hasattr(event, "task_id") else None
                ),
                "executor.type": executor_type,
                "context.id": self.context.id,
            },
        ) as span:
            try:
                # Add event configuration
                if hasattr(event, "get_retry_policy"):
                    retry_policy = event.get_retry_policy()
                    if retry_policy:
                        span.set_attribute(
                            "event.retry.max_attempts", retry_policy.max_attempts
                        )

                # Add executor configuration details
                if hasattr(executor, "max_workers"):
                    span.set_attribute("executor.max_workers", executor.max_workers)

                span.add_event("event_submission_started")

                # Record executor usage
                FlowMetrics.record_executor_usage(
                    executor_type=executor_type, flow_type=self.__class__.__name__
                )

                # Track event execution time
                start_time = time.time()

                # Submit event
                future = await original_submit(
                    self, executor, event, event_call_kwargs, **kwargs
                )

                # Note: The actual execution happens asynchronously
                # We'll track completion in a callback
                def record_completion(fut):
                    duration_ms = (time.time() - start_time) * 1000

                    try:
                        result = fut.result()
                        status = "success"

                        span.add_event(
                            "event_execution_completed",
                            {"duration_ms": duration_ms, "status": status},
                        )

                    except Exception as e:
                        status = "error"
                        span.record_exception(e)

                        FlowMetrics.increment_error_count(
                            error_type=type(e).__name__,
                            flow_type=self.__class__.__name__,
                            event_name=event_name,
                        )

                    # Record event duration metric
                    FlowMetrics.record_event_duration(
                        duration_ms=duration_ms,
                        event_name=event_name,
                        status=status,
                        executor_type=executor_type,
                    )

                future.add_done_callback(record_completion)

                span.set_status(Status(StatusCode.OK))
                return future

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                FlowMetrics.increment_error_count(
                    error_type=type(e).__name__,
                    flow_type=self.__class__.__name__,
                    event_name=event_name,
                )

                raise

    return instrumented_submit


def instrument_map_events_to_executor(original_map_events):
    """
    Decorator to instrument BaseFlow._map_events_to_executor() method.

    Tracks parallel event execution.
    """

    @functools.wraps(original_map_events)
    async def instrumented_map_events(
        self: "BaseFlow", executor, event_execution_config
    ):
        tracer = get_tracer()

        if not tracer:
            return await original_map_events(self, executor, event_execution_config)

        executor_type = executor.__class__.__name__
        event_count = len(event_execution_config)

        with tracer.start_as_current_span(
            "flow.map_events_to_executor",
            kind=trace.SpanKind.INTERNAL,
            attributes={"executor.type": executor_type, "event.count": event_count},
        ) as span:
            try:
                # List event names being executed
                event_names = [
                    event.__class__.__name__ for event in event_execution_config.keys()
                ]
                span.set_attribute("events", ",".join(event_names))

                span.add_event(
                    "parallel_execution_started", {"event_count": event_count}
                )

                start_time = time.time()

                # Execute original method
                result = await original_map_events(
                    self, executor, event_execution_config
                )

                duration_ms = (time.time() - start_time) * 1000

                span.add_event(
                    "parallel_execution_completed",
                    {"duration_ms": duration_ms, "event_count": event_count},
                )

                span.set_status(Status(StatusCode.OK))
                return result

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_map_events


def instrument_get_flow_executor(original_get_executor):
    """
    Decorator to instrument BaseFlow.get_flow_executor() method.

    Tracks executor selection and configuration.
    """

    @functools.wraps(original_get_executor)
    async def instrumented_get_executor(self: "BaseFlow", *args, **kwargs):
        tracer = get_tracer()

        if not tracer:
            return await original_get_executor(self, *args, **kwargs)

        with tracer.start_as_current_span(
            "flow.get_executor", kind=trace.SpanKind.INTERNAL
        ) as span:
            try:
                # Get executor
                executor = await original_get_executor(self, *args, **kwargs)

                # Record executor type
                executor_type = (
                    executor.__name__
                    if hasattr(executor, "__name__")
                    else str(executor)
                )
                span.set_attribute("executor.type", executor_type)

                span.add_event("executor_resolved", {"executor_type": executor_type})

                return executor

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_get_executor


# ============================================================================
# Patching Functions
# ============================================================================


def patch_flow_class():
    """
    Patch BaseFlow class to add OpenTelemetry instrumentation and metrics.

    This should be called during application initialization.
    """
    from volnux.flows.base import BaseFlow

    if not hasattr(BaseFlow, "_original_run"):
        # Store original methods
        BaseFlow._original_run = BaseFlow.run
        BaseFlow._original_get_initialized_event = BaseFlow.get_initialized_event
        BaseFlow._original_submit_event = BaseFlow._submit_event_to_executor
        BaseFlow._original_map_events = BaseFlow._map_events_to_executor
        BaseFlow._original_get_executor = BaseFlow.get_flow_executor

        # Apply instrumentation (needs to handle abstract methods properly)
        # Note: run() is abstract, so we'll instrument it at the subclass level
        BaseFlow.get_initialized_event = instrument_get_initialized_event(
            BaseFlow._original_get_initialized_event
        )
        BaseFlow._submit_event_to_executor = instrument_submit_event_to_executor(
            BaseFlow._original_submit_event
        )
        BaseFlow._map_events_to_executor = instrument_map_events_to_executor(
            BaseFlow._original_map_events
        )

        logger.info("BaseFlow instrumented with OpenTelemetry")


def patch_concrete_flow_classes(*flow_classes):
    """
    Patch concrete Flow implementations (subclasses of BaseFlow).

    Args:
        flow_classes: Flow classes to instrument (e.g., SingleTaskFlow, MultiTaskFlow)

    Example:
        from volnux.flows.single import SingleTaskFlow
        from volnux.flows.multi import MultiTaskFlow

        patch_concrete_flow_classes(SingleTaskFlow, MultiTaskFlow)
    """
    for flow_class in flow_classes:
        if not hasattr(flow_class, "_original_run"):
            flow_class._original_run = flow_class.run
            flow_class.run = instrument_flow_run(flow_class._original_run)

            logger.info(f"{flow_class.__name__} instrumented with OpenTelemetry")


def patch_all_flow_components():
    """
    Patch all flow-related components for OpenTelemetry instrumentation.

    Call this once during application initialization after importing flow classes.
    """
    patch_flow_class()

    # Try to patch concrete implementations if they're available
    try:
        from volnux.flows.single import SingleTaskFlow
        from volnux.flows.multi import MultiTaskFlow

        patch_concrete_flow_classes(SingleTaskFlow, MultiTaskFlow)
        logger.info("Concrete Flow classes instrumented")
    except ImportError:
        logger.info("Concrete Flow classes not found, skipping")

    logger.info("All Flow components instrumented with OpenTelemetry")


# Export
__all__ = [
    "FlowMetrics",
    "instrument_flow_run",
    "instrument_get_initialized_event",
    "instrument_submit_event_to_executor",
    "instrument_map_events_to_executor",
    "patch_flow_class",
    "patch_concrete_flow_classes",
    "patch_all_flow_components",
]
