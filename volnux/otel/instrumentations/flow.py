"""
OpenTelemetry instrumentation of the flow layer, with metrics.

* ``flow.run.<Flow>``: one span per flow ``run()``. ``run()`` returns the future
  of the work it submitted, so the span ends when that future does (not when
  ``run()`` returns), and its outcome and duration are the work's.
* ``event.execute.<Event>``: one span per event submitted to an executor, ended by
  the event's future (a failed or cancelled event ends it as an error).
* ``flow.initialize_event`` and ``flow.map_events_to_executor``.

Metrics (meter ``volnux.flows``): ``flow.duration``, ``event.duration``,
``task.count``, ``error.count``, ``executor.usage``, ``parallel_tasks.active``.
"""

import asyncio
import functools
import logging
import threading
import time
import typing
from typing import Any, Optional

from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode

from volnux.otel.context_manager import OTelContextManager, SpanHelper
from ._common import (
    clean_attributes,
    end_span_with,
    get_tracer,
    record_error,
    set_attributes,
    traced,
)
from ._patching import patch_attr

if typing.TYPE_CHECKING:
    from volnux.flows.base import BaseFlow
    from volnux import Event

logger = logging.getLogger(__name__)


class FlowMetrics:
    """
    Centralized metrics collection for Flow execution.

    The instruments are made on first use, once, under a lock. :meth:`reset`
    forgets them (a new meter provider, tests).
    """

    _meter = None
    _initialized = False
    _lock = threading.Lock()

    # Metric instruments
    flow_duration = None
    event_duration = None
    task_counter = None
    error_counter = None
    executor_usage = None
    parallel_task_gauge = None

    @classmethod
    def initialize(cls) -> None:
        """Create the instruments (once)."""
        if cls._initialized:
            return
        with cls._lock:
            if cls._initialized:
                return
            try:
                meter = metrics.get_meter_provider().get_meter(
                    "volnux.flows", version="1.0.0"
                )
                cls._meter = meter
                cls.flow_duration = meter.create_histogram(
                    name="flow.duration",
                    description="Duration of flow execution in milliseconds",
                    unit="ms",
                )
                cls.event_duration = meter.create_histogram(
                    name="event.duration",
                    description="Duration of event execution in milliseconds",
                    unit="ms",
                )
                cls.task_counter = meter.create_counter(
                    name="task.count", description="Number of tasks executed", unit="1"
                )
                cls.error_counter = meter.create_counter(
                    name="error.count",
                    description="Number of errors encountered",
                    unit="1",
                )
                cls.executor_usage = meter.create_counter(
                    name="executor.usage",
                    description="Number of times each executor type is used",
                    unit="1",
                )
                cls.parallel_task_gauge = meter.create_up_down_counter(
                    name="parallel_tasks.active",
                    description="Number of parallel tasks currently executing",
                    unit="1",
                )
                cls._initialized = True
                logger.info("Flow metrics initialized")
            except Exception as exc:
                logger.warning("Failed to initialize flow metrics: %s", exc)
                cls._initialized = False

    @classmethod
    def reset(cls) -> None:
        """Forget the instruments; the next use creates them again."""
        with cls._lock:
            cls._meter = None
            cls._initialized = False
            cls.flow_duration = None
            cls.event_duration = None
            cls.task_counter = None
            cls.error_counter = None
            cls.executor_usage = None
            cls.parallel_task_gauge = None

    @classmethod
    def record_flow_duration(cls, duration_ms: float, flow_type: str, status: str):
        """Record flow execution duration"""
        if cls.flow_duration:
            cls.flow_duration.record(
                duration_ms,
                attributes={"flow.type": flow_type, "flow.status": status},
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
        cls, error_type: str, flow_type: str, event_name: Optional[str] = None
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


def _parallel_count(flow) -> int:
    return sum(
        1
        for task in (getattr(flow, "task_profiles", None) or ())
        if getattr(task, "is_parallel_execution_node", False)
    )


class EventResultError(Exception):
    """An event finished without raising but reported an error result."""


def _outcome(
    future: "asyncio.Future", result_errors: bool = False
) -> typing.Tuple[str, Optional[BaseException]]:
    """('success' | 'error' | 'cancelled', the error) of a finished future.

    With ``result_errors`` a future that completed with an error *result* (events
    report failure as a result, not by raising) is an error too.
    """
    if future.cancelled():
        return "cancelled", asyncio.CancelledError()
    error = future.exception()
    if error is not None:
        return "error", error
    if result_errors:
        result = future.result()
        if getattr(result, "error", False) is True:
            return "error", EventResultError(str(getattr(result, "content", "")))
    return "success", None


def _end_when_done(
    span,
    future: Any,
    finished: typing.Callable[[str, Optional[BaseException]], None],
    result_errors: bool = False,
) -> None:
    """End ``span`` when ``future`` ends (now if it is not a future).

    ``finished(status, error)`` runs first, for the metrics.
    """

    def done(fut) -> None:
        try:
            status, error = _outcome(fut, result_errors)
        except Exception as exc:  # pragma: no cover - defensive
            status, error = "error", exc
        try:
            finished(status, error)
        except Exception:
            logger.debug("Flow metrics failed", exc_info=True)
        end_span_with(span, error)

    if hasattr(future, "add_done_callback") and hasattr(future, "done"):
        if future.done():
            done(future)
        else:
            future.add_done_callback(done)
    else:
        finished("success", None)
        end_span_with(span, None)


def instrument_flow_run(original_run):
    """
    Instrument a flow's ``run()`` (a coroutine function returning a future).

    The span is ended by the returned future; the work the flow submits inside
    ``run`` nests under it.
    """

    @functools.wraps(original_run)
    async def instrumented_run(self: "BaseFlow", *args: Any, **kwargs: Any):
        tracer = get_tracer()
        if tracer is None:
            return await original_run(self, *args, **kwargs)

        FlowMetrics.initialize()
        flow_type = self.__class__.__name__
        profiles = getattr(self, "task_profiles", None) or ()
        parallel = _parallel_count(self)
        started = time.perf_counter()

        span = tracer.start_span(
            f"flow.run.{flow_type}",
            context=OTelContextManager.get_async_context(),
            kind=trace.SpanKind.INTERNAL,
        )
        attrs = {"flow.type": flow_type, "flow.task_count": len(profiles)}
        try:
            attrs["flow.context_id"] = self.context.id
            attrs["flow.tasks"] = ",".join(t.get_event_name() for t in profiles)
        except Exception:
            pass
        if parallel:
            attrs["flow.parallel_tasks"] = parallel
            FlowMetrics.increment_parallel_tasks(parallel)
        set_attributes(span, attrs)
        span.add_event("flow_execution_started")

        def finished(status: str, error: Optional[BaseException]) -> None:
            duration_ms = (time.perf_counter() - started) * 1000
            span.add_event("flow_execution_completed", {"duration_ms": duration_ms})
            FlowMetrics.record_flow_duration(duration_ms, flow_type, status)
            if error is not None and status == "error":
                FlowMetrics.increment_error_count(type(error).__name__, flow_type)
            if parallel:
                FlowMetrics.decrement_parallel_tasks(parallel)

        try:
            with trace.use_span(span, end_on_exit=False):
                future = await original_run(self, *args, **kwargs)
        except BaseException as exc:
            finished(
                "cancelled" if isinstance(exc, asyncio.CancelledError) else "error", exc
            )
            end_span_with(span, exc)
            raise

        _end_when_done(span, future, finished)
        return future

    return instrumented_run


def instrument_get_initialized_event(original_get_initialized_event):
    """
    Instrument ``BaseFlow.get_initialized_event`` (a plain method: it looks the
    event up or builds it, and is called once per task *and* on cache hits, so it
    is traced but not counted).
    """

    def attributes(self, task_profile, *args, **kwargs):
        attrs = SpanHelper.task_attributes(task_profile)
        attrs["event.name"] = attrs.get("task.name")
        options = getattr(task_profile, "options", None)
        if options is not None and getattr(options, "retry_attempts", 0):
            attrs["event.retry_attempts"] = options.retry_attempts
        return attrs

    def on_finish(args, kwargs, duration, error):
        if error is not None:
            self, task_profile = args[0], args[1]
            FlowMetrics.initialize()
            FlowMetrics.increment_error_count(
                type(error).__name__,
                self.__class__.__name__,
                task_profile.get_event_name(),
            )

    def after(args, kwargs, result):
        event = result[0] if isinstance(result, tuple) and result else None
        return {"event.class": event.__class__.__name__ if event is not None else None}

    return traced(
        "flow.initialize_event",
        attributes=attributes,
        after=after,
        on_finish=on_finish,
    )(original_get_initialized_event)


def instrument_submit_event_to_executor(original_submit):
    """
    Instrument ``BaseFlow._submit_event_to_executor``.

    The span covers the event's *execution*: it is ended by the future that is
    returned (success, error or cancellation), so a failing event is an error
    span. Counting the task here counts each real submission once.
    """

    @functools.wraps(original_submit)
    async def instrumented_submit(
        self: "BaseFlow", executor, event: "Event", event_call_kwargs, *args, **kwargs
    ):
        tracer = get_tracer()
        if tracer is None:
            return await original_submit(
                self, executor, event, event_call_kwargs, *args, **kwargs
            )

        FlowMetrics.initialize()
        flow_type = self.__class__.__name__
        executor_type = executor.__class__.__name__
        event_name = event.__class__.__name__
        started = time.perf_counter()

        retry_policy = getattr(event, "retry_policy", None)
        span = tracer.start_span(
            f"event.execute.{event_name}", kind=trace.SpanKind.INTERNAL
        )
        set_attributes(
            span,
            {
                "event.name": event_name,
                "event.task_id": getattr(event, "_task_id", None),
                "executor.type": executor_type,
                "context.id": getattr(getattr(self, "context", None), "id", None),
                "event.retry.max_attempts": getattr(retry_policy, "max_attempts", None),
                "executor.max_workers": getattr(executor, "max_workers", None),
            },
        )
        span.add_event("event_submission_started")
        FlowMetrics.record_executor_usage(executor_type, flow_type)
        FlowMetrics.increment_task_count(event_name, flow_type)

        def finished(status: str, error: Optional[BaseException]) -> None:
            duration_ms = (time.perf_counter() - started) * 1000
            span.add_event(
                "event_execution_completed",
                {"duration_ms": duration_ms, "status": status},
            )
            FlowMetrics.record_event_duration(
                duration_ms, event_name, status, executor_type
            )
            if status == "error" and error is not None:
                FlowMetrics.increment_error_count(
                    type(error).__name__, flow_type, event_name
                )

        try:
            with trace.use_span(span, end_on_exit=False):
                future = await original_submit(
                    self, executor, event, event_call_kwargs, *args, **kwargs
                )
        except BaseException as exc:
            # (the submission itself failed: there is no future to wait for)
            finished("error", exc)
            end_span_with(span, exc)
            raise

        _end_when_done(span, future, finished, result_errors=True)
        return future

    return instrumented_submit


def instrument_map_events_to_executor(original_map_events):
    """Instrument ``BaseFlow._map_events_to_executor`` (a coroutine function)."""
    return traced(
        "flow.map_events_to_executor",
        attributes=lambda self, *a, **k: {
            "flow.type": self.__class__.__name__,
            "flow.task_count": len(getattr(self, "task_profiles", None) or ()),
        },
        result_attributes=lambda futures: {
            "flow.futures": len(futures) if hasattr(futures, "__len__") else None
        },
    )(original_map_events)


def patch_flow_class() -> int:
    """Instrument the methods ``BaseFlow`` defines (each once)."""
    from volnux.flows.base import BaseFlow

    patched = sum(
        (
            patch_attr(
                BaseFlow, "get_initialized_event", instrument_get_initialized_event
            ),
            patch_attr(
                BaseFlow,
                "_submit_event_to_executor",
                instrument_submit_event_to_executor,
            ),
            patch_attr(
                BaseFlow, "_map_events_to_executor", instrument_map_events_to_executor
            ),
        )
    )
    if patched:
        logger.info("BaseFlow instrumented with OpenTelemetry")
    return patched


def patch_concrete_flow_classes(*flow_classes: type) -> int:
    """Instrument ``run`` of the given flow classes, each class's own (a subclass
    that overrides ``run`` is patched separately from its base)."""
    patched = 0
    for flow_class in flow_classes:
        if "run" in flow_class.__dict__ and patch_attr(
            flow_class, "run", instrument_flow_run
        ):
            patched += 1
            logger.info("%s instrumented with OpenTelemetry", flow_class.__name__)
    return patched


def patch_all_flow_components() -> None:
    """Instrument the flow layer: ``BaseFlow`` and the concrete flows (once)."""
    from volnux.flows.parallel import ParallelFlow
    from volnux.flows.single import SingleFlow
    from volnux.flows.stream import StreamFlow

    patch_flow_class()
    patch_concrete_flow_classes(SingleFlow, ParallelFlow, StreamFlow)
    logger.info("All Flow components instrumented with OpenTelemetry")


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
