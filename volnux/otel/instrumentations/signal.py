"""
OpenTelemetry instrumentation of the pipeline and of Volnux's signals.

* ``Pipeline.start`` gets the root ``workflow.<Pipeline>`` span.
* Every signal Volnux emits is added as a ``signal.<name>`` event to the span that
  is current when it is emitted.
"""

import functools
import logging
import typing
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from volnux.otel.context_manager import SpanHelper
from ._common import (
    clean_attributes,
    get_tracer,
    mark_interrupted,
    record_error,
    set_attributes,
)
from ._patching import patch_attr

if typing.TYPE_CHECKING:
    from volnux.execution.pipeline import Pipeline

logger = logging.getLogger(__name__)


def _summarise_execution(span, execution_context) -> None:
    """Put the outcome of a run (status, counts, duration) on the workflow span."""
    from volnux.execution.status import ExecutionStatus

    if not execution_context:
        span.set_attribute("workflow.no_execution", True)
        span.set_status(Status(StatusCode.OK))
        return

    latest = execution_context.get_latest_context()
    total_duration = 0.0
    count = 0
    for ctx in execution_context:
        total_duration += ctx.metrics.duration or 0.0
        count += 1

    set_attributes(
        span,
        {
            "workflow.status": latest.status.name,
            "workflow.results_count": len(latest.results),
            "workflow.errors_count": len(latest.errors),
            "workflow.total_duration_ms": total_duration * 1000,
            "workflow.context_count": count,
        },
    )

    status = latest.status
    if status == ExecutionStatus.COMPLETED:
        span.set_status(Status(StatusCode.OK))
        span.add_event("workflow_completed")
    elif status == ExecutionStatus.FAILED:
        span.set_status(Status(StatusCode.ERROR, "Workflow failed"))
        span.add_event("workflow_failed")
        for error in latest.errors:
            record_error(span, error)
        # (record_error marks the span failed with the error's text)
        span.set_status(Status(StatusCode.ERROR, "Workflow failed"))
    elif status == ExecutionStatus.CANCELLED:
        span.set_status(Status(StatusCode.ERROR, "Workflow cancelled"))
        span.add_event("workflow_cancelled")
    elif status == ExecutionStatus.ABORTED:
        span.set_status(Status(StatusCode.ERROR, "Workflow aborted"))
        span.add_event("workflow_aborted")


def instrument_pipeline_start(original_start):
    """
    Wrap ``Pipeline.start()`` (a coroutine function) in the root workflow span.

    The wrapper is a coroutine function too and awaits the original, so the span
    lasts as long as the run. (It used to be a plain function that called the
    coroutine function without awaiting it, which broke every ``start()``.)
    """

    @functools.wraps(original_start)
    async def instrumented_start(self: "Pipeline", *args: Any, **kwargs: Any):
        tracer = get_tracer()
        if tracer is None:
            return await original_start(self, *args, **kwargs)

        force_rerun = kwargs.get("force_rerun", args[0] if args else False)
        with tracer.start_as_current_span(
            f"workflow.{self.__class__.__name__}",
            kind=trace.SpanKind.SERVER,  # the entry point of a run
            attributes=clean_attributes(
                {
                    "workflow.name": self.__class__.__name__,
                    "workflow.force_rerun": bool(force_rerun),
                }
            ),
        ) as span:
            try:
                SpanHelper.add_pipeline_attributes(span, self)
            except Exception:
                logger.debug("Could not describe the pipeline", exc_info=True)
            span.add_event("workflow_started")

            try:
                execution_context = await original_start(self, *args, **kwargs)
            except BaseException as exc:
                mark_interrupted(span, exc)
                span.add_event(
                    "workflow_error",
                    clean_attributes(
                        {
                            "exception.type": type(exc).__name__,
                            "exception.message": str(exc),
                        }
                    ),
                )
                raise  # (the span records it and is marked failed on the way out)

            try:
                _summarise_execution(span, execution_context)
            except Exception:
                logger.debug("Could not summarise the run", exc_info=True)
            return execution_context

    return instrumented_start


def patch_pipeline() -> bool:
    """Instrument ``Pipeline.start`` (once). Returns whether it patched."""
    from volnux.execution.pipeline import Pipeline

    patched = patch_attr(Pipeline, "start", instrument_pipeline_start)
    if patched:
        logger.info("Pipeline.start instrumented with OpenTelemetry")
    return patched


# --------------------------------------------------------------------- signals
def _names(task_profiles) -> str:
    return ",".join(task.get_event_name() for task in task_profiles or ())


def _context_id(execution_context) -> Optional[str]:
    return getattr(execution_context, "id", None) if execution_context else None


def _batch_id(batch) -> Optional[str]:
    batch_id = getattr(batch, "id", None)
    return None if batch_id is None else str(batch_id)


def _event_class(event) -> str:
    return event.__class__.__name__


# signal name -> function of the signal's keyword arguments returning the
# attributes of the event added to the current span
_SIGNAL_ATTRIBUTES: Dict[str, Callable[..., Mapping[str, Any]]] = {
    "pipeline_pre_init": lambda args, kwargs, **_: {
        "args_count": len(args),
        "kwargs_keys": ",".join(kwargs) if kwargs else "",
    },
    "pipeline_post_init": lambda pipeline, **_: {
        "pipeline.class": _event_class(pipeline),
        "pipeline.id": getattr(pipeline, "id", None),
    },
    "pipeline_execution_start": lambda pipeline, **_: {
        "pipeline.class": _event_class(pipeline),
        "pipeline.id": getattr(pipeline, "id", None),
    },
    "pipeline_execution_end": lambda execution_context, **_: {
        "context.id": _context_id(execution_context)
    },
    "pipeline_stop": lambda pipeline, execution_context, **_: {
        "pipeline.id": getattr(pipeline, "id", None),
        "context.id": _context_id(execution_context),
    },
    "pipeline_shutdown": lambda pipeline, execution_context, **_: {
        "pipeline.id": getattr(pipeline, "id", None),
        "context.id": _context_id(execution_context),
    },
    "event_init": lambda event, **_: {
        "event.class": _event_class(event),
        "event.task_id": getattr(event, "_task_id", None),
    },
    "event_execution_start": lambda event, execution_context, **_: {
        "event.class": _event_class(event),
        "context.id": _context_id(execution_context),
    },
    "event_execution_end": lambda event, execution_context, **_: {
        "event.class": _event_class(event),
        "context.id": _context_id(execution_context),
    },
    "event_execution_retry": lambda event, task_id, backoff, retry_count, max_attempts, **_: {
        "event.class": _event_class(event),
        "task_id": task_id,
        "retry_count": retry_count,
        "max_attempts": max_attempts,
        "backoff": backoff,
    },
    "event_execution_retry_done": lambda event, task_id, max_attempts, **_: {
        "event.class": _event_class(event),
        "task_id": task_id,
        "max_attempts": max_attempts,
    },
    "event_execution_failed": lambda task_profiles, execution_context, state, **_: {
        "tasks": _names(task_profiles),
        "context.id": _context_id(execution_context),
        "state": getattr(state, "name", state),
    },
    "event_execution_cancelled": lambda task_profiles, execution_context, state, **_: {
        "tasks": _names(task_profiles),
        "context.id": _context_id(execution_context),
        "state": getattr(state, "name", state),
    },
    "event_execution_aborted": lambda task_profiles, execution_context, state, **_: {
        "tasks": _names(task_profiles),
        "context.id": _context_id(execution_context),
        "state": getattr(state, "name", state),
    },
    "batch_pipeline_started": lambda batch, total_pipelines, timestamp, **_: {
        "batch.id": _batch_id(batch),
        "total_pipelines": total_pipelines,
        "timestamp": timestamp,
    },
    "batch_pipeline_finished": lambda batch, success_rate, total_duration, timestamp, **_: {
        "batch.id": _batch_id(batch),
        "success_rate": success_rate,
        "total_duration": total_duration,
        "timestamp": timestamp,
    },
    "pipeline_metrics_updated": lambda batch_id, active_count, completion_rate, timestamp, **_: {
        "batch.id": None if batch_id is None else str(batch_id),
        "active_count": active_count,
        "completion_rate": completion_rate,
        "timestamp": timestamp,
    },
}

# The first argument of the signal's handler is the sender, the rest are its
# keyword arguments (the names above).
SIGNAL_NAMES: Tuple[str, ...] = tuple(_SIGNAL_ATTRIBUTES)


def _make_handler(signal_name: str) -> Callable[..., None]:
    attributes = _SIGNAL_ATTRIBUTES[signal_name]

    def handler(sender, **kwargs):
        if get_tracer() is None:
            return
        span = trace.get_current_span()
        if span is None or not span.is_recording():
            return
        try:
            attrs = clean_attributes(attributes(**kwargs))
        except Exception:
            # a signal with other arguments than expected must not break its emitter
            logger.debug("signal.%s: unexpected arguments", signal_name, exc_info=True)
            return
        span.add_event(f"signal.{signal_name}", attrs)

    handler.__name__ = f"on_{signal_name}"
    handler.__qualname__ = f"SignalInstrumentation.on_{signal_name}"
    return handler


class SignalInstrumentation:
    """One ``on_<signal>`` hook per Volnux signal.

    Each adds a ``signal.<name>`` event to the current span, if there is a
    recording one; it does nothing without an initialised tracer.
    """


_HANDLERS: Dict[str, Callable[..., None]] = {}
for _name in SIGNAL_NAMES:
    _HANDLERS[_name] = _make_handler(_name)
    setattr(SignalInstrumentation, f"on_{_name}", staticmethod(_HANDLERS[_name]))
del _name


def connect_signal_instrumentation() -> int:
    """Connect the hooks to all of Volnux's signals (once; connecting again is a
    no-op). Returns the number of signals."""
    from volnux.signal import signals

    for name, handler in _HANDLERS.items():
        getattr(signals, name).connect(None, handler)
    logger.info(
        "Signal instrumentation connected for OpenTelemetry (%d signals)",
        len(_HANDLERS),
    )
    return len(_HANDLERS)


def disconnect_signal_instrumentation() -> int:
    """Disconnect the hooks again. Returns the number of signals."""
    from volnux.signal import signals

    for name, handler in _HANDLERS.items():
        getattr(signals, name).disconnect(None, handler)
    return len(_HANDLERS)


def patch_all_pipeline_components() -> None:
    """Instrument the pipeline and connect the signal hooks (once)."""
    patch_pipeline()
    connect_signal_instrumentation()
    logger.info("All pipeline components instrumented with OpenTelemetry")


__all__ = [
    "SIGNAL_NAMES",
    "SignalInstrumentation",
    "instrument_pipeline_start",
    "patch_pipeline",
    "connect_signal_instrumentation",
    "disconnect_signal_instrumentation",
    "patch_all_pipeline_components",
]
