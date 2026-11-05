"""
OpenTelemetry instrumentation of ExecutionContext and ExecutionCoordinator.

Every context method that changes or runs a context gets a ``context.<method>``
span (``dispatch`` also parents on the previous context of the chain, so a run
reads as one trace); the coordinator's ``_execute_async`` gets
``coordinator.execute_async``.

Each wrapper has the same call shape as the method it wraps (it forwards
``*args, **kwargs``) and the same kind (a coroutine function stays one).
"""

import asyncio
import logging
import functools
import typing
from typing import Any, Dict

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from volnux.execution.coordinator import ExecutionTimeoutError
from volnux.otel.context_manager import OTelContextManager, SpanHelper
from ._common import (
    get_tracer,
    mark_interrupted,
    record_error,
    set_attributes,
    traced,
)
from ._patching import patch_attr

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext
    from volnux.execution.coordinator import ExecutionCoordinator

logger = logging.getLogger(__name__)


def _safe_span_name(*parts: str) -> str:
    return ".".join(p for p in parts if p)


def _add_execution_context_attributes(
    span, execution_context: "ExecutionContext"
) -> None:
    SpanHelper.add_context_attributes(span, execution_context)


def _context_attrs(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return SpanHelper.context_attributes(self)


def _state_attrs(args, kwargs, result) -> Dict[str, Any]:
    """What a context holds after a method ran."""
    self = args[0]
    return {
        "context.current_status": self.status.value,
        "context.errors_count": len(self.errors),
        "context.results_count": len(self.results),
    }


def instrument_execution_context_dispatch(original_dispatch):
    """
    Instrument ``ExecutionContext.dispatch`` (a coroutine function).

    The span is parented on the previous context of the chain and made the async
    context while the context runs, so the coordinator and flows below it nest
    under it.
    """

    @functools.wraps(original_dispatch)
    async def instrumented_dispatch(self: "ExecutionContext", *args, **kwargs):
        tracer = get_tracer()
        if tracer is None:
            return await original_dispatch(self, *args, **kwargs)

        parent_otel_context = OTelContextManager.get_parent_context(self)
        timeout = kwargs.get("timeout", args[0] if args else None)

        with tracer.start_as_current_span(
            "context.dispatch",
            context=parent_otel_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            _add_execution_context_attributes(span, self)
            if timeout is not None:
                set_attributes(span, {"context.timeout": timeout})

            current_otel_context = trace.set_span_in_context(span)
            OTelContextManager.attach_span_to_context(self, span, current_otel_context)
            OTelContextManager.set_async_context(current_otel_context)
            span.add_event("context.dispatch_started")
            try:
                result = await original_dispatch(self, *args, **kwargs)

                span.add_event("context.dispatch_completed")
                try:
                    set_attributes(
                        span,
                        {
                            "context.duration_ms": (self.metrics.duration or 0) * 1000,
                            "context.results_count": len(self.results),
                            "context.errors_count": len(self.errors),
                            "context.status": self.status.value,
                        },
                    )
                    SpanHelper.set_status_from_execution(span, self)
                    if self.next_context:
                        OTelContextManager.propagate_context_to_next(
                            self, self.next_context
                        )
                except Exception:
                    logger.debug("Could not summarise the dispatch", exc_info=True)
                return result
            except BaseException as exc:
                mark_interrupted(span, exc)
                raise
            finally:
                OTelContextManager.clear_async_context()

    return instrumented_dispatch


def instrument_execution_context_spawn_child(original_spawn_child):
    """Instrument ``ExecutionContext.spawn_child`` (a coroutine function)."""
    return traced(
        "context.spawn_child",
        attributes=lambda self, task_profiles=(), *a, **k: {
            **SpanHelper.context_attributes(self),
            "child.task_count": len(task_profiles),
        },
        result_attributes=lambda child: {
            "child.context_id": getattr(child, "id", None),
            "child.depth": child.get_depth() if hasattr(child, "get_depth") else None,
        },
    )(original_spawn_child)


def _method_instrumenter(span_name: str, **options: Any):
    """An ``instrument_*`` function: wraps a method in ``span_name``."""

    def instrument(original):
        return traced(span_name, attributes=_context_attrs, **options)(original)

    return instrument


instrument_execution_context_update_status = _method_instrumenter(
    "context.update_status",
    after=_state_attrs,
)
instrument_execution_context_update_status_async = _method_instrumenter(
    "context.update_status_async",
    after=_state_attrs,
)
instrument_execution_context_cancel = _method_instrumenter(
    "context.cancel", after=_state_attrs
)
instrument_execution_context_cancel_async = _method_instrumenter(
    "context.cancel_async", after=_state_attrs
)
instrument_execution_context_abort = _method_instrumenter(
    "context.abort", after=_state_attrs
)
instrument_execution_context_abort_async = _method_instrumenter(
    "context.abort_async", after=_state_attrs
)
instrument_execution_context_failed = _method_instrumenter(
    "context.failed", after=_state_attrs
)
instrument_execution_context_failed_async = _method_instrumenter(
    "context.failed_async", after=_state_attrs
)
instrument_execution_context_save = _method_instrumenter(
    "context.save",
    after=lambda args, kwargs, result: {"context.status": args[0].status.value},
)


def instrument_execution_context_add_error(original_add_error):
    """Instrument ``ExecutionContext.add_error``: the error is recorded on the span."""

    @functools.wraps(original_add_error)
    def instrumented_add_error(self: "ExecutionContext", error, *args, **kwargs):
        tracer = get_tracer()
        if tracer is None:
            return original_add_error(self, error, *args, **kwargs)

        with tracer.start_as_current_span(
            "context.add_error", kind=trace.SpanKind.INTERNAL
        ) as span:
            _add_execution_context_attributes(span, self)
            record_error(span, error)
            return original_add_error(self, error, *args, **kwargs)

    return instrumented_add_error


def instrument_execution_context_add_result(original_add_result):
    """Instrument ``ExecutionContext.add_result``."""
    return traced(
        "context.add_result",
        attributes=lambda self, result=None, *a, **k: {
            **SpanHelper.context_attributes(self),
            "context.result_type": result.__class__.__name__,
        },
        after=lambda args, kwargs, result: {
            "context.results_count": len(args[0].results)
        },
    )(original_add_result)


def instrument_coordinator_execute_async(original_execute_async):
    """Instrument ``ExecutionCoordinator._execute_async`` (a coroutine function)."""

    @functools.wraps(original_execute_async)
    async def instrumented_execute_async(self: "ExecutionCoordinator", *args, **kw):
        tracer = get_tracer()
        if tracer is None:
            return await original_execute_async(self, *args, **kw)

        with tracer.start_as_current_span(
            "coordinator.execute_async",
            context=OTelContextManager.get_async_context(),
            kind=trace.SpanKind.INTERNAL,
            # (the status is set below: a timeout reads "Execution timeout", which
            # the span's own handling of the exception would overwrite)
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            timeout = getattr(self, "_timeout", None)
            if timeout is not None:
                set_attributes(span, {"coordinator.timeout": timeout})
            span.add_event("coordinator_started_async")
            try:
                result = await original_execute_async(self, *args, **kw)
            except (asyncio.TimeoutError, ExecutionTimeoutError) as exc:
                # (_execute_async turns a raw TimeoutError into
                # ExecutionTimeoutError; both read as a timeout)
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "Execution timeout"))
                raise
            except BaseException as exc:
                if isinstance(exc, Exception):
                    record_error(span, exc)
                else:
                    mark_interrupted(span, exc)
                raise
            span.add_event("coordinator_completed_async")
            span.set_status(Status(StatusCode.OK))
            return result

    return instrumented_execute_async


# method name -> instrumenter, for the ExecutionContext
_CONTEXT_PATCHES = {
    "dispatch": instrument_execution_context_dispatch,
    "spawn_child": instrument_execution_context_spawn_child,
    "update_status": instrument_execution_context_update_status,
    "update_status_async": instrument_execution_context_update_status_async,
    "add_error": instrument_execution_context_add_error,
    "add_result": instrument_execution_context_add_result,
    "cancel": instrument_execution_context_cancel,
    "cancel_async": instrument_execution_context_cancel_async,
    "abort": instrument_execution_context_abort,
    "abort_async": instrument_execution_context_abort_async,
    "failed": instrument_execution_context_failed,
    "failed_async": instrument_execution_context_failed_async,
    "save": instrument_execution_context_save,
}


def patch_execution_context() -> int:
    """Instrument the ExecutionContext methods (each once). Returns how many were
    patched by this call."""
    from volnux.execution.context import ExecutionContext

    patched = sum(
        patch_attr(ExecutionContext, name, instrument)
        for name, instrument in _CONTEXT_PATCHES.items()
    )
    if patched:
        logger.info("ExecutionContext instrumented with OpenTelemetry")
    return patched


def patch_execution_coordinator() -> bool:
    """Instrument ``ExecutionCoordinator._execute_async`` (once)."""
    from volnux.execution.coordinator import ExecutionCoordinator

    patched = patch_attr(
        ExecutionCoordinator, "_execute_async", instrument_coordinator_execute_async
    )
    if patched:
        logger.info("ExecutionCoordinator instrumented with OpenTelemetry")
    return patched


def patch_all_execution_components() -> None:
    """Patch all execution components for OpenTelemetry instrumentation."""
    patch_execution_context()
    patch_execution_coordinator()
    logger.info("All execution components instrumented with OpenTelemetry")


__all__ = [
    "instrument_execution_context_dispatch",
    "instrument_coordinator_execute_async",
    "patch_execution_context",
    "patch_execution_coordinator",
    "patch_all_execution_components",
]
