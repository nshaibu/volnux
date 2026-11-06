"""
Carrying a trace to a worker or a pod.

``volnux.context`` puts the context of a run (workflow id, execution id, the
non-secret configuration) in the carrier that is sent to a Celery worker or a
Kubernetes Job. This module adds the active trace to it: the snapshot gets the W3C
``traceparent`` (and ``tracestate``) of the current span, and a context that is
activated from a snapshot makes that trace the parent of what runs inside it. The
spans of the worker therefore join the trace of the dispatcher.

``initialize_otel`` installs it and ``shutdown_otel`` removes it; ``volnux.context``
does not import OpenTelemetry.
"""

import contextlib
import logging
from typing import Dict, Iterator, Optional

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from volnux.context import TracePropagation, register_trace_propagation

logger = logging.getLogger(__name__)

__all__ = [
    "current_trace_context",
    "attach_trace_context",
    "install_trace_propagation",
    "remove_trace_propagation",
]

_propagator = TraceContextTextMapPropagator()


def current_trace_context() -> Optional[Dict[str, str]]:
    """The active span as W3C trace context (``traceparent`` / ``tracestate``), or
    None when there is no valid active span."""
    span = trace.get_current_span()
    if not span.get_span_context().is_valid:
        return None
    carrier: Dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier or None


@contextlib.contextmanager
def attach_trace_context(carrier: Dict[str, str]) -> Iterator[None]:
    """Make the trace in ``carrier`` the parent of the spans started inside the
    block (the previous context is restored on exit)."""
    extracted = _propagator.extract(carrier)
    token = otel_context.attach(extracted)
    try:
        yield
    finally:
        otel_context.detach(token)


def install_trace_propagation() -> None:
    """Register the hook with ``volnux.context``."""
    register_trace_propagation(
        TracePropagation(inject=current_trace_context, attach=attach_trace_context)
    )


def remove_trace_propagation() -> None:
    """Unregister it."""
    register_trace_propagation(None)
