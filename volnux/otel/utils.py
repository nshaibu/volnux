import logging
from typing import Any, Dict, Optional

from opentelemetry import trace

logger = logging.getLogger(__name__)


def get_current_trace_id() -> Optional[str]:
    """
    Get the current trace ID.

    Useful for logging or debugging.

    Returns:
        Trace ID as hex string, or None if no active trace

    Example:
        >>> from volnux.otel.utils import get_current_trace_id
        >>> trace_id = get_current_trace_id()
        >>> logger.info(f"Processing request with trace_id={trace_id}")
    """
    try:

        span = trace.get_current_span()
        if span and span.is_recording():
            trace_id = span.get_span_context().trace_id
            return format(trace_id, "032x")
    except Exception as e:
        logger.debug(f"Failed to get trace ID: {e}")

    return None


def get_current_span_id() -> Optional[str]:
    """
    Get the current span ID.

    Returns:
        Span ID as hex string, or None if no active span
    """
    try:

        span = trace.get_current_span()
        if span and span.is_recording():
            span_id = span.get_span_context().span_id
            return format(span_id, "016x")
    except Exception as e:
        logger.debug(f"Failed to get span ID: {e}")

    return None


def add_span_attribute(key: str, value: Any) -> None:
    """
    Add an attribute to the current span.

    Args:
        key: Attribute key
        value: Attribute value

    Example:
        >>> from volnux.otel.utils import add_span_attribute
        >>> add_span_attribute("user.id", "12345")
        >>> add_span_attribute("records.processed", 1000)
    """
    try:

        span = trace.get_current_span()
        if span and span.is_recording():
            span.set_attribute(key, str(value))
    except Exception as e:
        logger.debug(f"Failed to add span attribute: {e}")


def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
    """
    Add an event to the current span.

    Args:
        name: Event name
        attributes: Event attributes

    Example:
        >>> from volnux.otel.utils import add_span_event
        >>> add_span_event("data_validated", {
        ...     "records": 1000,
        ...     "invalid": 5
        ... })
    """
    try:
        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(name, attributes or {})
    except Exception as e:
        logger.debug(f"Failed to add span event: {e}")
