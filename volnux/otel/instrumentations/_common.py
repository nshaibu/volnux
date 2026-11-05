"""Building blocks shared by the instrumentations.

* :func:`get_tracer` — the tracer of the initialised :class:`VolnuxTracer`, or
  ``None`` (instrumentation then does nothing).
* :func:`clean_attributes` — span attributes OpenTelemetry accepts: ``None`` is
  dropped, a value of an unsupported type becomes its text, text is bounded.
* :func:`traced` — wraps a function (sync or async, the kind is kept) in a span.
* :func:`end_span_with` — the outcome of a span that outlives its call.

Telemetry never changes what the instrumented code does: a failure to build
attributes is swallowed, an exception of the wrapped function is recorded and
re-raised unchanged.
"""

import functools
import inspect
import logging
import time
from typing import Any, Callable, Dict, Mapping, Optional

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from ..tracer_setup import get_tracer as _volnux_tracer

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_ATTRIBUTE_LENGTH",
    "get_tracer",
    "clean_attributes",
    "set_attributes",
    "record_error",
    "traced",
    "end_span_with",
    "mark_interrupted",
]

#: Longest text put in a span attribute (longer is cut, with an ellipsis).
MAX_ATTRIBUTE_LENGTH = 256

_PRIMITIVES = (bool, int, float, str)


def get_tracer() -> Optional[trace.Tracer]:
    """The Volnux tracer, or None before ``initialize_otel`` / after shutdown."""
    try:
        return _volnux_tracer()
    except Exception:  # pragma: no cover - defensive
        return None


def _bounded(text: str) -> str:
    if len(text) <= MAX_ATTRIBUTE_LENGTH:
        return text
    return text[: MAX_ATTRIBUTE_LENGTH - 1] + "…"


def clean_attributes(attributes: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """``attributes`` as OpenTelemetry accepts them.

    ``None`` values are dropped (the SDK rejects them and warns), booleans, numbers
    and text are kept (text bounded), anything else becomes its text.
    """
    cleaned: Dict[str, Any] = {}
    for key, value in (attributes or {}).items():
        if value is None:
            continue
        if isinstance(value, str):
            cleaned[key] = _bounded(value)
        elif isinstance(value, _PRIMITIVES):
            cleaned[key] = value
        else:
            try:
                cleaned[key] = _bounded(str(value))
            except Exception:
                continue
    return cleaned


def set_attributes(span: Span, attributes: Optional[Mapping[str, Any]]) -> None:
    """Set ``attributes`` on ``span`` (cleaned); never raises."""
    try:
        for key, value in clean_attributes(attributes).items():
            span.set_attribute(key, value)
    except Exception:
        logger.debug("Could not set span attributes", exc_info=True)


def record_error(span: Span, error: Any) -> None:
    """Record ``error`` on ``span`` and mark it failed; accepts what is not an
    exception (a stored error may be text or a dict)."""
    try:
        if isinstance(error, BaseException):
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, _bounded(str(error))))
        else:
            span.add_event("exception", {"exception.message": _bounded(str(error))})
            span.set_status(Status(StatusCode.ERROR, _bounded(str(error))))
    except Exception:
        logger.debug("Could not record the error on the span", exc_info=True)


def mark_interrupted(span: Span, exc: BaseException) -> None:
    """A cancellation (a ``BaseException``) is not recorded by OpenTelemetry's span
    context manager, which only handles ``Exception``; mark the span ourselves."""
    if isinstance(exc, Exception):
        return
    try:
        span.add_event("cancelled", {"exception.type": type(exc).__name__})
        span.set_status(Status(StatusCode.ERROR, f"{type(exc).__name__}"))
    except Exception:
        logger.debug("Could not mark the span as interrupted", exc_info=True)


def end_span_with(span: Span, error: Optional[BaseException] = None) -> None:
    """Finish a span that was started with ``tracer.start_span`` (its work ended
    later than the call that started it)."""
    try:
        if error is not None:
            record_error(span, error)
        elif span.is_recording():
            span.set_status(Status(StatusCode.OK))
    finally:
        span.end()


def traced(
    span_name: "str | Callable[..., str]",
    *,
    attributes: Optional[Callable[..., Mapping[str, Any]]] = None,
    result_attributes: Optional[Callable[[Any], Mapping[str, Any]]] = None,
    kind: SpanKind = SpanKind.INTERNAL,
    after: Optional[Callable[..., Optional[Mapping[str, Any]]]] = None,
    on_finish: Optional[Callable[..., None]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator factory: run the function inside a span.

    :param span_name: the name, or a function of the call's arguments returning it.
    :param attributes: function of the call's arguments returning the attributes
        known before the call.
    :param result_attributes: function of the result returning more attributes.
    :param after: ``after(args, kwargs, result)`` returning attributes read from the
        state the call left behind (the object it ran on); called only on success.
    :param on_finish: ``on_finish(args, kwargs, duration_s, error)`` after the call
        (metrics); ``error`` is the exception or None. Its failures are swallowed.

    The wrapper has the kind of the function it wraps: a coroutine function stays
    one, so the span covers the whole awaited call. Without an initialised tracer
    the function is called as it is. ``CancelledError`` is recorded and re-raised.
    """

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        def start(args: tuple, kwargs: dict):
            tracer = get_tracer()
            if tracer is None:
                return None, None
            try:
                name = span_name(*args, **kwargs) if callable(span_name) else span_name
            except Exception:
                logger.debug("Could not name the span for %s", function, exc_info=True)
                return None, None
            try:
                attrs = attributes(*args, **kwargs) if attributes else None
            except Exception:
                # (the span is still made, just without what could not be read)
                logger.debug("Could not describe %s", function, exc_info=True)
                attrs = None
            return tracer, (name, attrs)

        def finish(span, args, kwargs, started, error, result=None) -> None:
            try:
                if error is None and result_attributes is not None:
                    set_attributes(span, result_attributes(result))
                if error is None and after is not None:
                    set_attributes(span, after(args, kwargs, result))
            except Exception:
                logger.debug("Could not read the result attributes", exc_info=True)
            if on_finish is not None:
                try:
                    on_finish(args, kwargs, time.perf_counter() - started, error)
                except Exception:
                    logger.debug("on_finish failed", exc_info=True)

        if inspect.iscoroutinefunction(function):

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                tracer, prepared = start(args, kwargs)
                if tracer is None:
                    return await function(*args, **kwargs)
                name, attrs = prepared
                started = time.perf_counter()
                with tracer.start_as_current_span(name, kind=kind) as span:
                    set_attributes(span, attrs)
                    try:
                        result = await function(*args, **kwargs)
                    except BaseException as exc:
                        mark_interrupted(span, exc)
                        finish(span, args, kwargs, started, exc)
                        raise
                    finish(span, args, kwargs, started, None, result)
                    return result

            return async_wrapper

        @functools.wraps(function)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer, prepared = start(args, kwargs)
            if tracer is None:
                return function(*args, **kwargs)
            name, attrs = prepared
            started = time.perf_counter()
            with tracer.start_as_current_span(name, kind=kind) as span:
                set_attributes(span, attrs)
                try:
                    result = function(*args, **kwargs)
                except BaseException as exc:
                    mark_interrupted(span, exc)
                    finish(span, args, kwargs, started, exc)
                    raise
                finish(span, args, kwargs, started, None, result)
                return result

        return sync_wrapper

    return decorate
