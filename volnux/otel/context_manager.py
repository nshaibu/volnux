"""
OpenTelemetry Context Manager for Volnux Execution Contexts
Handles context propagation through the execution chain
"""

import logging
import typing
from contextvars import ContextVar
from opentelemetry import trace, context
from opentelemetry.trace import Status, StatusCode, Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

# Context variable to store OTel context across async boundaries
_otel_context_var: ContextVar[typing.Optional[context.Context]] = ContextVar(
    "volnux_otel_context", default=None
)
# What each set_async_context replaced, innermost last: clear_async_context puts
# the previous value back, so nested contexts unwind to their parent (clearing to
# None lost the outer context for the rest of the outer dispatch).
_otel_context_stack: ContextVar[typing.Tuple[typing.Optional[context.Context], ...]] = (
    ContextVar("volnux_otel_context_stack", default=())
)

#: Attribute names that hold a secret: their values are never put on a span.
SENSITIVE_NAME_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "authorization",
    "api_key",
    "apikey",
    "private_key",
)


class OTelContextManager:
    """
    Manages OpenTelemetry context propagation for Volnux execution contexts.

    This class ensures that:
    1. Each ExecutionContext has an associated OTel span
    2. Context is propagated through the execution chain (previous/next)
    3. Parallel tasks maintain proper parent-child relationships
    4. Context is serialized for distributed execution
    """

    SPAN_KEY = "_otel_span"
    CONTEXT_KEY = "_otel_context"
    PARENT_CONTEXT_KEY = "_otel_parent_context"

    @classmethod
    def attach_span_to_context(
        cls,
        execution_context: "ExecutionContext",
        span: Span,
        otel_context: typing.Optional[context.Context] = None,
    ) -> None:
        """
        Attach OpenTelemetry span and context to an ExecutionContext.

        Args:
            execution_context: The Volnux execution context
            span: The OpenTelemetry span
            otel_context: The OpenTelemetry context (optional)
        """
        if otel_context is None:
            otel_context = trace.set_span_in_context(span)

        # Store span and context on the execution context. An object that cannot
        # take the attributes (slots, frozen) just does not carry them.
        try:
            setattr(execution_context, cls.SPAN_KEY, span)
            setattr(execution_context, cls.CONTEXT_KEY, otel_context)
        except (AttributeError, TypeError, ValueError):
            logger.debug(
                "Cannot attach the span to %s", type(execution_context).__name__
            )

    @classmethod
    def get_span_from_context(
        cls, execution_context: "ExecutionContext"
    ) -> typing.Optional[Span]:
        """
        Retrieve the OpenTelemetry span from an ExecutionContext.

        Args:
            execution_context: The Volnux execution context

        Returns:
            The associated span or None
        """
        return getattr(execution_context, cls.SPAN_KEY, None)

    @classmethod
    def get_otel_context_from_execution_context(
        cls, execution_context: "ExecutionContext"
    ) -> typing.Optional[context.Context]:
        """
        Retrieve the OpenTelemetry context from an ExecutionContext.

        Args:
            execution_context: The Volnux execution context

        Returns:
            The associated OTel context or None
        """
        return getattr(execution_context, cls.CONTEXT_KEY, None)

    @classmethod
    def get_parent_context(
        cls, execution_context: "ExecutionContext"
    ) -> typing.Optional[context.Context]:
        """
        Get the parent OpenTelemetry context from the previous ExecutionContext.

        This ensures proper span hierarchy in the execution chain.

        Args:
            execution_context: The current execution context

        Returns:
            Parent OTel context or None
        """
        previous = getattr(execution_context, "previous_context", None)
        if previous:
            parent = cls.get_otel_context_from_execution_context(previous)
            if parent is not None:
                return parent
        # (what propagate_context_to_next stored, for a context linked after the
        # previous one had finished)
        return getattr(execution_context, cls.PARENT_CONTEXT_KEY, None)

    @classmethod
    def propagate_context_to_next(
        cls, current_context: "ExecutionContext", next_context: "ExecutionContext"
    ) -> None:
        """
        Propagate OpenTelemetry context from current to next ExecutionContext.

        Args:
            current_context: The current execution context
            next_context: The next execution context in the chain
        """
        current_otel_context = cls.get_otel_context_from_execution_context(
            current_context
        )
        if current_otel_context:
            try:
                setattr(next_context, cls.PARENT_CONTEXT_KEY, current_otel_context)
            except (AttributeError, TypeError, ValueError):
                logger.debug("Cannot propagate the trace to the next context")

    @classmethod
    def serialize_context(
        cls, execution_context: "ExecutionContext"
    ) -> typing.Dict[str, str]:
        """
        Serialize OpenTelemetry context for distributed execution.

        This is useful when tasks are executed across different processes/workers.

        Args:
            execution_context: The execution context with OTel context

        Returns:
            Dictionary with serialized trace context (W3C format)
        """
        otel_context = cls.get_otel_context_from_execution_context(execution_context)
        if not otel_context:
            return {}

        carrier = {}
        TraceContextTextMapPropagator().inject(carrier, context=otel_context)
        return carrier

    @classmethod
    def deserialize_context(
        cls, carrier: typing.Dict[str, str]
    ) -> typing.Optional[context.Context]:
        """
        Deserialize OpenTelemetry context from distributed execution.

        Args:
            carrier: Dictionary with serialized trace context

        Returns:
            Reconstructed OTel context or None
        """
        if not carrier:
            return None

        return TraceContextTextMapPropagator().extract(carrier)

    @classmethod
    def set_async_context(cls, otel_context: context.Context) -> None:
        """
        Set the OpenTelemetry context for async execution.

        Args:
            otel_context: The OTel context to set
        """
        _otel_context_stack.set((*_otel_context_stack.get(), _otel_context_var.get()))
        _otel_context_var.set(otel_context)

    @classmethod
    def get_async_context(cls) -> typing.Optional[context.Context]:
        """
        Get the current OpenTelemetry context for async execution.

        Returns:
            Current OTel context or None
        """
        return _otel_context_var.get()

    @classmethod
    def clear_async_context(cls) -> None:
        """Undo the last :meth:`set_async_context` (back to the context it replaced,
        None if there is none)."""
        stack = _otel_context_stack.get()
        if not stack:
            _otel_context_var.set(None)
            return
        _otel_context_stack.set(stack[:-1])
        _otel_context_var.set(stack[-1])


def _is_sensitive(name: str) -> bool:
    lowered = name.lower()
    return any(part in lowered for part in SENSITIVE_NAME_PARTS)


class SpanHelper:
    """Helper utilities for working with OpenTelemetry spans.

    The ``*_attributes`` functions build the attributes; ``add_*`` put them on a
    span. Values OpenTelemetry cannot hold (``None``, objects) are cleaned, and an
    attribute that cannot be read is left out: describing a span never fails the
    code that is being traced.
    """

    @staticmethod
    def set_status_from_execution(
        span: Span, execution_context: "ExecutionContext"
    ) -> None:
        """
        Set span status based on ExecutionContext state.

        Args:
            span: The OpenTelemetry span
            execution_context: The execution context with state
        """
        from volnux.execution.status import ExecutionStatus
        from volnux.otel.instrumentations._common import record_error

        status = execution_context.status
        if status == ExecutionStatus.COMPLETED:
            span.set_status(Status(StatusCode.OK))
        elif status == ExecutionStatus.FAILED:
            # Add errors as events (a stored error may be text, not an exception)
            for error in execution_context.errors:
                record_error(span, error)
            span.set_status(Status(StatusCode.ERROR, "Execution failed"))
        elif status == ExecutionStatus.CANCELLED:
            span.set_status(Status(StatusCode.ERROR, "Execution cancelled"))
        elif status == ExecutionStatus.ABORTED:
            span.set_status(Status(StatusCode.ERROR, "Execution aborted"))

    @staticmethod
    def task_attributes(task) -> typing.Dict[str, typing.Any]:
        attributes: typing.Dict[str, typing.Any] = {}
        for key, read in (
            ("task.id", lambda: str(task.get_id())),
            ("task.name", lambda: task.get_event_name()),
            ("task.is_conditional", lambda: getattr(task, "is_conditional", None)),
            (
                "task.is_parallel",
                lambda: getattr(task, "is_parallel_execution_node", False),
            ),
            ("task.descriptor", lambda: str(task.descriptor)),
        ):
            try:
                attributes[key] = read()
            except Exception:
                continue
        if getattr(task, "is_sink", False):
            attributes["task.is_sink"] = True
        return attributes

    @staticmethod
    def add_task_attributes(span: Span, task) -> None:
        """
        Add task-specific attributes to a span.

        Args:
            span: The OpenTelemetry span
            task: The task object
        """
        from volnux.otel.instrumentations._common import set_attributes

        set_attributes(span, SpanHelper.task_attributes(task))

    @staticmethod
    def context_attributes(execution_context) -> typing.Dict[str, typing.Any]:
        attributes: typing.Dict[str, typing.Any] = {}

        def put(key: str, read: typing.Callable[[], typing.Any]) -> None:
            try:
                attributes[key] = read()
            except Exception:
                pass

        put("context.id", lambda: execution_context.id)
        put("context.is_multitask", lambda: execution_context.is_multitask())
        if getattr(execution_context, "pipeline", None):
            put("context.pipeline_id", lambda: execution_context.pipeline.id)
        try:
            task_profiles = execution_context.get_task_profiles()
            attributes["context.task_count"] = len(task_profiles)
            attributes["context.tasks"] = ",".join(
                task.get_event_name() for task in task_profiles
            )
        except Exception:
            pass
        put("context.depth", lambda: execution_context.get_depth())
        if getattr(execution_context, "previous_context", None):
            put("context.previous_id", lambda: execution_context.previous_context.id)
        return attributes

    @staticmethod
    def add_context_attributes(
        span: Span, execution_context: "ExecutionContext"
    ) -> None:
        """
        Add ExecutionContext attributes to a span.

        Args:
            span: The OpenTelemetry span
            execution_context: The execution context
        """
        from volnux.otel.instrumentations._common import set_attributes

        set_attributes(span, SpanHelper.context_attributes(execution_context))

    @staticmethod
    def pipeline_attributes(pipeline) -> typing.Dict[str, typing.Any]:
        attributes: typing.Dict[str, typing.Any] = {
            "pipeline.id": getattr(pipeline, "id", None),
            "pipeline.class": pipeline.__class__.__name__,
        }
        try:
            fields = list(pipeline.get_fields())
        except Exception:
            fields = []
        for name, _field in fields:
            value = getattr(pipeline, name, None)
            if value is None:
                continue
            # (inputs are on every trace: never a secret, and never unbounded)
            attributes[f"pipeline.input.{name}"] = (
                "[redacted]" if _is_sensitive(name) else str(value)
            )
        return attributes

    @staticmethod
    def add_pipeline_attributes(span: Span, pipeline) -> None:
        """
        Add Pipeline attributes to a span. Inputs whose name suggests a secret
        (password, token, ...) are recorded as ``[redacted]``; long values are cut.

        Args:
            span: The OpenTelemetry span
            pipeline: The Pipeline object
        """
        from volnux.otel.instrumentations._common import set_attributes

        set_attributes(span, SpanHelper.pipeline_attributes(pipeline))
