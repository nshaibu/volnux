"""
OpenTelemetry Context Manager for Volnux Execution Contexts
Handles context propagation through the execution chain
"""

import typing
from contextvars import ContextVar
from opentelemetry import trace, context
from opentelemetry.trace import Status, StatusCode, Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

# Context variable to store OTel context across async boundaries
_otel_context_var: ContextVar[typing.Optional[context.Context]] = ContextVar(
    "volnux_otel_context", default=None
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
        # Store span and context on the execution context
        setattr(execution_context, cls.SPAN_KEY, span)

        if otel_context is None:
            otel_context = trace.set_span_in_context(span)

        setattr(execution_context, cls.CONTEXT_KEY, otel_context)

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
        if execution_context.previous_context:
            return cls.get_otel_context_from_execution_context(
                execution_context.previous_context
            )
        return None

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
            setattr(next_context, cls.PARENT_CONTEXT_KEY, current_otel_context)

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
        """Clear the async context variable."""
        _otel_context_var.set(None)


class SpanHelper:
    """Helper utilities for working with OpenTelemetry spans"""

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
        from volnux.execution.state_manager import ExecutionStatus

        state = execution_context.state

        if state.status == ExecutionStatus.COMPLETED:
            span.set_status(Status(StatusCode.OK))
        elif state.status == ExecutionStatus.FAILED:
            span.set_status(Status(StatusCode.ERROR, "Execution failed"))
            # Add errors as events
            for error in state.errors:
                span.record_exception(error)
        elif state.status == ExecutionStatus.CANCELLED:
            span.set_status(Status(StatusCode.ERROR, "Execution cancelled"))
        elif state.status == ExecutionStatus.ABORTED:
            span.set_status(Status(StatusCode.ERROR, "Execution aborted"))

    @staticmethod
    def add_task_attributes(span: Span, task) -> None:
        """
        Add task-specific attributes to a span.

        Args:
            span: The OpenTelemetry span
            task: The task object
        """
        span.set_attribute("task.id", str(task.get_id()))
        span.set_attribute("task.name", task.get_event_name())
        span.set_attribute("task.is_conditional", task.is_conditional)
        span.set_attribute(
            "task.is_parallel", getattr(task, "is_parallel_execution_node", False)
        )

        if hasattr(task, "descriptor"):
            span.set_attribute("task.descriptor", str(task.descriptor))

        if hasattr(task, "is_sink") and task.is_sink:
            span.set_attribute("task.is_sink", True)

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
        span.set_attribute("context.id", execution_context.id)
        span.set_attribute("context.is_multitask", execution_context.is_multitask())

        task_profiles = execution_context.get_task_profiles()
        span.set_attribute("context.task_count", len(task_profiles))

        task_names = [task.get_event_name() for task in task_profiles]
        span.set_attribute("context.tasks", ",".join(task_names))

        if execution_context.previous_context:
            span.set_attribute(
                "context.previous_id", execution_context.previous_context.id
            )

    @staticmethod
    def add_pipeline_attributes(span: Span, pipeline) -> None:
        """
        Add Pipeline attributes to a span.

        Args:
            span: The OpenTelemetry span
            pipeline: The Pipeline object
        """
        span.set_attribute("pipeline.id", pipeline.id)
        span.set_attribute("pipeline.class", pipeline.__class__.__name__)

        # Add input fields
        for name, field in pipeline.get_fields():
            value = getattr(pipeline, name, None)
            if value is not None:
                span.set_attribute(f"pipeline.input.{name}", str(value))
