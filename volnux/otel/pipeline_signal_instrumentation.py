"""
OpenTelemetry Instrumentation for Pipeline and Signal-based events
"""

import logging
import typing
import functools
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from volnux.otel.tracer_setup import get_tracer
from volnux.otel.context_manager import OTelContextManager, SpanHelper

if typing.TYPE_CHECKING:
    from volnux.pipeline import Pipeline

logger = logging.getLogger(__name__)


def instrument_pipeline_start(original_start):
    """
    Decorator to instrument Pipeline.start() method.

    This creates the root span for the entire workflow execution.
    """

    @functools.wraps(original_start)
    def instrumented_start(self: "Pipeline", force_rerun: bool = False):
        tracer = get_tracer()

        if not tracer:
            # No tracing configured
            return original_start(self, force_rerun)

        # Create root workflow span
        with tracer.start_as_current_span(
            f"workflow.{self.__class__.__name__}",
            kind=trace.SpanKind.SERVER,  # This is the entry point
            attributes={
                "workflow.name": self.__class__.__name__,
                "workflow.force_rerun": force_rerun,
            },
        ) as span:
            try:
                # Add pipeline attributes
                SpanHelper.add_pipeline_attributes(span, self)

                # Add custom attributes from pipeline fields
                for name, field in self.get_fields():
                    value = getattr(self, name, None)
                    if value is not None:
                        # Truncate long values
                        str_value = str(value)
                        if len(str_value) > 200:
                            str_value = str_value[:200] + "..."
                        span.set_attribute(f"workflow.input.{name}", str_value)

                span.add_event("workflow_started")

                # Execute original start method
                execution_context = original_start(self, force_rerun)

                if execution_context:
                    # Add execution summary
                    latest_context = execution_context.get_latest_context()
                    state = latest_context.state

                    span.set_attribute("workflow.status", state.status.name)
                    span.set_attribute("workflow.results_count", len(state.results))
                    span.set_attribute("workflow.errors_count", len(state.errors))

                    # Calculate total duration across all contexts
                    total_duration = 0.0
                    context_count = 0
                    for ctx in execution_context:
                        total_duration += ctx.metrics.duration
                        context_count += 1

                    span.set_attribute(
                        "workflow.total_duration_ms", total_duration * 1000
                    )
                    span.set_attribute("workflow.context_count", context_count)

                    # Set span status based on workflow outcome
                    from volnux.execution.state_manager import ExecutionStatus

                    if state.status == ExecutionStatus.COMPLETED:
                        span.set_status(Status(StatusCode.OK))
                        span.add_event("workflow_completed")
                    elif state.status == ExecutionStatus.FAILED:
                        span.set_status(Status(StatusCode.ERROR, "Workflow failed"))
                        span.add_event("workflow_failed")
                        # Add errors
                        for error in state.errors:
                            span.record_exception(error)
                    elif state.status == ExecutionStatus.CANCELLED:
                        span.set_status(Status(StatusCode.ERROR, "Workflow cancelled"))
                        span.add_event("workflow_cancelled")
                    elif state.status == ExecutionStatus.ABORTED:
                        span.set_status(Status(StatusCode.ERROR, "Workflow aborted"))
                        span.add_event("workflow_aborted")
                else:
                    span.set_attribute("workflow.no_execution", True)
                    span.set_status(Status(StatusCode.OK))

                return execution_context

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.add_event(
                    "workflow_error",
                    {"exception.type": type(e).__name__, "exception.message": str(e)},
                )
                raise

    return instrumented_start


def patch_pipeline():
    """
    Patch Pipeline class to add OpenTelemetry instrumentation.

    This should be called during application initialization.
    """
    from volnux.pipeline import Pipeline

    if not hasattr(Pipeline, "_original_start"):
        Pipeline._original_start = Pipeline.start
        Pipeline.start = instrument_pipeline_start(Pipeline._original_start)
        logger.info("Pipeline.start instrumented with OpenTelemetry")


# Signal-based instrumentation
class SignalInstrumentation:
    """
    Instrumentation hooks for Volnux signals.

    This allows automatic span events and logging based on signal emissions.
    """

    @staticmethod
    def on_pipeline_pre_init(sender, args, kwargs, **extra):
        """Hook for pipeline_pre_init signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_pre_init",
                {
                    "args_count": len(args),
                    "kwargs_keys": ",".join(kwargs.keys()) if kwargs else "",
                },
            )

    @staticmethod
    def on_pipeline_post_init(sender, pipeline, **extra):
        """Hook for pipeline_post_init signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_post_init",
                {
                    "pipeline.class": pipeline.__class__.__name__,
                    "pipeline.id": pipeline.id,
                },
            )

    @staticmethod
    def on_pipeline_execution_start(sender, pipeline, **extra):
        """Hook for pipeline_execution_start signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_execution_start",
                {
                    "pipeline.class": pipeline.__class__.__name__,
                    "pipeline.id": pipeline.id,
                },
            )

    @staticmethod
    def on_pipeline_execution_end(sender, execution_context, **extra):
        """Hook for pipeline_execution_end signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            if execution_context:
                span.add_event(
                    "signal.pipeline_execution_end",
                    {"context.id": execution_context.id},
                )

    @staticmethod
    def on_pipeline_stop(sender, pipeline, execution_context, **extra):
        """Hook for pipeline_stop signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_stop",
                {
                    "pipeline.id": pipeline.id,
                    "context.id": execution_context.id if execution_context else None,
                },
            )

    @staticmethod
    def on_pipeline_shutdown(sender, pipeline, execution_context, **extra):
        """Hook for pipeline_shutdown signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_shutdown",
                {
                    "pipeline.id": pipeline.id,
                    "context.id": execution_context.id if execution_context else None,
                },
            )

    @staticmethod
    def on_event_init(sender, event, init_kwargs, **extra):
        """Hook for event_init signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.event_init",
                {
                    "event.class": event.__class__.__name__,
                    "event.task_id": (
                        str(event.task_id) if hasattr(event, "task_id") else None
                    ),
                },
            )

    @staticmethod
    def on_event_execution_start(sender, event, execution_context, **extra):
        """Hook for event_execution_start signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.event_execution_start",
                {
                    "event.class": event.__class__.__name__,
                    "context.id": execution_context.id,
                },
            )

    @staticmethod
    def on_event_execution_end(sender, event, execution_context, **extra):
        """Hook for event_execution_end signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.event_execution_end",
                {
                    "event.class": event.__class__.__name__,
                    "context.id": execution_context.id,
                },
            )

    @staticmethod
    def on_event_execution_retry(
        sender,
        event,
        execution_context,
        task_id,
        backoff,
        retry_count,
        max_attempts,
        **extra,
    ):
        """Hook for event_execution_retry signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.event_execution_retry",
                {
                    "event.class": event.__class__.__name__,
                    "task_id": str(task_id),
                    "retry_count": retry_count,
                    "max_attempts": max_attempts,
                    "backoff": backoff,
                },
            )

    @staticmethod
    def on_event_execution_retry_done(
        sender, event, execution_context, task_id, max_attempts, **extra
    ):
        """Hook for event_execution_retry_done signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.event_execution_retry_done",
                {
                    "event.class": event.__class__.__name__,
                    "task_id": str(task_id),
                    "max_attempts": max_attempts,
                },
            )

    @staticmethod
    def on_event_execution_failed(
        sender, task_profiles, execution_context, state, **extra
    ):
        """Hook for event_execution_failed signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            task_names = [task.get_event_name() for task in task_profiles]
            span.add_event(
                "signal.event_execution_failed",
                {
                    "tasks": ",".join(task_names),
                    "context.id": execution_context.id,
                    "state": state.name,
                },
            )

    @staticmethod
    def on_event_execution_cancelled(
        sender, task_profiles, execution_context, state, **extra
    ):
        """Hook for event_execution_cancelled signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            task_names = [task.get_event_name() for task in task_profiles]
            span.add_event(
                "signal.event_execution_cancelled",
                {
                    "tasks": ",".join(task_names),
                    "context.id": execution_context.id,
                    "state": state.name,
                },
            )

    @staticmethod
    def on_event_execution_aborted(
        sender, task_profiles, execution_context, state, **extra
    ):
        """Hook for event_execution_aborted signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            task_names = [task.get_event_name() for task in task_profiles]
            span.add_event(
                "signal.event_execution_aborted",
                {
                    "tasks": ",".join(task_names),
                    "context.id": execution_context.id,
                    "state": state.name,
                },
            )

    @staticmethod
    def on_batch_pipeline_started(sender, batch, total_pipelines, timestamp, **extra):
        """Hook for batch_pipeline_started signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.batch_pipeline_started",
                {
                    "batch.id": str(batch.id) if hasattr(batch, "id") else None,
                    "total_pipelines": total_pipelines,
                    "timestamp": str(timestamp),
                },
            )

    @staticmethod
    def on_batch_pipeline_finished(
        sender, batch, metrics, success_rate, total_duration, timestamp, **extra
    ):
        """Hook for batch_pipeline_finished signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.batch_pipeline_finished",
                {
                    "batch.id": str(batch.id) if hasattr(batch, "id") else None,
                    "success_rate": success_rate,
                    "total_duration": total_duration,
                    "timestamp": str(timestamp),
                },
            )

    @staticmethod
    def on_pipeline_metrics_updated(
        sender, batch_id, metrics, active_count, completion_rate, timestamp, **extra
    ):
        """Hook for pipeline_metrics_updated signal"""
        tracer = get_tracer()
        if not tracer:
            return

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "signal.pipeline_metrics_updated",
                {
                    "batch.id": str(batch_id),
                    "active_count": active_count,
                    "completion_rate": completion_rate,
                    "timestamp": str(timestamp),
                },
            )


def connect_signal_instrumentation():
    """
    Connect signal handlers for OpenTelemetry instrumentation.

    This should be called during application initialization.
    Connects to ALL Volnux signals for comprehensive event tracking.
    """
    from volnux.signal.signals import (
        pipeline_pre_init,
        pipeline_post_init,
        pipeline_execution_start,
        pipeline_execution_end,
        pipeline_stop,
        pipeline_shutdown,
        event_init,
        event_execution_start,
        event_execution_end,
        event_execution_retry,
        event_execution_retry_done,
        event_execution_failed,
        event_execution_cancelled,
        event_execution_aborted,
        batch_pipeline_started,
        batch_pipeline_finished,
        pipeline_metrics_updated,
    )

    # Pipeline lifecycle signals
    pipeline_pre_init.connect(None, SignalInstrumentation.on_pipeline_pre_init)
    pipeline_post_init.connect(None, SignalInstrumentation.on_pipeline_post_init)
    pipeline_execution_start.connect(
        None, SignalInstrumentation.on_pipeline_execution_start
    )
    pipeline_execution_end.connect(
        None, SignalInstrumentation.on_pipeline_execution_end
    )
    pipeline_stop.connect(None, SignalInstrumentation.on_pipeline_stop)
    pipeline_shutdown.connect(None, SignalInstrumentation.on_pipeline_shutdown)

    # Event lifecycle signals
    event_init.connect(None, SignalInstrumentation.on_event_init)
    event_execution_start.connect(None, SignalInstrumentation.on_event_execution_start)
    event_execution_end.connect(None, SignalInstrumentation.on_event_execution_end)
    event_execution_retry.connect(None, SignalInstrumentation.on_event_execution_retry)
    event_execution_retry_done.connect(
        None, SignalInstrumentation.on_event_execution_retry_done
    )
    event_execution_failed.connect(
        None, SignalInstrumentation.on_event_execution_failed
    )
    event_execution_cancelled.connect(
        None, SignalInstrumentation.on_event_execution_cancelled
    )
    event_execution_aborted.connect(
        None, SignalInstrumentation.on_event_execution_aborted
    )

    # Batch pipeline signals
    batch_pipeline_started.connect(
        None, SignalInstrumentation.on_batch_pipeline_started
    )
    batch_pipeline_finished.connect(
        None, SignalInstrumentation.on_batch_pipeline_finished
    )
    pipeline_metrics_updated.connect(
        None, SignalInstrumentation.on_pipeline_metrics_updated
    )

    logger.info("Signal instrumentation connected for OpenTelemetry (16 signals)")


def patch_all_pipeline_components():
    """
    Patch all pipeline-related components for OpenTelemetry instrumentation.

    Call this once during application initialization.
    """
    patch_pipeline()
    connect_signal_instrumentation()
    logger.info("All pipeline components instrumented with OpenTelemetry")
