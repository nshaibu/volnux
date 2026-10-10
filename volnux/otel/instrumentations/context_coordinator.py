""" "
OpenTelemetry Instrumentation for ExecutionContext and ExecutionCoordinator
"""

import functools
import logging
import typing
import asyncio

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from volnux.otel.context_manager import OTelContextManager, SpanHelper
from volnux.otel.tracer_setup import get_tracer
from volnux.execution.coordinator import ExecutionTimeoutError

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


def instrument_execution_context_dispatch(original_dispatch):
    """
    Decorator to instrument ExecutionContext.dispatch() method.
    """

    @functools.wraps(original_dispatch)
    async def instrumented_dispatch(self: "ExecutionContext", timeout=None):
        tracer = get_tracer()
        if not tracer:
            return await original_dispatch(self, timeout)

        parent_otel_context = OTelContextManager.get_parent_context(self)

        with tracer.start_as_current_span(
            "context.dispatch",
            context=parent_otel_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)

                if timeout is not None:
                    span.set_attribute("context.timeout", timeout)

                current_otel_context = trace.set_span_in_context(span)
                OTelContextManager.attach_span_to_context(
                    self, span, current_otel_context
                )
                OTelContextManager.set_async_context(current_otel_context)

                span.add_event("context.dispatch_started")

                result = await original_dispatch(self, timeout)

                span.add_event("context.dispatch_completed")
                span.set_attribute("context.duration_ms", self.metrics.duration * 1000)

                span.set_attribute("context.results_count", len(self.results))
                span.set_attribute("context.errors_count", len(self.errors))
                span.set_attribute("context.status", self.status.value)

                SpanHelper.set_status_from_execution(span, self)

                if self.next_context:
                    OTelContextManager.propagate_context_to_next(
                        self, self.next_context
                    )

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
            finally:
                OTelContextManager.clear_async_context()

    return instrumented_dispatch


def instrument_execution_context_spawn_child(original_spawn_child):
    """
    Decorator to instrument ExecutionContext.spawn_child().
    """

    @functools.wraps(original_spawn_child)
    async def instrumented_spawn_child(self: "ExecutionContext", task_profiles):
        tracer = get_tracer()
        if not tracer:
            return await original_spawn_child(self, task_profiles)

        with tracer.start_as_current_span(
            "context.spawn_child",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.set_attribute("child.task_count", len(task_profiles))
                result = await original_spawn_child(self, task_profiles)

                span.set_attribute("child.context_id", result.id)
                span.set_attribute("child.depth", result.get_depth())
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_spawn_child


def instrument_execution_context_update_status(original_update_status):
    """
    Decorator to instrument ExecutionContext.update_status().
    """

    @functools.wraps(original_update_status)
    def instrumented_update_status(self: "ExecutionContext", new_status):
        tracer = get_tracer()
        if not tracer:
            return original_update_status(self, new_status)

        with tracer.start_as_current_span(
            "context.update_status",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.set_attribute("context.new_status", new_status.value)
                result = original_update_status(self, new_status)

                span.set_attribute("context.current_status", self.status.value)
                span.set_attribute("context.errors_count", len(self.errors))
                span.set_attribute("context.results_count", len(self.results))
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_update_status


def instrument_execution_context_update_status_async(original_update_status_async):
    """
    Decorator to instrument ExecutionContext.update_status_async().
    """

    @functools.wraps(original_update_status_async)
    async def instrumented_update_status_async(self: "ExecutionContext", new_status):
        tracer = get_tracer()
        if not tracer:
            return await original_update_status_async(self, new_status)

        with tracer.start_as_current_span(
            "context.update_status_async",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.set_attribute("context.new_status", new_status.value)
                result = await original_update_status_async(self, new_status)

                span.set_attribute("context.current_status", self.status.value)
                span.set_attribute("context.errors_count", len(self.errors))
                span.set_attribute("context.results_count", len(self.results))
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_update_status_async


def instrument_execution_context_add_error(original_add_error):
    """
    Decorator to instrument ExecutionContext.add_error().
    """

    @functools.wraps(original_add_error)
    def instrumented_add_error(self: "ExecutionContext", error):
        tracer = get_tracer()
        if not tracer:
            return original_add_error(self, error)

        with tracer.start_as_current_span(
            "context.add_error",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR, "context error recorded"))
                return original_add_error(self, error)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_add_error


def instrument_execution_context_add_result(original_add_result):
    """
    Decorator to instrument ExecutionContext.add_result().
    """

    @functools.wraps(original_add_result)
    def instrumented_add_result(self: "ExecutionContext", result):
        tracer = get_tracer()
        if not tracer:
            return original_add_result(self, result)

        with tracer.start_as_current_span(
            "context.add_result",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.set_attribute("context.result_type", result.__class__.__name__)
                response = original_add_result(self, result)
                span.set_attribute("context.results_count", len(self.results))
                span.set_status(Status(StatusCode.OK))
                return response
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_add_result


def instrument_execution_context_cancel(original_cancel):
    """
    Decorator to instrument ExecutionContext.cancel().
    """

    @functools.wraps(original_cancel)
    def instrumented_cancel(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return original_cancel(self)

        with tracer.start_as_current_span(
            "context.cancel",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = original_cancel(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_cancel


def instrument_execution_context_cancel_async(original_cancel_async):
    """
    Decorator to instrument ExecutionContext.cancel_async().
    """

    @functools.wraps(original_cancel_async)
    async def instrumented_cancel_async(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return await original_cancel_async(self)

        with tracer.start_as_current_span(
            "context.cancel_async",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = await original_cancel_async(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_cancel_async


def instrument_execution_context_abort(original_abort):
    """
    Decorator to instrument ExecutionContext.abort().
    """

    @functools.wraps(original_abort)
    def instrumented_abort(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return original_abort(self)

        with tracer.start_as_current_span(
            "context.abort",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = original_abort(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_abort


def instrument_execution_context_abort_async(original_abort_async):
    """
    Decorator to instrument ExecutionContext.abort_async().
    """

    @functools.wraps(original_abort_async)
    async def instrumented_abort_async(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return await original_abort_async(self)

        with tracer.start_as_current_span(
            "context.abort_async",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = await original_abort_async(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_abort_async


def instrument_execution_context_failed(original_failed):
    """
    Decorator to instrument ExecutionContext.failed().
    """

    @functools.wraps(original_failed)
    def instrumented_failed(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return original_failed(self)

        with tracer.start_as_current_span(
            "context.failed",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = original_failed(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.ERROR, "context marked failed"))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_failed


def instrument_execution_context_failed_async(original_failed_async):
    """
    Decorator to instrument ExecutionContext.failed_async().
    """

    @functools.wraps(original_failed_async)
    async def instrumented_failed_async(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return await original_failed_async(self)

        with tracer.start_as_current_span(
            "context.failed_async",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = await original_failed_async(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.ERROR, "context marked failed"))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_failed_async


def instrument_execution_context_create_snapshot(original_create_snapshot):
    """
    Decorator to instrument ExecutionContext.create_snapshot().
    """

    @functools.wraps(original_create_snapshot)
    async def instrumented_create_snapshot(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return await original_create_snapshot(self)

        with tracer.start_as_current_span(
            "context.snapshot",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                span.set_attribute("context.child_count", len(self.child_contexts))
                span.set_attribute("context.task_count", len(self.get_task_profiles()))
                result = await original_create_snapshot(self)

                span.set_attribute("snapshot.class", result.__class__.__name__)
                span.set_attribute(
                    "snapshot.version", getattr(result, "snapshot_version", "")
                )
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_create_snapshot


def instrument_execution_context_persist(original_persist):
    """
    Decorator to instrument ExecutionContext.persist().
    """

    @functools.wraps(original_persist)
    async def instrumented_persist(self: "ExecutionContext"):
        tracer = get_tracer()
        if not tracer:
            return await original_persist(self)

        with tracer.start_as_current_span(
            "context.persist",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                _add_execution_context_attributes(span, self)
                result = await original_persist(self)
                span.set_attribute("context.status", self.status.value)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_persist


def instrument_coordinator_execute(original_execute):
    """
    Decorator to instrument ExecutionCoordinator.execute().
    """

    @functools.wraps(original_execute)
    def instrumented_execute(self: "ExecutionCoordinator"):
        tracer = get_tracer()
        if not tracer:
            return original_execute(self)

        parent_context = OTelContextManager.get_async_context()

        with tracer.start_as_current_span(
            "coordinator.execute",
            context=parent_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                if getattr(self, "_timeout", None) is not None:
                    span.set_attribute("coordinator.timeout", self._timeout)

                span.add_event("coordinator_started")
                result = original_execute(self)
                span.add_event("coordinator_completed")
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_execute


def instrument_coordinator_execute_async(original_execute_async):
    """
    Decorator to instrument ExecutionCoordinator._execute_async().
    """

    @functools.wraps(original_execute_async)
    async def instrumented_execute_async(self: "ExecutionCoordinator"):
        tracer = get_tracer()
        if not tracer:
            return await original_execute_async(self)

        parent_context = OTelContextManager.get_async_context()

        with tracer.start_as_current_span(
            "coordinator.execute_async",
            context=parent_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                if getattr(self, "_timeout", None) is not None:
                    span.set_attribute("coordinator.timeout", self._timeout)

                span.add_event("coordinator_started_async")
                result = await original_execute_async(self)
                span.add_event("coordinator_completed_async")
                span.set_status(Status(StatusCode.OK))
                return result
            # _execute_async() already translates a raw asyncio.TimeoutError
            # into ExecutionTimeoutError internally, so that's what surfaces here.
            except (asyncio.TimeoutError, ExecutionTimeoutError) as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "Execution timeout"))
                raise
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_execute_async


def patch_execution_context():
    """
    Patch ExecutionContext class to add OpenTelemetry instrumentation.
    """
    from volnux.execution.context import ExecutionContext

    if not hasattr(ExecutionContext, "_otel_instrumented"):
        ExecutionContext._otel_instrumented = True

        ExecutionContext._original_dispatch = ExecutionContext.dispatch
        ExecutionContext.dispatch = instrument_execution_context_dispatch(
            ExecutionContext._original_dispatch
        )

        ExecutionContext._original_spawn_child = ExecutionContext.spawn_child
        ExecutionContext.spawn_child = instrument_execution_context_spawn_child(
            ExecutionContext._original_spawn_child
        )

        ExecutionContext._original_update_status = ExecutionContext.update_status
        ExecutionContext.update_status = instrument_execution_context_update_status(
            ExecutionContext._original_update_status
        )

        ExecutionContext._original_update_status_async = (
            ExecutionContext.update_status_async
        )
        ExecutionContext.update_status_async = (
            instrument_execution_context_update_status_async(
                ExecutionContext._original_update_status_async
            )
        )

        ExecutionContext._original_add_error = ExecutionContext.add_error
        ExecutionContext.add_error = instrument_execution_context_add_error(
            ExecutionContext._original_add_error
        )

        ExecutionContext._original_add_result = ExecutionContext.add_result
        ExecutionContext.add_result = instrument_execution_context_add_result(
            ExecutionContext._original_add_result
        )

        ExecutionContext._original_cancel = ExecutionContext.cancel
        ExecutionContext.cancel = instrument_execution_context_cancel(
            ExecutionContext._original_cancel
        )

        ExecutionContext._original_cancel_async = ExecutionContext.cancel_async
        ExecutionContext.cancel_async = instrument_execution_context_cancel_async(
            ExecutionContext._original_cancel_async
        )

        ExecutionContext._original_abort = ExecutionContext.abort
        ExecutionContext.abort = instrument_execution_context_abort(
            ExecutionContext._original_abort
        )

        ExecutionContext._original_abort_async = ExecutionContext.abort_async
        ExecutionContext.abort_async = instrument_execution_context_abort_async(
            ExecutionContext._original_abort_async
        )

        ExecutionContext._original_failed = ExecutionContext.failed
        ExecutionContext.failed = instrument_execution_context_failed(
            ExecutionContext._original_failed
        )

        ExecutionContext._original_failed_async = ExecutionContext.failed_async
        ExecutionContext.failed_async = instrument_execution_context_failed_async(
            ExecutionContext._original_failed_async
        )

        ExecutionContext._original_create_snapshot = ExecutionContext.create_snapshot
        ExecutionContext.create_snapshot = instrument_execution_context_create_snapshot(
            ExecutionContext._original_create_snapshot
        )

        ExecutionContext._original_persist = ExecutionContext.persist
        ExecutionContext.persist = instrument_execution_context_persist(
            ExecutionContext._original_persist
        )

        logger.info("ExecutionContext instrumented with OpenTelemetry")


def patch_execution_coordinator():
    """
    Patch ExecutionCoordinator class to add OpenTelemetry instrumentation.
    """
    from volnux.execution.coordinator import ExecutionCoordinator

    if not hasattr(ExecutionCoordinator, "_otel_instrumented"):
        ExecutionCoordinator._otel_instrumented = True

        # ExecutionCoordinator._original_execute = ExecutionCoordinator.execute
        # ExecutionCoordinator.execute = instrument_coordinator_execute(
        #     ExecutionCoordinator._original_execute
        # )

        ExecutionCoordinator._original_execute_async = (
            ExecutionCoordinator._execute_async
        )
        original_async = ExecutionCoordinator._original_execute_async

        async def wrapped_async(self):
            return await instrument_coordinator_execute_async(original_async)(self)

        ExecutionCoordinator._execute_async = wrapped_async
        logger.info("ExecutionCoordinator instrumented with OpenTelemetry")


def patch_all_execution_components():
    """
    Patch all execution components for OpenTelemetry instrumentation.
    """
    patch_execution_context()
    patch_execution_coordinator()
    logger.info("All execution components instrumented with OpenTelemetry")
