"""
OpenTelemetry Instrumentation for ExecutionContext and ExecutionCoordinator
"""

import logging
import typing
import functools
from opentelemetry import trace, context
from opentelemetry.trace import Status, StatusCode

from volnux.otel.tracer_setup import get_tracer
from volnux.otel.context_manager import OTelContextManager, SpanHelper

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext
    from volnux.execution.coordinator import ExecutionCoordinator

logger = logging.getLogger(__name__)


def instrument_execution_context_dispatch(original_dispatch):
    """
    Decorator to instrument ExecutionContext.dispatch() method.

    This creates a span for the context dispatch and ensures proper
    context propagation through the execution chain.
    """

    @functools.wraps(original_dispatch)
    def instrumented_dispatch(self: "ExecutionContext", timeout=None):
        tracer = get_tracer()

        if not tracer:
            # No tracing configured
            return original_dispatch(self, timeout)

        # Get parent context from previous ExecutionContext
        parent_otel_context = OTelContextManager.get_parent_context(self)

        # Start span with parent context
        with tracer.start_as_current_span(
            "context.dispatch",
            context=parent_otel_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                # Add context attributes
                SpanHelper.add_context_attributes(span, self)

                # Add timeout if specified
                if timeout:
                    span.set_attribute("context.timeout", timeout)

                # Store span and context on ExecutionContext
                current_otel_context = trace.set_span_in_context(span)
                OTelContextManager.attach_span_to_context(
                    self, span, current_otel_context
                )

                # Set async context for coordinator
                OTelContextManager.set_async_context(current_otel_context)

                # Record dispatch start
                self.metrics.start_time = (
                    self.metrics.start_time
                )  # Already set in __init__
                span.add_event("dispatch_started")

                # Execute original dispatch
                result = original_dispatch(self, timeout)

                # Record dispatch end
                span.add_event("dispatch_completed")

                # Update span with execution results
                SpanHelper.set_status_from_execution(span, self)

                # Add metrics
                span.set_attribute("context.duration_ms", self.metrics.duration * 1000)

                # Add result count
                state = self.state
                span.set_attribute("context.results_count", len(state.results))
                span.set_attribute("context.errors_count", len(state.errors))

                # Propagate context to next ExecutionContext if it exists
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
                # Clear async context
                OTelContextManager.clear_async_context()

    return instrumented_dispatch


def instrument_coordinator_execute(original_execute):
    """
    Decorator to instrument ExecutionCoordinator.execute() method.
    """

    @functools.wraps(original_execute)
    def instrumented_execute(self: "ExecutionCoordinator"):
        tracer = get_tracer()

        if not tracer:
            return original_execute(self)

        # Get context from ExecutionContext (set by dispatch)
        parent_context = OTelContextManager.get_async_context()

        with tracer.start_as_current_span(
            "coordinator.execute", context=parent_context, kind=trace.SpanKind.INTERNAL
        ) as span:
            try:
                # Add coordinator attributes
                if self._timeout:
                    span.set_attribute("coordinator.timeout", self._timeout)

                span.add_event("coordinator_started")

                # Execute original
                result = original_execute(self)

                span.add_event("coordinator_completed")
                span.set_status(Status(StatusCode.OK))

                return result

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    return instrumented_execute


async def instrument_coordinator_execute_async(original_execute_async):
    """
    Decorator to instrument ExecutionCoordinator._execute_async() method.
    """

    @functools.wraps(original_execute_async)
    async def instrumented_execute_async(self: "ExecutionCoordinator"):
        tracer = get_tracer()

        if not tracer:
            return await original_execute_async(self)

        # Get context from ExecutionContext
        parent_context = OTelContextManager.get_async_context()

        with tracer.start_as_current_span(
            "coordinator.execute_async",
            context=parent_context,
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            try:
                # Add coordinator attributes
                if self._timeout:
                    span.set_attribute("coordinator.timeout", self._timeout)

                # Setup flow span
                with tracer.start_as_current_span(
                    "flow.setup", kind=trace.SpanKind.INTERNAL
                ) as setup_span:
                    flow = self._setup_execution_flow()
                    self._flow = flow
                    setup_span.set_attribute("flow.type", flow.__class__.__name__)
                    setup_span.add_event("flow_configured")

                # Update status
                await self.execution_context.update_status_async(
                    self.execution_context.state.status.__class__.RUNNING
                )

                span.add_event("execution_started")

                # Run flow with span
                with tracer.start_as_current_span(
                    "flow.run", kind=trace.SpanKind.INTERNAL
                ) as run_span:
                    import asyncio

                    run_coro = flow.run()

                    if self._timeout:
                        future = await asyncio.wait_for(run_coro, timeout=self._timeout)
                    else:
                        future = await run_coro

                    run_span.add_event("flow_completed")

                # Process results with span
                with tracer.start_as_current_span(
                    "result_processor.process", kind=trace.SpanKind.INTERNAL
                ) as process_span:
                    results, errors = await self._result_processor.process_futures(
                        [future]
                    )

                    process_span.set_attribute("results.count", len(results))
                    process_span.set_attribute("errors.count", len(errors))

                    if errors:
                        for error in errors:
                            process_span.record_exception(error)

                    error_results = await self._result_processor.process_errors(errors)
                    results.extend(error_results)

                # Update context
                from volnux.execution.state_manager import ExecutionStatus

                await self.execution_context.bulk_update_async(
                    ExecutionStatus.COMPLETED, errors, results
                )

                # Check for stop/switch requests
                execution_state = await self.execution_context.state_async

                stop_processing_requested = (
                    execution_state.get_stop_processing_request()
                )
                if stop_processing_requested:
                    span.add_event(
                        "stop_processing_requested",
                        {"reason": str(stop_processing_requested)},
                    )
                    await self.execution_context.cancel_async()

                switch_request = execution_state.get_switch_request()
                if switch_request:
                    span.add_event(
                        "task_switch_requested",
                        {"next_descriptor": switch_request.next_task_descriptor},
                    )

                span.set_status(Status(StatusCode.OK))
                return results, errors

            except asyncio.TimeoutError as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "Execution timeout"))
                await self.execution_context.failed_async()
                raise
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                await self.execution_context.failed_async()
                raise

    return instrumented_execute_async


def patch_execution_context():
    """
    Patch ExecutionContext class to add OpenTelemetry instrumentation.

    This should be called during application initialization.
    """
    from volnux.execution.context import ExecutionContext

    # Store original methods
    if not hasattr(ExecutionContext, "_original_dispatch"):
        ExecutionContext._original_dispatch = ExecutionContext.dispatch
        ExecutionContext.dispatch = instrument_execution_context_dispatch(
            ExecutionContext._original_dispatch
        )
        logger.info("ExecutionContext.dispatch instrumented with OpenTelemetry")


def patch_execution_coordinator():
    """
    Patch ExecutionCoordinator class to add OpenTelemetry instrumentation.

    This should be called during application initialization.
    """
    from volnux.execution.coordinator import ExecutionCoordinator

    # Store original methods
    if not hasattr(ExecutionCoordinator, "_original_execute"):
        ExecutionCoordinator._original_execute = ExecutionCoordinator.execute
        ExecutionCoordinator.execute = instrument_coordinator_execute(
            ExecutionCoordinator._original_execute
        )

        ExecutionCoordinator._original_execute_async = (
            ExecutionCoordinator._execute_async
        )
        # For async, we need to wrap it properly
        original_async = ExecutionCoordinator._execute_async

        async def wrapped_async(self):
            return await instrument_coordinator_execute_async(original_async)(self)

        ExecutionCoordinator._execute_async = wrapped_async

        logger.info("ExecutionCoordinator instrumented with OpenTelemetry")


def patch_all_execution_components():
    """
    Patch all execution components for OpenTelemetry instrumentation.

    Call this once during application initialization.
    """
    patch_execution_context()
    patch_execution_coordinator()
    logger.info("All execution components instrumented with OpenTelemetry")
