"""
OpenTelemetry Instrumentation for DefaultWorkflowEngine
"""

import logging
import typing
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from volnux.otel.tracer_setup import get_tracer
from volnux.otel.context_manager import OTelContextManager, SpanHelper

if typing.TYPE_CHECKING:
    from volnux.engine import EngineResult
    from volnux.parser.protocols import TaskType
    from volnux.pipeline import Pipeline

logger = logging.getLogger(__name__)


class InstrumentedDefaultWorkflowEngine:
    """
    Instrumented version of DefaultWorkflowEngine with OpenTelemetry tracing.

    This can either wrap the existing engine or be used as a drop-in replacement.
    """

    def __init__(
        self,
        engine_instance,
        enable_debug_logging: bool = False,
    ):
        """
        Initialize instrumented engine.

        Args:
            engine_instance: Instance of DefaultWorkflowEngine to instrument
            enable_debug_logging: Enable debug logging for tracing
        """
        self.engine = engine_instance
        self.enable_debug_logging = enable_debug_logging
        self.tracer = get_tracer()

    def execute(
        self,
        root_task: "TaskType",
        pipeline: "Pipeline",
    ) -> "EngineResult":
        """
        Execute workflow with OpenTelemetry instrumentation.

        Creates a root span for the engine execution and instruments all key operations.
        """
        if not self.tracer:
            # Tracing not initialized, fall back to normal execution
            logger.warning(
                "OpenTelemetry tracer not initialized, executing without tracing"
            )
            return self.engine.execute(root_task, pipeline)

        # Create engine-level span
        with self.tracer.start_as_current_span(
            "workflow.engine.execute", kind=trace.SpanKind.INTERNAL
        ) as span:
            try:
                # Add engine attributes
                span.set_attribute("engine.name", self.engine.get_name())
                span.set_attribute("engine.strict_mode", self.engine.strict_mode)
                span.set_attribute(
                    "engine.debug_logging", self.engine.enable_debug_logging
                )

                # Add pipeline attributes
                SpanHelper.add_pipeline_attributes(span, pipeline)

                if not root_task:
                    span.set_attribute("engine.no_root_task", True)
                    span.set_status(Status(StatusCode.OK))
                    return self.engine.execute(root_task, pipeline)

                # Execute with instrumented methods
                result = self._execute_with_tracing(root_task, pipeline, span)

                # Add result attributes
                span.set_attribute("engine.tasks_processed", result.tasks_processed)
                span.set_attribute("engine.status", result.status.name)

                if result.error:
                    span.record_exception(result.error)
                    span.set_status(Status(StatusCode.ERROR, str(result.error)))
                else:
                    span.set_status(Status(StatusCode.OK))

                return result

            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    def _execute_with_tracing(
        self, root_task: "TaskType", pipeline: "Pipeline", parent_span: trace.Span
    ) -> "EngineResult":
        """
        Execute workflow with detailed tracing.

        This method instruments the queue-based execution loop.
        """
        from collections import deque
        from volnux.engine import TaskNode, EngineResult, EngineExecutionResult

        queue = deque()
        queue.append(TaskNode(root_task, None))

        sink_queue = deque()
        final_context = None
        tasks_processed = 0
        execution_error = None

        try:
            while queue:
                executable_node = queue.popleft()
                tasks_processed += 1

                if self.enable_debug_logging:
                    logger.debug(f"[Engine] Processing task: {executable_node.task}")

                try:
                    # Create span for task processing
                    with self.tracer.start_as_current_span(
                        f"engine.process_task", kind=trace.SpanKind.INTERNAL
                    ) as task_span:
                        # Add task attributes
                        SpanHelper.add_task_attributes(task_span, executable_node.task)
                        task_span.set_attribute("engine.tasks_in_queue", len(queue))

                        # Detect parallelism
                        parallel_tasks = self.engine._detect_parallel_tasks(
                            executable_node.task
                        )

                        if parallel_tasks:
                            task_span.set_attribute(
                                "engine.parallel_tasks_detected", len(parallel_tasks)
                            )
                            task_span.add_event(
                                "parallel_execution_detected",
                                {"task_count": len(parallel_tasks)},
                            )

                        # Build context (this will create ExecutionContext)
                        execution_context = self.engine._build_context(
                            task=executable_node.task,
                            pipeline=pipeline,
                            previous_context=executable_node.previous_context,
                            parallel_tasks=parallel_tasks,
                            sink_queue=sink_queue,
                        )

                        final_context = execution_context

                        # The dispatch method will be called and will create its own spans
                        # via the instrumented ExecutionContext
                        execution_context.dispatch()
                        execution_state = execution_context.state

                        # Update span with execution result
                        task_span.set_attribute(
                            "execution.status", execution_state.status.name
                        )

                        if self.engine._should_terminate(execution_state):
                            task_span.add_event(
                                "early_termination",
                                {"status": execution_state.status.name},
                            )
                            status = self.engine._map_termination_status(
                                execution_state.status
                            )
                            return EngineResult(
                                status=status,
                                final_context=final_context,
                                tasks_processed=tasks_processed,
                            )

                        # Handle task switching
                        switched = self.engine._handle_task_switch(
                            task=executable_node.task,
                            execution_state=execution_state,
                            previous_context=executable_node.previous_context,
                            queue=queue,
                        )

                        if switched:
                            task_span.add_event("task_switched")
                            continue

                        # Determine next task
                        with self.tracer.start_as_current_span(
                            "engine.resolve_next_task", kind=trace.SpanKind.INTERNAL
                        ) as resolve_span:
                            next_task = self.engine._resolve_next_task(
                                executable_node.task, execution_context
                            )

                            if next_task:
                                resolve_span.set_attribute(
                                    "next_task.id", str(next_task.get_id())
                                )
                                resolve_span.set_attribute(
                                    "next_task.name", next_task.get_event_name()
                                )

                        # Schedule next task
                        if next_task:
                            queue.appendleft(TaskNode(next_task, execution_context))

                except Exception as e:
                    logger.error(
                        f"[Engine] Error processing task {executable_node.task}: {e}",
                        exc_info=True,
                    )
                    execution_error = e

                    if self.engine.strict_mode:
                        return EngineResult(
                            status=EngineExecutionResult.FAILED,
                            final_context=final_context,
                            error=e,
                            tasks_processed=tasks_processed,
                        )

            # Process sink nodes
            if sink_queue:
                with self.tracer.start_as_current_span(
                    "engine.sink_nodes", kind=trace.SpanKind.INTERNAL
                ) as sink_span:
                    sink_span.set_attribute("sink_nodes.count", len(sink_queue))
                    self.engine._drain_sink_nodes(sink_queue, pipeline)

            if self.enable_debug_logging:
                logger.debug(f"[Engine] Completed processing {tasks_processed} tasks")

        except Exception as e:
            logger.exception("[Engine] Fatal error during workflow execution")
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=final_context,
                error=e,
                tasks_processed=tasks_processed,
            )

        return EngineResult(
            status=EngineExecutionResult.COMPLETED,
            final_context=final_context,
            tasks_processed=tasks_processed,
        )


def instrument_engine(engine_instance):
    """
    Instrument an existing DefaultWorkflowEngine instance.

    Args:
        engine_instance: The engine to instrument

    Returns:
        Instrumented engine wrapper
    """
    return InstrumentedDefaultWorkflowEngine(engine_instance)
