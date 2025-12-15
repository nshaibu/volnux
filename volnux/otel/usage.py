"""
Practical Example: Complete OpenTelemetry Integration with Volnux

This example demonstrates a real-world setup with:
- Application initialization
- Datadog and Grafana backends
- Custom instrumentation
- Error handling
- Graceful shutdown
"""

import sys
import logging
import atexit
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# Application Initialization with OpenTelemetry
# ============================================================================


def initialize_application(
    service_name: str = "workflow-orchestration-service",
    environment: str = "production",
    enable_datadog: bool = True,
    enable_grafana: bool = False,
    debug: bool = False,
) -> None:
    """
    Initialize the application with OpenTelemetry observability.

    This should be called at the very beginning of your application,
    before any Volnux pipelines are created.
    """
    from volnux.otel import initialize_otel, shutdown_otel

    logger.info(f"Initializing {service_name}...")

    # Configure backends
    backends = []
    config = {}

    if enable_datadog:
        backends.append("datadog")
        config["datadog"] = {
            "agent_url": "http://localhost:4317"  # Datadog Agent with OTLP
        }

    if enable_grafana:
        backends.append("tempo")
        config["tempo"] = {"endpoint": "http://tempo:4317"}  # Grafana Tempo

    if not backends:
        # Fallback to generic OTLP
        backends.append("otlp")
        config["otlp"] = {"endpoint": "http://localhost:4317"}

    # Initialize OpenTelemetry
    try:
        tracer = initialize_otel(
            service_name=service_name,
            service_version="2.0.0",
            environment=environment,
            backends=backends,
            config=config,
            custom_attributes={
                "team": "data-platform",
                "deployment.region": "us-east-1",
                "app.version": "2.0.0",
            },
            sample_rate=1.0,  # 100% sampling for demo; adjust in production
            enable_metrics=True,
            debug=debug,
        )

        logger.info("✓ OpenTelemetry initialized successfully")
        logger.info(f"✓ Backends: {', '.join(backends)}")

        # Register shutdown hook
        atexit.register(shutdown_otel)
        logger.info("✓ Shutdown hook registered")

        return tracer

    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry: {e}", exc_info=True)
        logger.warning("Continuing without observability...")
        return None


# ============================================================================
# Example Pipeline with Custom Instrumentation
# ============================================================================

from volnux.pipeline import Pipeline
from volnux.fields import InputDataField
from volnux.otel import add_span_attribute, add_span_event, get_current_trace_id


class DataProcessingPipeline(Pipeline):
    """
    Example data processing pipeline with custom instrumentation.
    """

    # Input fields
    input_file = InputDataField(data_type=str, required=True)
    batch_size = InputDataField(data_type=int, default=1000)

    class Meta:
        # Pointy workflow definition
        pointy = """
        start -> load_data -> validate_data -> transform_data -> save_data -> end

        validate_data -[on_failure]-> handle_validation_error -> end
        transform_data -[on_failure]-> handle_transform_error -> end
        """

    def on_start(self):
        """Custom hook called at pipeline start"""
        trace_id = get_current_trace_id()
        logger.info(f"Starting pipeline with trace_id={trace_id}")

        # Add custom span attributes
        add_span_attribute("pipeline.input_file", self.input_file)
        add_span_attribute("pipeline.batch_size", self.batch_size)


# Example Events with Custom Instrumentation
from volnux import Event
from volnux.otel.tracer_setup import get_tracer


class LoadDataEvent(Event):
    """Load data from input file"""

    def execute(self):
        tracer = get_tracer()
        if tracer:
            with tracer.start_as_current_span(
                "event.load_data",
                attributes={
                    "event.type": "load_data",
                    "event.input_file": self.pipeline.input_file,
                },
            ) as span:
                try:
                    # Simulate data loading
                    logger.info(f"Loading data from {self.pipeline.input_file}")

                    # Simulate reading records
                    records_loaded = 5000
                    span.set_attribute("data.records_loaded", records_loaded)
                    span.add_event(
                        "data_loaded", {"records": records_loaded, "file_size_mb": 15.3}
                    )

                    return {"records": records_loaded, "status": "success"}

                except Exception as e:
                    span.record_exception(e)
                    raise
        else:
            # Fallback without tracing
            return {"records": 5000, "status": "success"}


class ValidateDataEvent(Event):
    """Validate loaded data"""

    def execute(self):
        tracer = get_tracer()
        if tracer:
            with tracer.start_as_current_span("event.validate_data") as span:
                # Add validation metrics
                total_records = 5000
                invalid_records = 15

                span.set_attribute("validation.total_records", total_records)
                span.set_attribute("validation.invalid_records", invalid_records)
                span.set_attribute(
                    "validation.error_rate", invalid_records / total_records
                )

                if invalid_records > 100:
                    span.add_event(
                        "validation_threshold_exceeded",
                        {"threshold": 100, "actual": invalid_records},
                    )
                    raise ValueError(f"Too many invalid records: {invalid_records}")

                return {"valid": True, "invalid_count": invalid_records}
        else:
            return {"valid": True, "invalid_count": 15}


class TransformDataEvent(Event):
    """Transform data"""

    def execute(self):
        from volnux.otel import add_span_attribute, add_span_event

        # Add custom attributes to current span
        add_span_attribute("transform.type", "normalization")
        add_span_attribute("transform.batch_size", self.pipeline.batch_size)

        # Simulate transformation
        batches_processed = 5
        for i in range(batches_processed):
            add_span_event(
                f"batch_processed",
                {"batch_number": i + 1, "records": self.pipeline.batch_size},
            )

        add_span_attribute("transform.batches_processed", batches_processed)

        return {"transformed": True, "batches": batches_processed}


class SaveDataEvent(Event):
    """Save processed data"""

    def execute(self):
        tracer = get_tracer()
        if tracer:
            with tracer.start_as_current_span("event.save_data") as span:
                output_path = "/tmp/output.parquet"
                records_saved = 4985  # Some records filtered out

                span.set_attribute("output.path", output_path)
                span.set_attribute("output.format", "parquet")
                span.set_attribute("output.records_saved", records_saved)
                span.set_attribute("output.compression", "snappy")

                span.add_event("data_saved", {"path": output_path, "size_mb": 12.1})

                return {"saved": True, "records": records_saved, "path": output_path}
        else:
            return {"saved": True, "records": 4985}


# ============================================================================
# Main Application
# ============================================================================


def run_pipeline_with_observability():
    """
    Run a pipeline with full OpenTelemetry observability.
    """
    try:
        # Initialize application
        tracer = initialize_application(
            service_name="data-processing-service",
            environment="production",
            enable_datadog=True,
            enable_grafana=False,
            debug=False,
        )

        logger.info("=" * 70)
        logger.info("Starting workflow execution with OpenTelemetry tracing")
        logger.info("=" * 70)

        # Create and execute pipeline
        pipeline = DataProcessingPipeline(input_file="/data/input.csv", batch_size=1000)

        # Execute pipeline - automatically traced!
        execution_context = pipeline.start()

        if execution_context:
            # Get execution summary
            latest_context = execution_context.get_latest_context()
            state = latest_context.state

            logger.info("=" * 70)
            logger.info("Pipeline execution completed!")
            logger.info(f"Status: {state.status.name}")
            logger.info(f"Results: {len(state.results)}")
            logger.info(f"Errors: {len(state.errors)}")

            # Get trace ID for debugging
            trace_id = get_current_trace_id()
            if trace_id:
                logger.info(f"Trace ID: {trace_id}")
                logger.info(
                    f"View in Datadog: https://app.datadoghq.com/apm/traces/{trace_id}"
                )

            logger.info("=" * 70)

            # Print execution chain
            logger.info("Execution chain:")
            context_num = 1
            for ctx in execution_context:
                task_names = [task.get_event_name() for task in ctx.get_task_profiles()]
                duration_ms = ctx.metrics.duration * 1000
                logger.info(
                    f"  {context_num}. {', '.join(task_names)} ({duration_ms:.2f}ms)"
                )
                context_num += 1

        return 0

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1


def run_multiple_pipelines():
    """
    Example of running multiple pipelines with trace correlation.
    """
    tracer = initialize_application(
        service_name="multi-pipeline-orchestrator", enable_datadog=True
    )

    pipelines = [
        ("pipeline1", "/data/input1.csv"),
        ("pipeline2", "/data/input2.csv"),
        ("pipeline3", "/data/input3.csv"),
    ]

    results = []

    for pipeline_name, input_file in pipelines:
        logger.info(f"Executing {pipeline_name}...")

        try:
            pipeline = DataProcessingPipeline(input_file=input_file, batch_size=500)

            execution_context = pipeline.start()
            results.append(
                {
                    "name": pipeline_name,
                    "status": "success",
                    "trace_id": get_current_trace_id(),
                }
            )

        except Exception as e:
            logger.error(f"{pipeline_name} failed: {e}")
            results.append({"name": pipeline_name, "status": "failed", "error": str(e)})

    # Summary
    logger.info("=" * 70)
    logger.info("Multi-pipeline execution summary:")
    for result in results:
        logger.info(f"  {result['name']}: {result['status']}")
        if result.get("trace_id"):
            logger.info(f"    Trace ID: {result['trace_id']}")
    logger.info("=" * 70)

    return results


# ============================================================================
# Distributed Tracing Example
# ============================================================================


def distributed_workflow_example():
    """
    Example of distributed workflow with context propagation.

    This shows how to propagate trace context when workflows
    execute across different services/workers.
    """
    from volnux.otel.context_manager import OTelContextManager
    import json

    # Service A: Main orchestrator
    tracer = initialize_application(
        service_name="orchestrator-service", enable_datadog=True
    )

    logger.info("Starting distributed workflow...")

    # Create and start pipeline
    pipeline = DataProcessingPipeline(
        input_file="/data/distributed_input.csv", batch_size=1000
    )

    execution_context = pipeline.start()

    # Extract trace context for worker
    trace_context = OTelContextManager.serialize_context(execution_context)

    logger.info("Trace context for worker:")
    logger.info(json.dumps(trace_context, indent=2))

    # In real scenario, you would send this to a message queue:
    # send_to_worker_queue({
    #     "task": "process_batch",
    #     "data": {...},
    #     "trace_context": trace_context  # Include trace context
    # })

    # Worker would then deserialize and continue the trace:
    # parent_context = OTelContextManager.deserialize_context(trace_context)
    # with tracer.start_as_current_span("worker.process", context=parent_context):
    #     process_data()

    return trace_context


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    """
    Main entry point with different execution modes.
    """

    # Parse command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "single"

    if mode == "single":
        # Run single pipeline
        exit_code = run_pipeline_with_observability()
        sys.exit(exit_code)

    elif mode == "multiple":
        # Run multiple pipelines
        results = run_multiple_pipelines()
        sys.exit(0 if all(r["status"] == "success" for r in results) else 1)

    elif mode == "distributed":
        # Demonstrate distributed tracing
        trace_context = distributed_workflow_example()
        sys.exit(0)

    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python example.py [single|multiple|distributed]")
        sys.exit(1)


# ============================================================================
# Additional Utilities
# ============================================================================


def health_check_with_tracing():
    """
    Health check endpoint with tracing.
    Useful for Kubernetes/service health checks.
    """
    from volnux.otel.tracer_setup import get_tracer
    from volnux.otel import VolnuxObservability

    tracer = get_tracer()

    health_status = {
        "status": "healthy",
        "observability": {
            "enabled": VolnuxObservability.is_initialized(),
            "backends": [b.value for b in VolnuxObservability.get_backends()],
        },
    }

    if tracer:
        with tracer.start_as_current_span("health_check") as span:
            span.set_attribute("health.status", "healthy")
            span.add_event("health_check_performed")

    return health_status


def get_metrics_summary():
    """
    Get metrics summary from completed workflows.
    """
    # This would query your metrics backend
    # For demo purposes, return mock data
    return {
        "workflows_executed": 150,
        "avg_duration_ms": 1250.5,
        "error_rate": 0.02,
        "p95_duration_ms": 2100.0,
        "p99_duration_ms": 3500.0,
    }
