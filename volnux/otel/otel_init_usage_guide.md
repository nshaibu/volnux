# OpenTelemetry Integration for Volnux - Complete Guide

## Overview

This guide provides a comprehensive strategy for integrating OpenTelemetry into the Volnux workflow orchestration framework, with support for Datadog and Grafana/Tempo backends.

## Architecture

### Instrumentation Layers

```
Layer 1: Pipeline Level (Root Span)
  └─ Layer 2: Engine Level (Orchestration)
      └─ Layer 3: ExecutionContext Level (Task Groups)
          └─ Layer 4: Coordinator Level (Execution)
              └─ Layer 5: Flow Level (Individual Tasks)
```

### Span Hierarchy Example

```
workflow.MyPipeline [2.5s]
├── engine.execute [2.4s]
│   ├── engine.process_task [0.8s]
│   │   ├── context.dispatch [0.75s]
│   │   │   ├── coordinator.execute [0.7s]
│   │   │   │   ├── flow.setup [0.05s]
│   │   │   │   ├── flow.run [0.6s]
│   │   │   │   │   ├── task.execute:task1 [0.3s]
│   │   │   │   │   └── task.execute:task2 [0.3s] (parallel)
│   │   │   │   └── result_processor.process [0.05s]
│   │   ├── engine.resolve_next_task [0.01s]
│   └── engine.sink_nodes [0.1s]
```

## Installation

### Dependencies

```bash
pip install opentelemetry-api
pip install opentelemetry-sdk
pip install opentelemetry-exporter-otlp-proto-grpc
pip install opentelemetry-instrumentation-logging
```

### Optional (for direct Datadog integration)
```bash
pip install ddtrace
```

## Setup

### 1. Initialize OpenTelemetry Tracer

```python
from volnux.otel.tracer_setup import VolnuxTracerConfig, VolnuxTracer

# For Datadog
config = VolnuxTracerConfig(
    service_name="my-volnux-service",
    service_version="1.0.0",
    environment="production",
    # Datadog Agent with OTLP enabled (port 4317)
    datadog_agent_url="http://localhost:4317",
    custom_attributes={
        "team": "data-platform",
        "region": "us-east-1"
    }
)

# For Grafana/Tempo
config = VolnuxTracerConfig(
    service_name="my-volnux-service",
    service_version="1.0.0",
    environment="production",
    # Grafana Agent or Tempo endpoint
    tempo_endpoint="http://tempo:4317",
    custom_attributes={
        "team": "data-platform"
    }
)

# For Generic OTLP (works with both)
config = VolnuxTracerConfig(
    service_name="my-volnux-service",
    otlp_endpoint="http://otel-collector:4317",
)

# Initialize tracer
tracer = VolnuxTracer.initialize(config)
```

### 2. Patch Volnux Components

```python
from volnux.otel.context_coordinator_instrumentation import patch_all_execution_components
from volnux.otel.pipeline_signal_instrumentation import patch_all_pipeline_components
from volnux.otel.engine_instrumentation import instrument_engine

# Patch all components at application startup
patch_all_execution_components()  # ExecutionContext & Coordinator
patch_all_pipeline_components()   # Pipeline & Signals

# For engine, you have two options:

# Option 1: Auto-patch (if you control engine instantiation)
from volnux.engine import DefaultWorkflowEngine
original_engine = DefaultWorkflowEngine(enable_debug_logging=True)
instrumented_engine = instrument_engine(original_engine)

# Option 2: Use instrumented engine directly
from volnux.otel.engine_instrumentation import InstrumentedDefaultWorkflowEngine
engine = InstrumentedDefaultWorkflowEngine(
    DefaultWorkflowEngine(),
    enable_debug_logging=True
)
```

### 3. Application Initialization (Complete Example)

```python
# app_init.py
import logging
from volnux.otel.tracer_setup import VolnuxTracerConfig, VolnuxTracer
from volnux.otel.context_coordinator_instrumentation import patch_all_execution_components
from volnux.otel.pipeline_signal_instrumentation import patch_all_pipeline_components

logger = logging.getLogger(__name__)

def initialize_observability(
    service_name: str,
    environment: str = "production",
    datadog_enabled: bool = True,
    grafana_enabled: bool = False
):
    """
    Initialize OpenTelemetry instrumentation for Volnux.
    
    Args:
        service_name: Name of the service
        environment: Deployment environment
        datadog_enabled: Enable Datadog export
        grafana_enabled: Enable Grafana/Tempo export
    """
    
    # Configure tracer
    config_kwargs = {
        "service_name": service_name,
        "environment": environment,
        "custom_attributes": {
            "framework": "volnux",
            "framework.version": "1.0.0"
        }
    }
    
    if datadog_enabled:
        config_kwargs["datadog_agent_url"] = "http://localhost:4317"
    
    if grafana_enabled:
        config_kwargs["tempo_endpoint"] = "http://tempo:4317"
    
    config = VolnuxTracerConfig(**config_kwargs)
    
    # Initialize tracer
    tracer = VolnuxTracer.initialize(config)
    logger.info(f"OpenTelemetry tracer initialized for {service_name}")
    
    # Patch Volnux components
    patch_all_execution_components()
    patch_all_pipeline_components()
    logger.info("Volnux components instrumented with OpenTelemetry")
    
    return tracer

# Call during application startup
if __name__ == "__main__":
    initialize_observability(
        service_name="my-workflow-service",
        environment="production",
        datadog_enabled=True,
        grafana_enabled=False
    )
```

## Usage

### Basic Workflow Execution

Once initialized, all workflows are automatically instrumented:

```python
from my_pipelines import MyDataPipeline

# Create pipeline
pipeline = MyDataPipeline(input_data="example")

# Execute - automatically traced!
execution_context = pipeline.start()

# Traces are automatically sent to Datadog/Grafana
```

### Custom Span Attributes

Add custom attributes to the current span:

```python
from opentelemetry import trace

# In your custom event/task code
def my_custom_task():
    span = trace.get_current_span()
    if span.is_recording():
        span.set_attribute("custom.attribute", "value")
        span.add_event("custom_event", {
            "key": "value"
        })
```

### Manual Instrumentation

For custom code not automatically instrumented:

```python
from volnux.otel.tracer_setup import get_tracer

def my_custom_function():
    tracer = get_tracer()
    if tracer:
        with tracer.start_as_current_span("my_custom_operation") as span:
            span.set_attribute("operation.type", "data_processing")
            # Your code here
            result = process_data()
            span.set_attribute("operation.records_processed", len(result))
            return result
```

### Distributed Tracing

For workflows that execute across multiple services/workers:

```python
from volnux.otel.context_manager import OTelContextManager

# Service A: Serialize context before sending to worker
execution_context = pipeline.start()
trace_context = OTelContextManager.serialize_context(execution_context)

# Send trace_context along with the task to the worker
send_to_worker(task_data, trace_context=trace_context)

# Service B (Worker): Deserialize and continue trace
def worker_process(task_data, trace_context):
    from opentelemetry import trace
    from volnux.otel.tracer_setup import get_tracer
    
    # Reconstruct parent context
    parent_context = OTelContextManager.deserialize_context(trace_context)
    
    tracer = get_tracer()
    with tracer.start_as_current_span(
        "worker.process_task",
        context=parent_context
    ) as span:
        # Process task - this span will be a child of the original workflow span
        result = process_task(task_data)
        return result
```

## Configuration for Different Backends

### Datadog Configuration

#### Using Datadog Agent with OTLP

1. **Enable OTLP in Datadog Agent** (`datadog.yaml`):
```yaml
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
```

2. **Volnux Configuration**:
```python
config = VolnuxTracerConfig(
    service_name="volnux-workflows",
    datadog_agent_url="http://localhost:4317",
    environment="production"
)
```

3. **View Traces**: Navigate to Datadog APM → Traces

#### Using Native Datadog Tracer (Alternative)

```python
from ddtrace import tracer as dd_tracer
from ddtrace.opentelemetry import TracerProvider

# Use Datadog's native tracer
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)
```

### Grafana/Tempo Configuration

#### Using Grafana Agent

1. **Configure Grafana Agent** (`agent.yaml`):
```yaml
traces:
  configs:
    - name: default
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: 0.0.0.0:4317
      remote_write:
        - endpoint: tempo:4317
          insecure: true
```

2. **Volnux Configuration**:
```python
config = VolnuxTracerConfig(
    service_name="volnux-workflows",
    tempo_endpoint="http://grafana-agent:4317",
    environment="production"
)
```

3. **View Traces**: Grafana → Explore → Select Tempo datasource

#### Direct to Tempo

```python
config = VolnuxTracerConfig(
    service_name="volnux-workflows",
    tempo_endpoint="http://tempo:4317",
    environment="production"
)
```

## Metrics Collection

### Automatic Metrics

The instrumentation automatically tracks:
- Workflow execution duration
- Task execution duration
- Error rates
- Context chain length
- Parallel task counts

### Custom Metrics with OpenTelemetry Metrics API

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Setup metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://localhost:4317")
)
meter_provider = MeterProvider(metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# Get meter
meter = metrics.get_meter("volnux.workflows")

# Create custom metrics
workflow_duration = meter.create_histogram(
    name="workflow.duration",
    description="Workflow execution duration",
    unit="ms"
)

# Use in code
workflow_duration.record(execution_time_ms, {"workflow.name": "MyPipeline"})
```

## Debugging

### Enable Debug Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("volnux.otel").setLevel(logging.DEBUG)
logging.getLogger("opentelemetry").setLevel(logging.DEBUG)
```

### Verify Spans are Being Created

```python
from opentelemetry import trace

span = trace.get_current_span()
if span.is_recording():
    print(f"Active span: {span.name}")
    print(f"Trace ID: {span.get_span_context().trace_id}")
    print(f"Span ID: {span.get_span_context().span_id}")
```

### Console Exporter (Development)

For development/testing, use console exporter:

```python
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, BatchSpanProcessor

# Add console exporter to see spans in stdout
console_exporter = ConsoleSpanExporter()
provider.add_span_processor(BatchSpanProcessor(console_exporter))
```

## Best Practices

### 1. Span Naming Convention
- Use hierarchical names: `workflow.pipeline.operation`
- Keep names consistent and meaningful
- Example: `workflow.DataProcessingPipeline`, `task.execute.transform_data`

### 2. Attribute Guidelines
- Use semantic conventions when possible
- Prefix custom attributes: `volnux.*`, `app.*`
- Keep attribute values reasonably sized (< 200 chars)
- Use structured naming: `workflow.input.field_name`

### 3. Error Handling
- Always record exceptions: `span.record_exception(e)`
- Set appropriate status: `span.set_status(Status(StatusCode.ERROR, description))`
- Add context to errors: Use span events with additional details

### 4. Performance Considerations
- Use sampling for high-volume workflows
- Batch span exports (already configured)
- Avoid creating too many spans for simple operations
- Use span attributes instead of creating child spans for small operations

### 5. Context Propagation
- Always propagate context in distributed scenarios
- Use W3C Trace Context format for interoperability
- Test context propagation across service boundaries

## Troubleshooting

### Spans Not Appearing in Backend

1. **Check tracer initialization**:
```python
from volnux.otel.tracer_setup import VolnuxTracer
instance = VolnuxTracer.get_instance()
if not instance:
    print("Tracer not initialized!")
```

2. **Verify endpoint connectivity**:
```bash
# Test OTLP endpoint
telnet localhost 4317
```

3. **Check logs**:
```python
import logging
logging.getLogger("opentelemetry.exporter").setLevel(logging.DEBUG)
```

### Missing Parent-Child Relationships

- Ensure `patch_all_execution_components()` is called before workflow execution
- Verify context propagation in ExecutionContext chain
- Check async context management

### High Cardinality Attributes

Avoid attributes with many unique values:
```python
# Bad: Creates too many unique traces
span.set_attribute("user.id", user_id)  # If millions of users

# Good: Use as span event instead
span.add_event("user_action", {"user.id": user_id})
```

## Example: Complete Integration

```python
# main.py
from volnux.otel.tracer_setup import VolnuxTracerConfig, VolnuxTracer
from volnux.otel.context_coordinator_instrumentation import patch_all_execution_components
from volnux.otel.pipeline_signal_instrumentation import patch_all_pipeline_components
from my_pipelines import DataProcessingPipeline

def main():
    # Initialize observability
    config = VolnuxTracerConfig(
        service_name="data-processing-service",
        environment="production",
        datadog_agent_url="http://localhost:4317",
        custom_attributes={
            "team": "data-engineering",
            "version": "2.0.0"
        }
    )
    
    tracer = VolnuxTracer.initialize(config)
    
    # Patch components
    patch_all_execution_components()
    patch_all_pipeline_components()
    
    print("OpenTelemetry initialized and components instrumented")
    
    # Run workflow - automatically traced
    pipeline = DataProcessingPipeline(
        input_file="data.csv",
        output_path="/tmp/output"
    )
    
    try:
        context = pipeline.start()
        print(f"Workflow completed. Trace ID: {context.id}")
    except Exception as e:
        print(f"Workflow failed: {e}")
    finally:
        # Cleanup
        tracer.shutdown()

if __name__ == "__main__":
    main()
```

## Summary

This integration provides:

✅ **Automatic instrumentation** of all Volnux components
✅ **Distributed tracing** across workflow execution
✅ **Multi-backend support** (Datadog, Grafana/Tempo, any OTLP-compatible)
✅ **Context propagation** through execution chains
✅ **Signal-based event tracking**
✅ **Custom span attributes** and metrics
✅ **Async/sync support**
✅ **Production-ready** error handling and cleanup

The instrumentation is non-invasive and can be enabled/disabled via configuration without code changes to existing workflows.
