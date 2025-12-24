"""
Complete OpenTelemetry Initialization Module for Volnux

This module provides a single entry point for initializing OpenTelemetry
instrumentation across all Volnux components.

Usage:
    from volnux.otel import initialize_otel, shutdown_otel

    # At application startup
    initialize_otel(
        service_name="my-workflow-service",
        environment="production",
        backends=["datadog", "tempo"],
        config={
            "datadog": {"agent_url": "http://localhost:4317"},
            "tempo": {"endpoint": "http://tempo:4317"}
        }
    )

    # Run workflows (automatically instrumented)
    pipeline = MyPipeline()
    pipeline.start()

    # At application shutdown
    shutdown_otel()
"""

import logging
import sys
from typing import Dict, List, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)


class ObservabilityBackend(str, Enum):
    """Supported observability backends"""

    DATADOG = "datadog"
    GRAFANA = "grafana"
    TEMPO = "tempo"
    JAEGER = "jaeger"
    GENERIC_OTLP = "otlp"


class VolnuxObservability:
    """
    Centralized observability configuration for Volnux.

    This class manages OpenTelemetry initialization and provides
    utilities for accessing tracer instances.
    """

    _initialized: bool = False
    _tracer_instance: Optional[Any] = None
    _backends: List[ObservabilityBackend] = []

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if observability has been initialized"""
        return cls._initialized

    @classmethod
    def get_tracer_instance(cls) -> Optional[Any]:
        """Get the tracer instance"""
        return cls._tracer_instance

    @classmethod
    def get_backends(cls) -> List[ObservabilityBackend]:
        """Get configured backends"""
        return cls._backends.copy()


def initialize_otel(
    service_name: str,
    service_version: str = "1.0.0",
    environment: str = "production",
    backends: Optional[List[str]] = None,
    config: Optional[Dict[str, Dict[str, Any]]] = None,
    custom_attributes: Optional[Dict[str, str]] = None,
    sample_rate: float = 1.0,
    enable_logging_instrumentation: bool = True,
    enable_metrics: bool = True,
    patch_components: bool = True,
    patch_flow_components: bool = True,
    debug: bool = False,
) -> "VolnuxTracer":
    """
    Initialize OpenTelemetry instrumentation for Volnux.

    This is the main entry point for setting up observability.

    Args:
        service_name: Name of the service (e.g., "my-workflow-service")
        service_version: Version of the service
        environment: Deployment environment (production, staging, development)
        backends: List of backends to enable (datadog, grafana, tempo, jaeger, otlp)
        config: Backend-specific configuration
            {
                "datadog": {"agent_url": "http://localhost:4317"},
                "tempo": {"endpoint": "http://tempo:4317"},
                "otlp": {"endpoint": "http://collector:4317"}
            }
        custom_attributes: Additional attributes to add to all spans
        sample_rate: Sampling rate (0.0 to 1.0)
        enable_logging_instrumentation: Enable log correlation with traces
        enable_metrics: Enable metrics collection (requires additional setup)
        patch_components: Automatically patch Volnux core components
        patch_flow_components: Automatically patch Flow components
        debug: Enable debug logging

    Returns:
        Initialized VolnuxTracer instance

    Example:
        >>> from volnux.otel import initialize_otel
        >>> tracer = initialize_otel(
        ...     service_name="data-pipeline-service",
        ...     environment="production",
        ...     backends=["datadog"],
        ...     config={
        ...         "datadog": {"agent_url": "http://localhost:4317"}
        ...     },
        ...     custom_attributes={
        ...         "team": "data-engineering",
        ...         "region": "us-east-1"
        ...     }
        ... )
    """

    if VolnuxObservability.is_initialized():
        logger.warning("OpenTelemetry already initialized. Skipping re-initialization.")
        return VolnuxObservability.get_tracer_instance()

    # Setup logging
    if debug:
        logging.getLogger("volnux.otel").setLevel(logging.DEBUG)
        logging.getLogger("opentelemetry").setLevel(logging.DEBUG)

    logger.info(f"Initializing OpenTelemetry for service: {service_name}")

    # Import required modules
    from volnux.otel.tracer_setup import VolnuxTracerConfig, VolnuxTracer
    from volnux.otel.context_coordinator_instrumentation import (
        patch_all_execution_components,
    )
    from volnux.otel.pipeline_signal_instrumentation import (
        patch_all_pipeline_components,
    )

    # Default backends and config
    if backends is None:
        backends = ["otlp"]
        logger.info("No backends specified, using default OTLP endpoint")

    if config is None:
        config = {}

    if custom_attributes is None:
        custom_attributes = {}

    # Add framework metadata
    custom_attributes.update(
        {"framework": "volnux", "instrumentation.version": "1.0.0"}
    )

    # Build tracer configuration
    tracer_config_kwargs = {
        "service_name": service_name,
        "service_version": service_version,
        "environment": environment,
        "custom_attributes": custom_attributes,
        "sample_rate": sample_rate,
    }

    # Configure backends
    for backend in backends:
        backend_enum = ObservabilityBackend(backend.lower())
        VolnuxObservability._backends.append(backend_enum)

        if backend_enum == ObservabilityBackend.DATADOG:
            datadog_config = config.get("datadog", {})
            tracer_config_kwargs["datadog_agent_url"] = datadog_config.get(
                "agent_url", "http://localhost:4317"
            )
            logger.info(
                f"Datadog backend configured: {tracer_config_kwargs['datadog_agent_url']}"
            )

        elif backend_enum in [ObservabilityBackend.GRAFANA, ObservabilityBackend.TEMPO]:
            tempo_config = config.get(backend, {})
            tracer_config_kwargs["tempo_endpoint"] = tempo_config.get(
                "endpoint", "http://tempo:4317"
            )
            logger.info(
                f"Tempo/Grafana backend configured: {tracer_config_kwargs['tempo_endpoint']}"
            )

        elif backend_enum == ObservabilityBackend.GENERIC_OTLP:
            otlp_config = config.get("otlp", {})
            tracer_config_kwargs["otlp_endpoint"] = otlp_config.get(
                "endpoint", "http://localhost:4317"
            )
            logger.info(
                f"OTLP backend configured: {tracer_config_kwargs['otlp_endpoint']}"
            )

        elif backend_enum == ObservabilityBackend.JAEGER:
            # Jaeger also uses OTLP
            jaeger_config = config.get("jaeger", {})
            tracer_config_kwargs["otlp_endpoint"] = jaeger_config.get(
                "endpoint", "http://jaeger:4317"
            )
            logger.info(
                f"Jaeger backend configured: {tracer_config_kwargs['otlp_endpoint']}"
            )

    # Initialize tracer
    tracer_config = VolnuxTracerConfig(**tracer_config_kwargs)
    tracer = VolnuxTracer.initialize(tracer_config)
    VolnuxObservability._tracer_instance = tracer

    logger.info("✓ OpenTelemetry tracer initialized")

    # Patch Volnux components
    if patch_components:
        patch_all_execution_components()
        patch_all_pipeline_components()
        logger.info("✓ Volnux components instrumented")

    # Patch Flow components
    if patch_flow_components:
        from volnux.otel.flow_instrumentation import patch_all_flow_components

        patch_all_flow_components()
        logger.info("✓ Flow components instrumented")

    # Setup metrics if enabled
    if enable_metrics:
        _setup_metrics(service_name, backends, config)
        logger.info("✓ Metrics collection enabled")

    VolnuxObservability._initialized = True

    logger.info(
        f"✓ OpenTelemetry initialization complete for {service_name} "
        f"({environment}) with backends: {', '.join(backends)}"
    )

    return tracer


def _setup_metrics(
    service_name: str, backends: List[str], config: Dict[str, Dict[str, Any]]
) -> None:
    """
    Setup OpenTelemetry metrics collection.

    Args:
        service_name: Name of the service
        backends: List of backends
        config: Backend configuration
    """
    try:
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME

        # Determine metrics endpoint
        endpoint = None
        for backend in backends:
            if backend == "datadog":
                endpoint = config.get("datadog", {}).get(
                    "agent_url", "http://localhost:4317"
                )
                break
            elif backend in ["grafana", "tempo"]:
                endpoint = config.get(backend, {}).get("endpoint", "http://tempo:4317")
                break
            elif backend == "otlp":
                endpoint = config.get("otlp", {}).get(
                    "endpoint", "http://localhost:4317"
                )
                break

        if not endpoint:
            logger.warning(
                "No suitable endpoint found for metrics, skipping metrics setup"
            )
            return

        # Create metric exporter
        metric_exporter = OTLPMetricExporter(endpoint=endpoint)
        metric_reader = PeriodicExportingMetricReader(
            metric_exporter, export_interval_millis=60000  # Export every 60 seconds
        )

        # Create meter provider
        resource = Resource.create({SERVICE_NAME: service_name})
        meter_provider = MeterProvider(
            resource=resource, metric_readers=[metric_reader]
        )

        # Set as global meter provider
        metrics.set_meter_provider(meter_provider)

        logger.info(f"Metrics exporter configured: {endpoint}")

    except Exception as e:
        logger.error(f"Failed to setup metrics: {e}", exc_info=True)


def shutdown_otel() -> None:
    """
    Shutdown OpenTelemetry and flush remaining spans/metrics.

    Should be called during application shutdown to ensure all
    telemetry data is exported.

    Example:
        >>> from volnux.otel import shutdown_otel
        >>> import atexit
        >>> atexit.register(shutdown_otel)
    """
    if not VolnuxObservability.is_initialized():
        logger.warning("OpenTelemetry not initialized, nothing to shutdown")
        return

    logger.info("Shutting down OpenTelemetry...")

    tracer = VolnuxObservability.get_tracer_instance()
    if tracer:
        tracer.shutdown()

    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None
    VolnuxObservability._backends = []

    logger.info("✓ OpenTelemetry shutdown complete")


def get_current_trace_id() -> Optional[str]:
    """
    Get the current trace ID.

    Useful for logging or debugging.

    Returns:
        Trace ID as hex string, or None if no active trace

    Example:
        >>> from volnux.otel import get_current_trace_id
        >>> trace_id = get_current_trace_id()
        >>> logger.info(f"Processing request with trace_id={trace_id}")
    """
    try:
        from opentelemetry import trace

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
        from opentelemetry import trace

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
        >>> from volnux.otel import add_span_attribute
        >>> add_span_attribute("user.id", "12345")
        >>> add_span_attribute("records.processed", 1000)
    """
    try:
        from opentelemetry import trace

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
        >>> from volnux.otel import add_span_event
        >>> add_span_event("data_validated", {
        ...     "records": 1000,
        ...     "invalid": 5
        ... })
    """
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(name, attributes or {})
    except Exception as e:
        logger.debug(f"Failed to add span event: {e}")


# Convenience function for quick setup
def quick_setup(
    service_name: str,
    backend: str = "datadog",
    environment: str = "production",
    debug: bool = False,
) -> "VolnuxTracer":
    """
    Quick setup with sensible defaults.

    Args:
        service_name: Service name
        backend: Backend to use (datadog, grafana, tempo, otlp)
        environment: Environment name
        debug: Enable debug logging

    Returns:
        Initialized tracer

    Example:
        >>> from volnux.otel import quick_setup
        >>> quick_setup("my-service", backend="datadog")
    """
    return initialize_otel(
        service_name=service_name,
        environment=environment,
        backends=[backend],
        debug=debug,
    )


# Export main functions
__all__ = [
    "initialize_otel",
    "shutdown_otel",
    "quick_setup",
    "get_current_trace_id",
    "get_current_span_id",
    "add_span_attribute",
    "add_span_event",
    "ObservabilityBackend",
    "VolnuxObservability",
]
