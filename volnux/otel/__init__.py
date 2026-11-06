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
from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict

from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

from volnux import __version__ as volnux_version
from .tracer_setup import VolnuxTracerConfig, VolnuxTracer
from .instrumentations.context_coordinator import (
    patch_all_execution_components,
)
from .instrumentations.signal import (
    patch_all_pipeline_components,
    disconnect_signal_instrumentation,
)
from .instrumentations.flow import patch_all_flow_components, FlowMetrics
from .instrumentations.engine import patch_engine
from .instrumentations._patching import unpatch_all
from .metrics import VolnuxMetricsCollector, initialize_metrics
from .propagation import install_trace_propagation, remove_trace_propagation

logger = logging.getLogger(__name__)


class BackendConfig(TypedDict, total=False):
    endpoint: str
    agent_url: str


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
    """

    _initialized: bool = False
    _tracer_instance: Optional[Any] = None
    _backends: List[ObservabilityBackend] = []
    _meter_provider: Optional[Any] = None

    @classmethod
    def is_initialized(cls) -> bool:
        return cls._initialized

    @classmethod
    def get_tracer_instance(cls) -> Optional[Any]:
        return cls._tracer_instance

    @classmethod
    def get_backends(cls) -> List[ObservabilityBackend]:
        return cls._backends.copy()


def initialize_otel(
    service_name: str,
    *,
    service_version: str = volnux_version,
    environment: str = "production",
    backends: Optional[List[str]] = None,
    config: Optional[Dict[str, BackendConfig]] = None,
    custom_attributes: Optional[Dict[str, Any]] = None,
    sample_rate: float = 1.0,
    enable_logging_instrumentation: bool = True,
    enable_metrics: bool = True,
    patch_components: bool = True,
    patch_flow_components: bool = True,
    debug: bool = False,
) -> "VolnuxTracer":
    """
    Initialize OpenTelemetry instrumentation for Volnux.
    """

    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be between 0.0 and 1.0")

    if VolnuxObservability.is_initialized():
        logger.warning("OpenTelemetry already initialized. Skipping re-initialization.")
        tracer_instance = VolnuxObservability.get_tracer_instance()
        if tracer_instance is None:
            raise RuntimeError(
                "Observability is marked initialized but tracer instance is missing."
            )
        return tracer_instance

    if debug:
        logging.getLogger("volnux.otel").setLevel(logging.DEBUG)
        logging.getLogger("opentelemetry").setLevel(logging.DEBUG)

    logger.info("Initializing OpenTelemetry for service: %s", service_name)

    if backends is None:
        backends = ["otlp"]
        logger.info("No backends specified; using OTLP only.")

    # Resolve the backends first: an unknown one must not leave half a setup behind.
    resolved: List[ObservabilityBackend] = []
    for backend in backends:
        try:
            backend_enum = ObservabilityBackend(str(backend).lower())
        except ValueError:
            valid = ", ".join(b.value for b in ObservabilityBackend)
            raise ValueError(
                f"Unknown observability backend {backend!r}; use one of: {valid}"
            ) from None
        if backend_enum not in resolved:
            resolved.append(backend_enum)

    if config is None:
        config = {}

    if custom_attributes is None:
        custom_attributes = {}

    custom_attributes = {
        **custom_attributes,
        "framework": "volnux",
        "instrumentation.version": volnux_version,
    }

    tracer_config_kwargs: Dict[str, Any] = {
        "service_name": service_name,
        "service_version": service_version,
        "environment": environment,
        "custom_attributes": custom_attributes,
        "sample_rate": sample_rate,
        "instrument_logging": enable_logging_instrumentation,
    }

    # The endpoint each backend writes to; a backend without one leaves the
    # others' alone (a later backend used to overwrite an endpoint with None).
    for backend_enum in resolved:
        endpoint = _endpoint_for(backend_enum, config)
        if not endpoint:
            logger.warning("Backend %s has no endpoint configured", backend_enum.value)
            continue
        if backend_enum is ObservabilityBackend.DATADOG:
            tracer_config_kwargs["datadog_agent_url"] = endpoint
        elif backend_enum in (ObservabilityBackend.GRAFANA, ObservabilityBackend.TEMPO):
            tracer_config_kwargs["tempo_endpoint"] = endpoint
        else:  # OTLP, Jaeger
            tracer_config_kwargs.setdefault("otlp_endpoint", endpoint)
        logger.info("%s backend configured", backend_enum.value)

    VolnuxObservability._backends = resolved

    tracer_config = VolnuxTracerConfig(**tracer_config_kwargs)
    tracer = VolnuxTracer.initialize(tracer_config)
    VolnuxObservability._tracer_instance = tracer
    VolnuxObservability._initialized = True

    logger.info("✓ OpenTelemetry tracer initialized")

    install_trace_propagation()

    if patch_components:
        patch_all_execution_components()
        patch_all_pipeline_components()
        patch_engine()
        patch_framework_components()
        logger.info("✓ Volnux components instrumented")

    if patch_flow_components:
        patch_all_flow_components()
        logger.info("✓ Flow components instrumented")

    if enable_metrics:
        if _setup_metrics(
            service_name,
            [b.value for b in VolnuxObservability._backends],
            config,
            service_version=service_version,
        ):
            logger.info("✓ Metrics collection enabled")

    logger.info(
        "✓ OpenTelemetry initialization complete for %s (%s) with backends: %s",
        service_name,
        environment,
        ", ".join([b.value for b in VolnuxObservability.get_backends()]),
    )
    return tracer


def _endpoint_for(
    backend: ObservabilityBackend, config: Dict[str, Any]
) -> Optional[str]:
    """The endpoint configured for ``backend`` (``agent_url`` for Datadog)."""
    entry = config.get(backend.value, {}) or {}
    if backend is ObservabilityBackend.DATADOG:
        return entry.get("agent_url")
    return entry.get("endpoint")


def _setup_metrics(
    service_name: str,
    backends: List[str],
    config: Dict[str, Dict[str, Any]],
    service_version: str = volnux_version,
) -> bool:
    """
    Setup OpenTelemetry metrics collection: an OTLP exporter to the first backend
    that has an endpoint, the meter provider, and the metric instruments.

    :return: True if metrics are being collected.
    """
    try:
        endpoint = None
        for backend in backends:
            endpoint = _endpoint_for(ObservabilityBackend(backend), config)
            if endpoint:
                break

        if not endpoint:
            logger.warning(
                "No suitable endpoint found for metrics; skipping metrics setup"
            )
            return False

        metric_exporter = OTLPMetricExporter(endpoint=endpoint)
        metric_reader = PeriodicExportingMetricReader(
            metric_exporter,
            export_interval_millis=60000,
        )

        resource = Resource.create(
            {SERVICE_NAME: service_name, SERVICE_VERSION: service_version}
        )
        meter_provider = MeterProvider(
            resource=resource, metric_readers=[metric_reader]
        )
        otel_metrics.set_meter_provider(meter_provider)
        VolnuxObservability._meter_provider = meter_provider

        # The instruments of VolnuxMetricsRegistry (this used to be left to the
        # caller, so get_metrics_collector() stayed None)
        initialize_metrics(service_name, service_version)
        FlowMetrics.reset()

        logger.info("Metrics exporter configured: %s", endpoint)
        return True

    except Exception as e:
        logger.error("Failed to setup metrics: %s", e, exc_info=True)
        return False


def patch_framework_components() -> None:
    """Instrument the key parts of the framework beyond the pipeline: triggers,
    workflow executors, the workflow registry, checkpoints and rehydration, sagas,
    messaging and remote batch streaming. Each is patched once."""
    from .instrumentations import framework

    framework.patch_all()


def shutdown_otel() -> None:
    """
    Shutdown OpenTelemetry and flush remaining spans/metrics.
    """
    if not VolnuxObservability.is_initialized():
        logger.warning("OpenTelemetry not initialized, nothing to shutdown")
        return

    logger.info("Shutting down OpenTelemetry...")

    # Stop tracing first: restore the patched methods and disconnect the signal
    # hooks, so nothing makes spans against a provider that is shutting down.
    remove_trace_propagation()
    disconnect_signal_instrumentation()
    unpatch_all()

    tracer = VolnuxObservability.get_tracer_instance()
    if tracer:
        tracer.shutdown()

    meter_provider = VolnuxObservability._meter_provider
    if meter_provider is not None:
        try:
            meter_provider.shutdown()
        except Exception as exc:
            logger.warning("Meter provider shutdown error: %s", exc)
    VolnuxMetricsCollector.shutdown()
    FlowMetrics.reset()

    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None
    VolnuxObservability._backends = []
    VolnuxObservability._meter_provider = None

    logger.info("✓ OpenTelemetry shutdown complete")


__all__ = [
    "initialize_otel",
    "shutdown_otel",
    "ObservabilityBackend",
    "VolnuxObservability",
]
