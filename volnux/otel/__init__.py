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

from opentelemetry import metrics
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
)
from .instrumentations.flow import patch_all_flow_components

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

    if VolnuxObservability.is_initialized():
        logger.warning("OpenTelemetry already initialized. Skipping re-initialization.")
        tracer_instance = VolnuxObservability.get_tracer_instance()
        if tracer_instance is None:
            raise RuntimeError(
                "Observability is marked initialized but tracer instance is missing."
            )
        return tracer_instance

    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be between 0.0 and 1.0")

    if debug:
        logging.getLogger("volnux.otel").setLevel(logging.DEBUG)
        logging.getLogger("opentelemetry").setLevel(logging.DEBUG)

    logger.info("Initializing OpenTelemetry for service: %s", service_name)

    if backends is None:
        backends = ["otlp"]
        logger.info("No backends specified; using OTLP only.")

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
    }

    VolnuxObservability._backends = []

    seen_backends: set[ObservabilityBackend] = set()
    for backend in backends:
        backend_enum = ObservabilityBackend(backend.lower())
        if backend_enum in seen_backends:
            continue
        seen_backends.add(backend_enum)
        VolnuxObservability._backends.append(backend_enum)

        if backend_enum == ObservabilityBackend.DATADOG:
            datadog_config = config.get("datadog", {})
            tracer_config_kwargs["datadog_agent_url"] = datadog_config.get("agent_url")
            logger.info("Datadog backend configured")

        elif backend_enum in (ObservabilityBackend.GRAFANA, ObservabilityBackend.TEMPO):
            tempo_config = config.get(backend.lower(), {})
            tracer_config_kwargs["tempo_endpoint"] = tempo_config.get("endpoint")
            logger.info("Tempo/Grafana backend configured")

        elif backend_enum == ObservabilityBackend.GENERIC_OTLP:
            otlp_config = config.get("otlp", {})
            tracer_config_kwargs["otlp_endpoint"] = otlp_config.get("endpoint")
            logger.info("OTLP backend configured")

        elif backend_enum == ObservabilityBackend.JAEGER:
            jaeger_config = config.get("jaeger", {})
            tracer_config_kwargs["otlp_endpoint"] = jaeger_config.get("endpoint")
            logger.info("Jaeger backend configured")

    tracer_config = VolnuxTracerConfig(**tracer_config_kwargs)
    tracer = VolnuxTracer.initialize(tracer_config)
    VolnuxObservability._tracer_instance = tracer
    VolnuxObservability._initialized = True

    logger.info("✓ OpenTelemetry tracer initialized")

    if patch_components:
        patch_all_execution_components()
        patch_all_pipeline_components()
        logger.info("✓ Volnux components instrumented")

    if patch_flow_components:
        patch_all_flow_components()
        logger.info("✓ Flow components instrumented")

    if enable_metrics:
        _setup_metrics(
            service_name,
            [b.value for b in VolnuxObservability._backends],
            config,
            service_version=service_version,
        )
        logger.info("✓ Metrics collection enabled")

    logger.info(
        "✓ OpenTelemetry initialization complete for %s (%s) with backends: %s",
        service_name,
        environment,
        ", ".join([b.value for b in VolnuxObservability.get_backends()]),
    )
    return tracer


def _setup_metrics(
    service_name: str,
    backends: List[str],
    config: Dict[str, Dict[str, Any]],
    service_version: str = volnux_version,
) -> None:
    """
    Setup OpenTelemetry metrics collection.
    """
    try:
        endpoint = None
        for backend in backends:
            if backend == "datadog":
                endpoint = config.get("datadog", {}).get("agent_url")
                break
            if backend in ("grafana", "tempo"):
                endpoint = config.get(backend, {}).get("endpoint")
                break
            if backend == "otlp":
                endpoint = config.get("otlp", {}).get("endpoint")
                break
            if backend == "jaeger":
                endpoint = config.get("jaeger", {}).get("endpoint")
                break

        if not endpoint:
            logger.warning(
                "No suitable endpoint found for metrics; skipping metrics setup"
            )
            return

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
        metrics.set_meter_provider(meter_provider)

        logger.info("Metrics exporter configured: %s", endpoint)

    except Exception as e:
        logger.error("Failed to setup metrics: %s", e, exc_info=True)


def shutdown_otel() -> None:
    """
    Shutdown OpenTelemetry and flush remaining spans/metrics.
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


__all__ = [
    "initialize_otel",
    "shutdown_otel",
    "ObservabilityBackend",
    "VolnuxObservability",
]
