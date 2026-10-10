"""
OpenTelemetry Tracer Setup for Volnux
Supports Datadog and Grafana backends
"""

import logging
from typing import Any, Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)

logger = logging.getLogger(__name__)


class VolnuxTracerConfig:
    """Configuration for OpenTelemetry tracing"""

    def __init__(
        self,
        service_name: str = "volnux-workflow",
        service_version: str = "1.0.0",
        environment: str = "production",
        # OTLP Endpoint (works with Datadog Agent, Grafana Agent, etc.)
        otlp_endpoint: Optional[str] = None,
        # Datadog specific
        datadog_agent_url: Optional[str] = None,
        # Grafana specific (Tempo)
        tempo_endpoint: Optional[str] = None,
        # Sampling
        sample_rate: float = 1.0,
        # Additional attributes
        custom_attributes: Optional[dict[str, Any]] = None,
    ):
        if not 0.0 <= sample_rate <= 1.0:
            raise ValueError("sample_rate must be between 0.0 and 1.0")

        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment
        self.otlp_endpoint = otlp_endpoint
        self.datadog_agent_url = datadog_agent_url
        self.tempo_endpoint = tempo_endpoint
        self.sample_rate = sample_rate
        self.custom_attributes = custom_attributes or {}


class VolnuxTracer:
    """OpenTelemetry tracer for Volnux workflows"""

    _instance: Optional["VolnuxTracer"] = None
    _tracer: Optional[trace.Tracer] = None
    _provider: Optional[TracerProvider] = None
    _logging_instrumented: bool = False
    _global_provider_set: bool = False

    def __init__(self, config: VolnuxTracerConfig):
        self.config = config
        self._setup_tracer()

    @classmethod
    def initialize(cls, config: VolnuxTracerConfig) -> "VolnuxTracer":
        """Initialize the global tracer instance"""
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance

    @classmethod
    def get_instance(cls) -> Optional["VolnuxTracer"]:
        """Get the global tracer instance"""
        return cls._instance

    def _build_sampler(self):
        if self.config.sample_rate <= 0.0:
            return ALWAYS_OFF
        if self.config.sample_rate >= 1.0:
            return ALWAYS_ON
        return ParentBased(TraceIdRatioBased(self.config.sample_rate))

    def _setup_tracer(self):
        """Setup OpenTelemetry tracer with exporters"""

        resource = Resource.create(
            {
                SERVICE_NAME: self.config.service_name,
                SERVICE_VERSION: self.config.service_version,
                "deployment.environment": self.config.environment,
                **self.config.custom_attributes,
            }
        )

        self._provider = TracerProvider(
            resource=resource, sampler=self._build_sampler()
        )

        self._setup_exporters()

        if not self.__class__._global_provider_set:
            trace.set_tracer_provider(self._provider)
            self.__class__._global_provider_set = True

        self._tracer = trace.get_tracer(
            instrumenting_module_name="volnux",
            instrumenting_library_version=self.config.service_version,
        )

        if not self.__class__._logging_instrumented:
            LoggingInstrumentor().instrument(set_logging_format=True)
            self.__class__._logging_instrumented = True

        logger.info("OpenTelemetry tracer initialized for %s", self.config.service_name)

    def _setup_exporters(self):
        """Setup span exporters for different backends"""

        exporters_added = 0
        seen_endpoints: set[str] = set()

        def add_otlp_exporter(endpoint: Optional[str], label: str) -> None:
            nonlocal exporters_added
            if not endpoint:
                return
            if endpoint in seen_endpoints:
                logger.info(
                    "%s exporter skipped (duplicate endpoint): %s", label, endpoint
                )
                return

            seen_endpoints.add(endpoint)
            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            self._provider.add_span_processor(BatchSpanProcessor(exporter))
            exporters_added += 1
            logger.info("%s exporter configured: %s", label, endpoint)

        add_otlp_exporter(self.config.otlp_endpoint, "OTLP")
        add_otlp_exporter(self.config.datadog_agent_url, "Datadog")
        add_otlp_exporter(self.config.tempo_endpoint, "Tempo")

        if exporters_added == 0:
            logger.warning(
                "No trace exporter configured. Traces will stay local unless an endpoint is provided."
            )

    def get_tracer(self) -> trace.Tracer:
        """Get the OpenTelemetry tracer"""
        if self._tracer is None:
            raise RuntimeError("Tracer not initialized. Call initialize() first.")
        return self._tracer

    def shutdown(self):
        """Shutdown the tracer and flush remaining spans"""
        if self._provider:
            self._provider.shutdown()
            logger.info("OpenTelemetry tracer shutdown complete")


def get_tracer() -> Optional[trace.Tracer]:
    """Get the global tracer instance"""
    instance = VolnuxTracer.get_instance()
    return instance.get_tracer() if instance else None
