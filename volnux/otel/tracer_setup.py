"""
OpenTelemetry Tracer Setup for Volnux
Supports Datadog and Grafana backends
"""

import logging
from typing import Optional
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor

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
        custom_attributes: Optional[dict] = None,
    ):
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment
        self.otlp_endpoint = otlp_endpoint or "http://localhost:4317"
        self.datadog_agent_url = datadog_agent_url
        self.tempo_endpoint = tempo_endpoint
        self.sample_rate = sample_rate
        self.custom_attributes = custom_attributes or {}


class VolnuxTracer:
    """OpenTelemetry tracer for Volnux workflows"""

    _instance: Optional["VolnuxTracer"] = None
    _tracer: Optional[trace.Tracer] = None
    _provider: Optional[TracerProvider] = None

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

    def _setup_tracer(self):
        """Setup OpenTelemetry tracer with exporters"""

        # Create resource with service information
        resource = Resource.create(
            {
                SERVICE_NAME: self.config.service_name,
                SERVICE_VERSION: self.config.service_version,
                "deployment.environment": self.config.environment,
                **self.config.custom_attributes,
            }
        )

        # Create tracer provider
        self._provider = TracerProvider(resource=resource)

        # Setup exporters based on configuration
        self._setup_exporters()

        # Set as global tracer provider
        trace.set_tracer_provider(self._provider)

        # Get tracer
        self._tracer = trace.get_tracer(
            instrumenting_module_name="volnux",
            instrumenting_library_version=self.config.service_version,
        )

        # Instrument logging to correlate logs with traces
        LoggingInstrumentor().instrument(set_logging_format=True)

        logger.info(f"OpenTelemetry tracer initialized for {self.config.service_name}")

    def _setup_exporters(self):
        """Setup span exporters for different backends"""

        # OTLP Exporter (Universal - works with Datadog, Grafana, etc.)
        if self.config.otlp_endpoint:
            otlp_exporter = OTLPSpanExporter(
                endpoint=self.config.otlp_endpoint,
                insecure=True,  # Set to False in production with TLS
            )
            self._provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            logger.info(f"OTLP exporter configured: {self.config.otlp_endpoint}")

        # Datadog specific configuration
        if self.config.datadog_agent_url:
            # Datadog accepts OTLP, just point to Datadog Agent
            datadog_exporter = OTLPSpanExporter(
                endpoint=self.config.datadog_agent_url, insecure=True
            )
            self._provider.add_span_processor(BatchSpanProcessor(datadog_exporter))
            logger.info(f"Datadog exporter configured: {self.config.datadog_agent_url}")

        # Grafana Tempo specific configuration
        if self.config.tempo_endpoint:
            tempo_exporter = OTLPSpanExporter(
                endpoint=self.config.tempo_endpoint, insecure=True
            )
            self._provider.add_span_processor(BatchSpanProcessor(tempo_exporter))
            logger.info(f"Tempo exporter configured: {self.config.tempo_endpoint}")

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


# Convenience function
def get_tracer() -> Optional[trace.Tracer]:
    """Get the global tracer instance"""
    instance = VolnuxTracer.get_instance()
    return instance.get_tracer() if instance else None
