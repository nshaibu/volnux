"""
Comprehensive Metrics Collection for Volnux

This module provides complete metrics instrumentation including:
- Workflow-level metrics
- Task execution metrics
- Performance metrics
- Error tracking metrics
- Resource utilization metrics
- Custom business metrics

Supports export to Datadog, Grafana/Prometheus, and generic OTLP backends.
"""

import logging
import typing
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    ConsoleMetricExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

logger = logging.getLogger(__name__)


# ============================================================================
# Metric Types and Configuration
# ============================================================================


class MetricType(str, Enum):
    """Types of metrics collected"""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    UP_DOWN_COUNTER = "up_down_counter"


@dataclass
class MetricDefinition:
    """Definition of a metric"""

    name: str
    description: str
    unit: str
    metric_type: MetricType
    attributes: Optional[Dict[str, str]] = None


# ============================================================================
# Volnux Metrics Registry
# ============================================================================


class VolnuxMetricsRegistry:
    """
    Central registry for all Volnux metrics.

    Defines all metrics that will be collected across the system.
    """

    # Workflow Metrics
    WORKFLOW_DURATION = MetricDefinition(
        name="workflow.duration",
        description="Total workflow execution duration",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    WORKFLOW_COUNT = MetricDefinition(
        name="workflow.executions",
        description="Number of workflow executions",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    WORKFLOW_SUCCESS_RATE = MetricDefinition(
        name="workflow.success_rate",
        description="Workflow success rate",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    # Task Metrics
    TASK_DURATION = MetricDefinition(
        name="task.duration",
        description="Individual task execution duration",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    TASK_COUNT = MetricDefinition(
        name="task.executions",
        description="Number of task executions",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    TASK_QUEUE_SIZE = MetricDefinition(
        name="task.queue_size",
        description="Number of tasks in execution queue",
        unit="1",
        metric_type=MetricType.UP_DOWN_COUNTER,
    )

    # Context Metrics
    CONTEXT_CHAIN_LENGTH = MetricDefinition(
        name="context.chain_length",
        description="Length of execution context chain",
        unit="1",
        metric_type=MetricType.HISTOGRAM,
    )

    CONTEXT_DURATION = MetricDefinition(
        name="context.duration",
        description="ExecutionContext dispatch duration",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    # Engine Metrics
    ENGINE_TASKS_PROCESSED = MetricDefinition(
        name="engine.tasks_processed",
        description="Number of tasks processed by engine",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    ENGINE_PARALLEL_TASKS = MetricDefinition(
        name="engine.parallel_tasks",
        description="Number of parallel tasks detected",
        unit="1",
        metric_type=MetricType.HISTOGRAM,
    )

    # Flow Metrics
    FLOW_DURATION = MetricDefinition(
        name="flow.duration",
        description="Flow execution duration",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    EVENT_DURATION = MetricDefinition(
        name="event.duration",
        description="Event execution duration",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    # Error Metrics
    ERROR_COUNT = MetricDefinition(
        name="error.count",
        description="Number of errors encountered",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    ERROR_RATE = MetricDefinition(
        name="error.rate",
        description="Error rate per execution",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    # Executor Metrics
    EXECUTOR_USAGE = MetricDefinition(
        name="executor.usage",
        description="Executor usage count by type",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    EXECUTOR_QUEUE_TIME = MetricDefinition(
        name="executor.queue_time",
        description="Time spent waiting in executor queue",
        unit="ms",
        metric_type=MetricType.HISTOGRAM,
    )

    # Retry Metrics
    RETRY_COUNT = MetricDefinition(
        name="retry.count",
        description="Number of task retries",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    RETRY_SUCCESS_RATE = MetricDefinition(
        name="retry.success_rate",
        description="Success rate of retries",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    # Resource Metrics
    MEMORY_USAGE = MetricDefinition(
        name="resource.memory_usage",
        description="Memory usage during execution",
        unit="MB",
        metric_type=MetricType.HISTOGRAM,
    )

    CPU_USAGE = MetricDefinition(
        name="resource.cpu_usage",
        description="CPU usage during execution",
        unit="%",
        metric_type=MetricType.HISTOGRAM,
    )

    # Business Metrics
    RECORDS_PROCESSED = MetricDefinition(
        name="business.records_processed",
        description="Number of records processed",
        unit="1",
        metric_type=MetricType.COUNTER,
    )

    DATA_VOLUME = MetricDefinition(
        name="business.data_volume",
        description="Volume of data processed",
        unit="MB",
        metric_type=MetricType.HISTOGRAM,
    )


# ============================================================================
# Metrics Collector
# ============================================================================


class VolnuxMetricsCollector:
    """
    Main metrics collector for Volnux.

    Provides a unified interface for recording metrics across all components.
    """

    _instance: Optional["VolnuxMetricsCollector"] = None
    _meter: Optional[metrics.Meter] = None
    _instruments: Dict[str, Any] = {}
    _initialized: bool = False

    def __init__(self):
        self._instruments = {}

    @classmethod
    def initialize(
        cls,
        service_name: str,
        service_version: str = "1.0.0",
        endpoint: Optional[str] = None,
        export_interval_ms: int = 60000,
        console_export: bool = False,
    ) -> "VolnuxMetricsCollector":
        """
        Initialize the metrics collector.

        Args:
            service_name: Name of the service
            service_version: Version of the service
            endpoint: OTLP endpoint for metrics export
            export_interval_ms: Interval for exporting metrics (milliseconds)
            console_export: Enable console export for debugging

        Returns:
            Initialized VolnuxMetricsCollector instance
        """
        if cls._instance and cls._initialized:
            return cls._instance

        try:
            # Create resource
            resource = Resource.create(
                {
                    SERVICE_NAME: service_name,
                    SERVICE_VERSION: service_version,
                    "telemetry.sdk.name": "opentelemetry",
                    "telemetry.sdk.language": "python",
                }
            )

            # Setup metric readers
            metric_readers = []

            # Console exporter for debugging
            if console_export:
                console_reader = PeriodicExportingMetricReader(
                    ConsoleMetricExporter(), export_interval_millis=export_interval_ms
                )
                metric_readers.append(console_reader)

            # OTLP exporter for production
            if endpoint:
                otlp_exporter = OTLPMetricExporter(
                    endpoint=endpoint,
                    insecure=True,  # Set to False with TLS in production
                )
                otlp_reader = PeriodicExportingMetricReader(
                    otlp_exporter, export_interval_millis=export_interval_ms
                )
                metric_readers.append(otlp_reader)

            if not metric_readers:
                logger.warning("No metric exporters configured")
                return None

            # Create meter provider
            meter_provider = MeterProvider(
                resource=resource, metric_readers=metric_readers
            )

            # Set as global meter provider
            metrics.set_meter_provider(meter_provider)

            # Get meter
            cls._meter = metrics.get_meter("volnux", version=service_version)

            # Create instance
            cls._instance = cls()
            cls._instance._setup_instruments()
            cls._initialized = True

            logger.info(f"Metrics collector initialized for {service_name}")
            return cls._instance

        except Exception as e:
            logger.error(f"Failed to initialize metrics collector: {e}", exc_info=True)
            return None

    @classmethod
    def get_instance(cls) -> Optional["VolnuxMetricsCollector"]:
        """Get the global metrics collector instance"""
        return cls._instance

    def _setup_instruments(self):
        """Setup all metric instruments from registry"""
        if not self._meter:
            return

        # Get all metric definitions from registry
        registry_attrs = [
            attr
            for attr in dir(VolnuxMetricsRegistry)
            if not attr.startswith("_")
            and isinstance(getattr(VolnuxMetricsRegistry, attr), MetricDefinition)
        ]

        for attr_name in registry_attrs:
            metric_def: MetricDefinition = getattr(VolnuxMetricsRegistry, attr_name)

            try:
                if metric_def.metric_type == MetricType.COUNTER:
                    instrument = self._meter.create_counter(
                        name=metric_def.name,
                        description=metric_def.description,
                        unit=metric_def.unit,
                    )
                elif metric_def.metric_type == MetricType.HISTOGRAM:
                    instrument = self._meter.create_histogram(
                        name=metric_def.name,
                        description=metric_def.description,
                        unit=metric_def.unit,
                    )
                elif metric_def.metric_type == MetricType.UP_DOWN_COUNTER:
                    instrument = self._meter.create_up_down_counter(
                        name=metric_def.name,
                        description=metric_def.description,
                        unit=metric_def.unit,
                    )
                else:
                    logger.warning(f"Unsupported metric type: {metric_def.metric_type}")
                    continue

                self._instruments[metric_def.name] = instrument
                logger.debug(f"Created metric instrument: {metric_def.name}")

            except Exception as e:
                logger.error(f"Failed to create instrument {metric_def.name}: {e}")

    def record_metric(
        self,
        metric_def: MetricDefinition,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
    ):
        """
        Record a metric value.

        Args:
            metric_def: MetricDefinition to record
            value: Value to record
            attributes: Optional attributes for the metric
        """
        instrument = self._instruments.get(metric_def.name)
        if not instrument:
            logger.debug(f"Instrument not found: {metric_def.name}")
            return

        try:
            if metric_def.metric_type in [
                MetricType.COUNTER,
                MetricType.UP_DOWN_COUNTER,
            ]:
                instrument.add(value, attributes=attributes or {})
            elif metric_def.metric_type == MetricType.HISTOGRAM:
                instrument.record(value, attributes=attributes or {})
        except Exception as e:
            logger.error(f"Failed to record metric {metric_def.name}: {e}")

    # ========================================================================
    # Convenience Methods for Common Metrics
    # ========================================================================

    def record_workflow_duration(
        self,
        duration_ms: float,
        pipeline_name: str,
        status: str,
        environment: str = "production",
    ):
        """Record workflow execution duration"""
        self.record_metric(
            VolnuxMetricsRegistry.WORKFLOW_DURATION,
            duration_ms,
            attributes={
                "pipeline.name": pipeline_name,
                "workflow.status": status,
                "environment": environment,
            },
        )

    def increment_workflow_count(
        self, pipeline_name: str, status: str, environment: str = "production"
    ):
        """Increment workflow execution counter"""
        self.record_metric(
            VolnuxMetricsRegistry.WORKFLOW_COUNT,
            1,
            attributes={
                "pipeline.name": pipeline_name,
                "workflow.status": status,
                "environment": environment,
            },
        )

    def record_task_duration(
        self, duration_ms: float, task_name: str, status: str, pipeline_name: str
    ):
        """Record task execution duration"""
        self.record_metric(
            VolnuxMetricsRegistry.TASK_DURATION,
            duration_ms,
            attributes={
                "task.name": task_name,
                "task.status": status,
                "pipeline.name": pipeline_name,
            },
        )

    def increment_task_count(self, task_name: str, pipeline_name: str):
        """Increment task execution counter"""
        self.record_metric(
            VolnuxMetricsRegistry.TASK_COUNT,
            1,
            attributes={"task.name": task_name, "pipeline.name": pipeline_name},
        )

    def increment_error_count(
        self, error_type: str, component: str, pipeline_name: Optional[str] = None
    ):
        """Increment error counter"""
        attrs = {"error.type": error_type, "component": component}
        if pipeline_name:
            attrs["pipeline.name"] = pipeline_name

        self.record_metric(VolnuxMetricsRegistry.ERROR_COUNT, 1, attributes=attrs)

    def record_parallel_tasks(self, count: int, pipeline_name: str):
        """Record number of parallel tasks"""
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_PARALLEL_TASKS,
            count,
            attributes={"pipeline.name": pipeline_name},
        )

    def record_executor_usage(self, executor_type: str, pipeline_name: str):
        """Record executor usage"""
        self.record_metric(
            VolnuxMetricsRegistry.EXECUTOR_USAGE,
            1,
            attributes={"executor.type": executor_type, "pipeline.name": pipeline_name},
        )

    def record_business_metric(
        self, records_processed: int, data_volume_mb: float, pipeline_name: str
    ):
        """Record business metrics"""
        self.record_metric(
            VolnuxMetricsRegistry.RECORDS_PROCESSED,
            records_processed,
            attributes={"pipeline.name": pipeline_name},
        )

        self.record_metric(
            VolnuxMetricsRegistry.DATA_VOLUME,
            data_volume_mb,
            attributes={"pipeline.name": pipeline_name},
        )


# ============================================================================
# Convenience Functions
# ============================================================================


def get_metrics_collector() -> Optional[VolnuxMetricsCollector]:
    """Get the global metrics collector instance"""
    return VolnuxMetricsCollector.get_instance()


def initialize_metrics(
    service_name: str, endpoint: Optional[str] = None, console_export: bool = False
) -> Optional[VolnuxMetricsCollector]:
    """
    Initialize metrics collection.

    Args:
        service_name: Name of the service
        endpoint: OTLP endpoint (e.g., "http://localhost:4317")
        console_export: Enable console export for debugging

    Returns:
        Initialized metrics collector

    Example:
        >>> from volnux.otel.metrics import initialize_metrics
        >>> collector = initialize_metrics(
        ...     service_name="workflow-service",
        ...     endpoint="http://localhost:4317"
        ... )
    """
    return VolnuxMetricsCollector.initialize(
        service_name=service_name, endpoint=endpoint, console_export=console_export
    )


# Export
__all__ = [
    "MetricType",
    "MetricDefinition",
    "VolnuxMetricsRegistry",
    "VolnuxMetricsCollector",
    "get_metrics_collector",
    "initialize_metrics",
]
