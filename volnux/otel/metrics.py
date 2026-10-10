"""
Comprehensive Metrics Collection for Volnux

This module provides a thin OpenTelemetry metrics recording layer for Volnux.
It assumes OpenTelemetry has already been initialized by the observability bootstrap.
"""

import logging
import typing
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from opentelemetry import metrics

from volnux import __version__ as volnux_version

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics collected."""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    UP_DOWN_COUNTER = "up_down_counter"


@dataclass(frozen=True)
class MetricDefinition:
    """Definition of a metric."""

    name: str
    description: str
    unit: str
    metric_type: MetricType
    attributes: Optional[Dict[str, str]] = None


class VolnuxMetricsRegistry:
    """
    Central registry for all Volnux metrics.
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
        metric_type=MetricType.COUNTER,
    )
    ENGINE_TARGET_WORKERS = MetricDefinition(
        name="engine.target_workers",
        description="Target worker count",
        unit="1",
        metric_type=MetricType.UP_DOWN_COUNTER,
    )
    ENGINE_ACTUAL_WORKERS = MetricDefinition(
        name="engine.actual_workers",
        description="Actual active worker count",
        unit="1",
        metric_type=MetricType.UP_DOWN_COUNTER,
    )
    ENGINE_QUEUE_LENGTH = MetricDefinition(
        name="engine.queue_length",
        description="Current task queue length",
        unit="1",
        metric_type=MetricType.UP_DOWN_COUNTER,
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
        unit="cores",
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


class VolnuxMetricsCollector:
    """
    Thin metrics collector for Volnux.

    Assumes OpenTelemetry metrics provider is already configured.
    """

    _instance: Optional["VolnuxMetricsCollector"] = None
    _meter: Optional[metrics.Meter] = None
    _instruments: Dict[str, Any] = {}
    _initialized: bool = False

    def __init__(self, meter: Optional[metrics.Meter] = None):
        self._instruments = {}
        self._meter = meter or self.__class__._meter

    @classmethod
    def initialize(
        cls,
        service_name: str,
        service_version: str = "1.0.0",
    ) -> "VolnuxMetricsCollector":
        """
        Initializes the `VolnuxMetricsCollector` singleton instance, setting up
        the metrics collection instruments and returning the instance. Subsequent
        calls will return the previously initialized instance if it exists.

        :param service_name: Name of the service for which the metrics collector
            is being initialized.
        :param service_version: Version of the service. Defaults to "1.0.0".
        :return: The singleton instance of `VolnuxMetricsCollector`.
        :rtype: VolnuxMetricsCollector
        :raises Exception: If the metrics collector fails to initialize.
        """
        if cls._instance and cls._initialized:
            return cls._instance

        try:
            cls._meter = metrics.get_meter(service_name, version=service_version)
            cls._instance = cls(cls._meter)
            cls._instance._setup_instruments()
            cls._initialized = True
            logger.info("Metrics collector initialized for %s", service_name)
            return cls._instance
        except Exception as e:
            logger.error("Failed to initialize metrics collector: %s", e, exc_info=True)
            raise

    @classmethod
    def get_instance(cls) -> Optional["VolnuxMetricsCollector"]:
        """Get the global metrics collector instance."""
        return cls._instance

    @classmethod
    def shutdown(cls) -> None:
        """Reset the collector state."""
        cls._instance = None
        cls._meter = None
        cls._instruments = {}
        cls._initialized = False
        logger.info("Metrics collector shut down")

    def _setup_instruments(self) -> None:
        """Setup all metric instruments from registry."""
        if not self._meter:
            logger.warning(
                "No meter available; metrics instruments will not be created"
            )
            return

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
                    logger.warning(
                        "Unsupported metric type: %s", metric_def.metric_type
                    )
                    continue

                self._instruments[metric_def.name] = instrument
                logger.debug("Created metric instrument: %s", metric_def.name)

            except Exception as e:
                logger.error("Failed to create instrument %s: %s", metric_def.name, e)

    def record_metric(
        self,
        metric_def: MetricDefinition,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a metric value.
        """
        instrument = self._instruments.get(metric_def.name)
        if not instrument:
            logger.debug("Instrument not found: %s", metric_def.name)
            return

        attrs = attributes or {}
        try:
            if metric_def.metric_type in (
                MetricType.COUNTER,
                MetricType.UP_DOWN_COUNTER,
            ):
                instrument.add(value, attributes=attrs)
            elif metric_def.metric_type == MetricType.HISTOGRAM:
                instrument.record(value, attributes=attrs)
        except Exception as e:
            logger.error("Failed to record metric %s: %s", metric_def.name, e)

    def record_engine_metrics(
        self, metrics_dict: Dict[str, Any], pipeline_name: str
    ) -> None:
        """Record runtime engine metrics from a metrics snapshot."""
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH,
            float(metrics_dict.get("task_queue_length", 0)),
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_TARGET_WORKERS,
            float(metrics_dict.get("target_workers", 0)),
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_ACTUAL_WORKERS,
            float(metrics_dict.get("actual_workers", 0)),
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_PARALLEL_TASKS,
            float(metrics_dict.get("parallel_tasks", 0)),
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.CPU_USAGE,
            float(metrics_dict.get("cpu_usage_cores", 0.0)),
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.MEMORY_USAGE,
            float(metrics_dict.get("memory_usage_gb", 0.0)),
            attributes={"pipeline.name": pipeline_name},
        )

    def record_system_utilization(
        self,
        cpu_usage_cores: float,
        memory_usage_mb: float,
        pipeline_name: str,
    ) -> None:
        """Record system resource utilization."""
        self.record_metric(
            VolnuxMetricsRegistry.CPU_USAGE,
            cpu_usage_cores,
            attributes={"pipeline.name": pipeline_name},
        )
        self.record_metric(
            VolnuxMetricsRegistry.MEMORY_USAGE,
            memory_usage_mb,
            attributes={"pipeline.name": pipeline_name},
        )

    def record_workflow_duration(
        self,
        duration_ms: float,
        pipeline_name: str,
        status: str,
        environment: str = "production",
    ) -> None:
        """Record workflow execution duration."""
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
    ) -> None:
        """Increment workflow execution counter."""
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
    ) -> None:
        """Record task execution duration."""
        self.record_metric(
            VolnuxMetricsRegistry.TASK_DURATION,
            duration_ms,
            attributes={
                "task.name": task_name,
                "task.status": status,
                "pipeline.name": pipeline_name,
            },
        )

    def increment_task_count(self, task_name: str, pipeline_name: str) -> None:
        """Increment task execution counter."""
        self.record_metric(
            VolnuxMetricsRegistry.TASK_COUNT,
            1,
            attributes={"task.name": task_name, "pipeline.name": pipeline_name},
        )

    def increment_error_count(
        self, error_type: str, component: str, pipeline_name: Optional[str] = None
    ) -> None:
        """Increment error counter."""
        attrs = {"error.type": error_type, "component": component}
        if pipeline_name:
            attrs["pipeline.name"] = pipeline_name

        self.record_metric(VolnuxMetricsRegistry.ERROR_COUNT, 1, attributes=attrs)

    def record_parallel_tasks(self, count: int, pipeline_name: str) -> None:
        """Record number of parallel tasks."""
        self.record_metric(
            VolnuxMetricsRegistry.ENGINE_PARALLEL_TASKS,
            count,
            attributes={"pipeline.name": pipeline_name},
        )

    def record_executor_usage(self, executor_type: str, pipeline_name: str) -> None:
        """Record executor usage."""
        self.record_metric(
            VolnuxMetricsRegistry.EXECUTOR_USAGE,
            1,
            attributes={"executor.type": executor_type, "pipeline.name": pipeline_name},
        )

    def record_business_metric(
        self, records_processed: int, data_volume_mb: float, pipeline_name: str
    ) -> None:
        """Record business metrics."""
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


def get_metrics_collector() -> Optional[VolnuxMetricsCollector]:
    """Get the global metrics collector instance."""
    return VolnuxMetricsCollector.get_instance()


def initialize_metrics(
    service_name: str,
    service_version: str = volnux_version,
) -> Optional[VolnuxMetricsCollector]:
    """
    Initialize metrics collection.

    OTEL bootstrap should be done first.
    """
    return VolnuxMetricsCollector.initialize(
        service_name=service_name,
        service_version=service_version,
    )


__all__ = [
    "MetricType",
    "MetricDefinition",
    "VolnuxMetricsRegistry",
    "VolnuxMetricsCollector",
    "get_metrics_collector",
    "initialize_metrics",
]
