import logging
import threading
import time
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from volnux.exceptions import MultiValueError, ObjectDoesNotExist
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.result import ResultSet

from .publisher import MetricsPublisher

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from volnux.pipeline import PipelineExecutionMetrics


@dataclass
class EventMetrics(ObjectIdentityMixin):
    """Metrics for a single event execution"""

    event_name: str
    task_id: str
    start_time: float
    end_time: typing.Optional[float] = None
    status: str = "pending"
    error: typing.Optional[str] = None
    retry_count: int = 0
    process_id: typing.Optional[int] = None
    pipeline_id: typing.Optional[str] = None

    def __hash__(self) -> int:
        return hash(self.id)

    def duration(self) -> float:
        """Calculate execution duration in seconds"""
        if not self.end_time:
            return 0
        return self.end_time - self.start_time

    def to_dict(self) -> dict:
        """Convert to dictionary format"""
        return {
            "event_name": self.event_name,
            "task_id": self.task_id,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": (
                datetime.fromtimestamp(self.end_time).isoformat()
                if self.end_time
                else None
            ),
            "duration": f"{self.duration():.3f}s",
            "status": self.status,
            "error": self.error,
            "retry_count": self.retry_count,
            "process_id": self.process_id,
            "pipeline_id": self.pipeline_id,
        }


Task_Id = str
Pipeline_Id = typing.Union[str, None]


class AbstractTelemetryLogger(ABC):
    """Abstract telemetry logger"""

    @abstractmethod
    def start_event(
        self,
        event_name: str,
        task_id: str,
        process_id: typing.Optional[int] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the start of an event execution"""
        pass

    @abstractmethod
    def add_publisher(self, publisher: MetricsPublisher) -> None:
        """Add a metrics publisher"""
        pass

    @abstractmethod
    def end_event(
        self,
        task_id: str,
        name: str,
        error: typing.Optional[str] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the end of an event execution"""
        pass

    @abstractmethod
    def record_retry(
        self, task_id: str, name: str, pipeline_id: typing.Optional[str] = None
    ) -> None:
        """Record a retry attempt for an event"""
        pass

    @abstractmethod
    def get_metrics(self, **kwargs) -> typing.Union[EventMetrics, ResultSet, None]:
        """Get metrics for a specific task"""
        pass

    @abstractmethod
    def get_all_metrics(
        self,
    ) -> typing.Union[ResultSet, typing.Dict[Pipeline_Id, ResultSet]]:
        """Get all collected metrics"""
        pass

    @abstractmethod
    def add_pipeline_metrics(
        self, pipeline_execution_metrics: "PipelineExecutionMetrics"
    ):
        """add and publish pipeline metrics"""
        pass


class StandardTelemetryLogger(AbstractTelemetryLogger):
    """
    Thread-safe telemetry logger for event pipeline monitoring.
    Tracks metrics, events, and performance data.
    """

    def __init__(self):
        """ """
        self._metrics: ResultSet = ResultSet([])
        self._lock = threading.Lock()
        self._publishers: typing.List[MetricsPublisher] = []

    def add_publisher(self, publisher: MetricsPublisher) -> None:
        """Add a metrics publisher"""
        self._publishers.append(publisher)

    def remove_publisher(self, publisher: MetricsPublisher) -> None:
        """Remove a metrics publisher"""
        if publisher in self._publishers:
            self._publishers.remove(publisher)

    def start_event(
        self,
        event_name: str,
        task_id: str,
        process_id: typing.Optional[int] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the start of an event execution"""
        with self._lock:
            self._metrics.add(
                EventMetrics(
                    event_name=event_name,
                    task_id=task_id,
                    start_time=time.time(),
                    process_id=process_id,
                    pipeline_id=pipeline_id,
                )
            )

            logger.debug(f"Started tracking event: {event_name} (task_id: {task_id})")

    def end_event(
        self,
        task_id: str,
        name: str,
        error: typing.Optional[str] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the end of an event execution"""
        with self._lock:
            try:
                metric = self._metrics.get(event_name=name, task_id=task_id)
            except MultiValueError:
                logger.error(
                    f"Multiple metrics found for event_name='{name}' and task_id='{task_id}'. "
                    f"This suggests you may be using the wrong logger type. "
                    f"Consider using DefaultBatchTelemetryLogger for batch processing."
                )
                return
            except ObjectDoesNotExist:
                logger.error(
                    f"No metric found for event_name='{name}' and task_id='{task_id}'. "
                    f"Make sure start_event was called before end_event."
                )
                return

            if metric is None:
                logger.error(
                    f"No metric found for event_name='{name}' and task_id='{task_id}'. "
                    f"Make sure start_event was called before end_event."
                )
                return

            metric.end_time = time.time()
            metric.status = "failed" if error else "completed"
            metric.error = error

            for publisher in self._publishers:
                try:
                    publisher.publish_event_metrics(metric)
                except Exception as e:
                    logger.error(f"Failed to publish metrics: {e}")

            logger.debug(
                f"Event {metric.event_name} {metric.status} "
                f"in {metric.duration():.2f}s (task_id: {task_id})"
            )

    def record_retry(
        self, task_id: str, name: str, pipeline_id: typing.Optional[str] = None
    ) -> None:
        """Record a retry attempt for an event"""
        with self._lock:
            metric = self._metrics.get(event_name=name, task_id=task_id)

            metric.retry_count += 1
            logger.debug(f"Retry #{metric.retry_count} for task {task_id}")

    def get_metrics(self, **kwargs) -> typing.Optional[EventMetrics]:
        """Get metrics for a specific task"""
        with self._lock:
            name = kwargs.get("event_name")
            return self._metrics.get(event_name=name)

    def get_all_metrics(self) -> ResultSet:
        """Get all collected metrics"""
        with self._lock:
            return self._metrics.copy()

    def add_pipeline_metrics(self, metrics: "PipelineExecutionMetrics") -> None:
        """publish pipeline metrics"""
        for publisher in self._publishers:
            try:
                publisher.publish_batch_pipeline_metrics(metrics)
            except Exception as e:
                logger.error(f"❗️Failed to publish metrics: {e}")


class DefaultBatchTelemetryLogger(AbstractTelemetryLogger):
    """
    Thread-safe telemetry logger for event pipeline monitoring.
    Tracks metrics, events, and performance data.
    """

    def __init__(self):
        """ """
        self._metrics: typing.Dict[Pipeline_Id, ResultSet] = {}
        self._lock = threading.Lock()
        self._publishers: typing.List[MetricsPublisher] = []

    def add_publisher(self, publisher: MetricsPublisher) -> None:
        """Add a metrics publisher"""
        self._publishers.append(publisher)

    def remove_publisher(self, publisher: MetricsPublisher) -> None:
        """Remove a metrics publisher"""
        if publisher in self._publishers:
            self._publishers.remove(publisher)

    def start_event(
        self,
        event_name: str,
        task_id: str,
        process_id: typing.Optional[int] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the start of an event execution"""
        with self._lock:
            self._metrics.setdefault(pipeline_id, ResultSet([])).add(
                EventMetrics(
                    event_name=event_name,
                    task_id=task_id,
                    start_time=time.time(),
                    process_id=process_id,
                    pipeline_id=pipeline_id,
                )
            )
            logger.debug(f"Started tracking event: {event_name} (task_id: {task_id})")

    def end_event(
        self,
        task_id: str,
        name: str,
        error: typing.Optional[str] = None,
        pipeline_id: typing.Optional[str] = None,
    ) -> None:
        """Record the end of an event execution"""
        with self._lock:
            if pipeline_id not in self._metrics:
                logger.error(
                    f"No metrics found for pipeline_id='{pipeline_id}'. "
                    f"Make sure start_event was called before end_event."
                )
                return

            pipeline_metrics = self._metrics[pipeline_id]

            try:
                metrics = pipeline_metrics.get(event_name=name, task_id=task_id)
            except ObjectDoesNotExist:
                logger.error(
                    f"No metric found for event_name='{name}' and task_id='{task_id}' "
                    f"in pipeline_id='{pipeline_id}'. Make sure start_event was called before end_event."
                )
                return

            if metrics is None:
                logger.error(
                    f"No metric found for event_name='{name}' and task_id='{task_id}' "
                    f"in pipeline_id='{pipeline_id}'. Make sure start_event was called before end_event."
                )
                return

            metrics.end_time = time.time()
            metrics.status = "failed" if error else "completed"
            metrics.error = error

            # Publish metrics
            for publisher in self._publishers:
                try:
                    publisher.publish_event_metrics(metrics)
                except Exception as e:
                    logger.error(f"Failed to publish metrics: {e}")

            logger.debug(
                f"Event {metrics.event_name} {metrics.status} "
                f"in {metrics.duration():.2f}s (task_id: {task_id})"
            )

    def record_retry(
        self, task_id: str, name: str, pipeline_id: typing.Optional[str] = None
    ) -> None:
        """Record a retry attempt for an event"""
        with self._lock:
            pipeline_metrics = self._metrics[pipeline_id]
            metrics = pipeline_metrics.get(event_name=name, task_id=task_id)
            metrics.retry_count += 1
            logger.debug(f"Retry #{metrics.retry_count} for task {task_id}")

    def get_metrics(self, **kwargs) -> ResultSet:
        """Get metrics for a specific task"""
        with self._lock:
            pipeline_id = kwargs.get("pipeline_id")
            return self._metrics[pipeline_id]

    def get_all_metrics(self) -> typing.Dict[Pipeline_Id, ResultSet]:
        """Get all collected metrics"""
        with self._lock:
            return self._metrics.copy()

    def add_pipeline_metrics(self, metrics: "PipelineExecutionMetrics") -> None:
        """publish pipeline metrics"""
        for publisher in self._publishers:
            try:
                publisher.publish_batch_pipeline_metrics(metrics)
            except Exception as e:
                logger.error(f"❗️Failed to publish metrics: {e}")
