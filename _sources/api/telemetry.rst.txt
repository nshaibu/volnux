API Reference - Telemetry
====================

Core Components
-------------

EventMetrics
~~~~~~~~~~
.. py:class:: EventMetrics

   Metrics for a single event execution.

   **Attributes:**

   :param event_name: Name of the event
   :param task_id: Unique task identifier
   :param start_time: Event start timestamp
   :param end_time: Event end timestamp
   :param status: Execution status (pending/completed/failed)
   :param error: Error message if failed
   :param retry_count: Number of retry attempts
   :param process_id: Process ID executing the event

   **Methods:**

   .. py:method:: duration() -> float

      Calculate execution duration in seconds.

NetworkMetrics
~~~~~~~~~~~~
.. py:class:: NetworkMetrics

   Metrics for a network operation.

   **Attributes:**

   :param task_id: Unique task identifier
   :param host: Remote host address
   :param port: Remote port number
   :param start_time: Operation start timestamp
   :param end_time: Operation end timestamp
   :param bytes_sent: Number of bytes sent
   :param bytes_received: Number of bytes received
   :param error: Error message if failed
   :param operation: Operation type identifier

   **Methods:**

   .. py:method:: latency() -> float

      Calculate operation latency in seconds.

Telemetry Collection
-----------------

TelemetryLogger
~~~~~~~~~~~~
.. py:class:: TelemetryLogger

   Thread-safe telemetry logger for event pipeline monitoring.

   **Methods:**

   .. py:method:: start_event(event_name: str, task_id: str, process_id: Optional[int] = None)

      Record the start of an event execution.

   .. py:method:: end_event(task_id: str, error: Optional[str] = None)

      Record the end of an event execution.

   .. py:method:: record_retry(task_id: str)

      Record a retry attempt for an event.

   .. py:method:: get_metrics(task_id: str) -> Optional[EventMetrics]

      Get metrics for a specific task.

NetworkTelemetry
~~~~~~~~~~~~~
.. py:class:: NetworkTelemetry

   Thread-safe telemetry for network operations.

   **Methods:**

   .. py:method:: start_operation(task_id: str, host: str, port: int, operation: str = "remote_call")

      Record the start of a network operation.

   .. py:method:: end_operation(task_id: str, bytes_sent: int = 0, bytes_received: int = 0, error: Optional[str] = None)

      Record the end of a network operation.

   .. py:method:: get_metrics(task_id: str) -> Optional[NetworkMetrics]

      Get metrics for a specific operation.

   .. py:method:: get_failed_operations() -> Dict[str, NetworkMetrics]

      Get metrics for all failed operations.

   .. py:method:: get_slow_operations(threshold_seconds: float = 1.0) -> Dict[str, NetworkMetrics]

      Get metrics for operations that took longer than threshold.

Metrics Publishing
---------------

MetricsPublisher
~~~~~~~~~~~~~
.. py:class:: MetricsPublisher

   Base interface for metrics publishing adapters.

   **Methods:**

   .. py:method:: publish_event_metrics(metrics: EventMetrics)

      Publish event metrics to the backend system.

   .. py:method:: publish_network_metrics(metrics: dict)

      Publish network metrics to the backend system.

ElasticsearchPublisher
~~~~~~~~~~~~~~~~~~~
.. py:class:: ElasticsearchPublisher

   Publishes metrics to Elasticsearch.

   **Parameters:**

   :param hosts: List of Elasticsearch hosts
   :param index_prefix: Prefix for metrics indices
   :param **kwargs: Additional Elasticsearch client options

PrometheusPublisher
~~~~~~~~~~~~~~~~
.. py:class:: PrometheusPublisher

   Exposes metrics for Prometheus scraping.

   **Parameters:**

   :param port: Port to expose metrics on
   :param addr: Interface address to bind to
   :param registry: Custom Prometheus registry

GrafanaCloudPublisher
~~~~~~~~~~~~~~~~~~
.. py:class:: GrafanaCloudPublisher

   Publishes metrics to Grafana Cloud.

   **Parameters:**

   :param api_key: Grafana Cloud API key
   :param org_slug: Organization identifier
   :param endpoint: Custom API endpoint

CompositePublisher
~~~~~~~~~~~~~~~
.. py:class:: CompositePublisher

   Publishes metrics to multiple backends simultaneously.

   **Parameters:**

   :param publishers: List of MetricsPublisher instances

Utility Functions
--------------

.. py:function:: monitor_events(publishers: Optional[List[MetricsPublisher]] = None)

   Start monitoring pipeline events and collecting metrics.

.. py:function:: get_metrics() -> str

   Get all collected metrics as JSON string.

.. py:function:: get_failed_events() -> list

   Get metrics for all failed events.

.. py:function:: get_slow_events(threshold_seconds: float = 1.0) -> List[Dict[str, Any]]

   Get metrics for events that took longer than threshold.

.. py:function:: get_retry_stats() -> Dict[str, Any]

   Get retry statistics.

.. py:function:: get_failed_network_ops() -> Dict[str, Any]

   Get metrics for failed network operations.

.. py:function:: get_slow_network_ops(threshold_seconds: float = 1.0) -> Dict[str, Any]

   Get metrics for slow network operations.