API Reference - Metrics
=======================

Core Metric Classes
-------------------

EventMetrics
~~~~~~~~~~~~
.. py:class:: EventMetrics

   Metrics for a single event execution.

   **Attributes:**

   .. py:attribute:: event_name
      :type: str

      Name of the event being tracked.

   .. py:attribute:: task_id
      :type: str

      ID of the task instance.

   .. py:attribute:: start_time
      :type: float

      Event start timestamp.

   .. py:attribute:: end_time
      :type: float

      Event completion timestamp.

   .. py:attribute:: status
      :type: str

      Current execution status.

   .. py:attribute:: error
      :type: Optional[str]

      Error message if failed.

   .. py:attribute:: retry_count
      :type: int

      Number of retry attempts.

   .. py:attribute:: process_id
      :type: Optional[int]

      Process ID executing the event.

NetworkMetrics
~~~~~~~~~~~~~~
.. py:class:: NetworkMetrics

   Metrics for network operations.

   **Attributes:**

   .. py:attribute:: operation
      :type: str

      Type of network operation.

   .. py:attribute:: host
      :type: str

      Remote host address.

   .. py:attribute:: port
      :type: int

      Remote port number.

   .. py:attribute:: start_time
      :type: float

      Operation start timestamp.

   .. py:attribute:: end_time
      :type: float

      Operation completion timestamp.

   .. py:attribute:: bytes_sent
      :type: int

      Number of bytes sent.

   .. py:attribute:: bytes_received
      :type: int

      Number of bytes received.

   .. py:attribute:: error
      :type: Optional[str]

      Error message if failed.

Publisher Classes
-----------------

MetricsPublisher
~~~~~~~~~~~~~~~~
.. py:class:: MetricsPublisher

   Base interface for metrics publishing adapters.

   **Methods:**

   .. py:method:: publish_event_metrics(metrics: EventMetrics) -> None

      Publishes event metrics to backend system.

      :param metrics: Event metrics to publish
      :type metrics: EventMetrics

   .. py:method:: publish_network_metrics(metrics: dict) -> None

      Publishes network metrics to backend system.

      :param metrics: Network metrics to publish
      :type metrics: dict

   .. py:method:: format_metrics(metrics: Union[EventMetrics, dict]) -> dict

      Formats metrics into standardized dictionary.

      :param metrics: Metrics to format
      :return: Formatted metrics dictionary
      :rtype: dict

ElasticsearchPublisher
~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: ElasticsearchPublisher

   Publishes metrics to Elasticsearch.

   **Parameters:**

   :param hosts: List of Elasticsearch hosts
   :type hosts: List[str]
   :param index_prefix: Prefix for metrics indices
   :type index_prefix: str
   :param kwargs: Additional client options

PrometheusPublisher
~~~~~~~~~~~~~~~~~~~
.. py:class:: PrometheusPublisher

   Exposes metrics for Prometheus scraping.

   **Parameters:**

   :param port: Port to expose metrics on
   :type port: int

   **Metrics Exposed:**

   - event_duration_seconds (Histogram)
   - event_retries_total (Counter) 
   - network_bytes_total (Counter)
   - network_latency_seconds (Histogram)

GrafanaCloudPublisher
~~~~~~~~~~~~~~~~~~~~~
.. py:class:: GrafanaCloudPublisher

   Publishes metrics to Grafana Cloud.

   **Parameters:**

   :param api_key: Grafana Cloud API key
   :type api_key: str
   :param org_slug: Organization identifier
   :type org_slug: str
   :param region: Cloud region
   :type region: str

CompositePublisher
~~~~~~~~~~~~~~~~~~
.. py:class:: CompositePublisher

   Publishes metrics to multiple backends.

   **Parameters:**

   :param publishers: List of publisher instances
   :type publishers: List[MetricsPublisher]