Telemetry
=========

This section covers the telemetry capabilities in the volnux framework.

Overview
--------

The volnux library includes built-in telemetry capabilities for monitoring and tracking:

- Event execution (timing, success/failure, retries)
- Network operation monitoring
- Performance metrics collection
- Metrics publishing to various backends

Basic Usage
----------

Enable and collect telemetry data:

.. code-block:: python

    from volnux.telemetry import monitor_events, get_metrics

    # Enable telemetry collection
    monitor_events()

    # Run your pipeline...

    # Get metrics after execution
    metrics_json = get_metrics()
    print(metrics_json)

    # Get specific metrics
    failed_events = get_failed_events()
    slow_events = get_slow_events(threshold_seconds=2.0)
    retry_stats = get_retry_stats()

Network Telemetry
---------------

Monitor network operations in remote execution scenarios:

.. code-block:: python

    from volnux.telemetry import (
        get_failed_network_ops,
        get_slow_network_ops
    )

    # Get metrics for failed network operations
    failed_ops = get_failed_network_ops()

    # Get metrics for slow network operations (> 1 second)
    slow_ops = get_slow_network_ops(threshold_seconds=1.0)

Network telemetry tracks:
- Operation latency
- Bytes sent/received
- Connection errors
- Host/port information

Metrics Publishing
---------------

The telemetry module supports publishing metrics to various monitoring systems through publishers.

Elasticsearch Publisher
~~~~~~~~~~~~~~~~~~~~

Publish metrics to Elasticsearch for Kibana visualization:

.. code-block:: python

    from volnux.telemetry import ElasticsearchPublisher

    es_publisher = ElasticsearchPublisher(
        hosts=["localhost:9200"],
        index_prefix="pipeline-metrics"
    )
    monitor_events([es_publisher])

Prometheus Publisher
~~~~~~~~~~~~~~~~~

Expose metrics for Prometheus scraping:

.. code-block:: python

    from volnux.telemetry import PrometheusPublisher

    prometheus_publisher = PrometheusPublisher(port=9090)
    monitor_events([prometheus_publisher])

Grafana Cloud Publisher
~~~~~~~~~~~~~~~~~~~~

Publish metrics directly to Grafana Cloud:

.. code-block:: python

    from volnux.telemetry import GrafanaCloudPublisher

    grafana_publisher = GrafanaCloudPublisher(
        api_key="your-api-key",
        org_slug="your-org"
    )
    monitor_events([grafana_publisher])

Composite Publisher
~~~~~~~~~~~~~~~~

Publish metrics to multiple backends simultaneously:

.. code-block:: python

    from volnux.telemetry import CompositePublisher

    publisher = CompositePublisher([
        es_publisher,
        prometheus_publisher,
        grafana_publisher
    ])
    monitor_events([publisher])

Dashboard Templates
----------------

The framework provides sample dashboard templates for visualization:

Prometheus + Grafana Dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Located at `examples/telemetry/prometheus_dashboard.json`, includes:

- Event duration metrics
- Retry statistics
- Network throughput
- Latency tracking

Elasticsearch + Kibana Dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Located at `examples/telemetry/elasticsearch_dashboard.json`, includes:

- Event duration distribution
- Status breakdown
- Network performance metrics
- Error tracking

Installation
-----------

To use metrics publishing features, install the required dependencies:

.. code-block:: bash

    pip install "event-pipeline[metrics]"

This installs optional dependencies for each publisher:

- elasticsearch-py for Elasticsearch
- prometheus-client for Prometheus
- requests for Grafana Cloud

Custom Publishers
--------------

Create custom publishers by implementing the MetricsPublisher interface:

.. code-block:: python

    from volnux.telemetry import MetricsPublisher

    class CustomPublisher(MetricsPublisher):
        def publish_event_metrics(self, metrics: EventMetrics) -> None:
            # Implement event metrics publishing
            pass

        def publish_network_metrics(self, metrics: dict) -> None:
            # Implement network metrics publishing
            pass