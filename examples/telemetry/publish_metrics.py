"""Example demonstrating metrics publishing integration with dashboard setup

This example shows how to:
1. Set up metrics publishers for different backends (Elasticsearch, Prometheus, Grafana)
2. Configure dashboards to visualize the metrics
3. Integrate metrics publishing with events
"""

from volnux import EventBase
from volnux.telemetry import (
    monitor_events,
    ElasticsearchPublisher,
    PrometheusPublisher,
    GrafanaCloudPublisher,
    CompositePublisher,
)
from volnux.executors.remote_executor import RemoteExecutor


class MetricsPublishingEvent(EventBase):
    """Example event that demonstrates metrics publishing"""

    # Configure remote execution
    executor = RemoteExecutor
    executor_config = {"host": "localhost", "port": 8990, "use_encryption": True}

    def process(self, data: dict) -> tuple[bool, dict]:
        """Process data with metrics publishing"""
        # Process the data (metrics will be automatically published)
        processed = {
            key: value.upper() if isinstance(value, str) else value * 2
            for key, value in data.items()
        }
        return True, processed


def setup_elasticsearch_dashboard():
    """Set up Elasticsearch + Kibana dashboard

    1. Create an index pattern in Kibana:
       - Go to Stack Management > Index Patterns
       - Create pattern matching "pipeline-metrics-*"
       - Set timestamp field to @timestamp

    2. Create visualizations:
       - Event duration histogram
       - Success/failure ratio
       - Network latency graph
       - Retry count by event

    3. Combine visualizations into a dashboard
    """
    pass


def setup_prometheus_dashboard():
    """Set up Prometheus + Grafana dashboard

    1. Configure Prometheus scraping:
       prometheus.yml:
       ```yaml
       scrape_configs:
         - job_name: 'event_pipeline'
           static_configs:
             - targets: ['localhost:9090']
       ```

    2. Create Grafana dashboard:
       - Add Prometheus as data source
       - Import dashboard JSON provided in dashboard.json
       - Customize panels as needed
    """
    pass


def run_metrics_example():
    """Run the metrics publishing example with dashboard setup"""

    # Create publishers for different backends
    elasticsearch_pub = ElasticsearchPublisher(
        hosts=["localhost:9200"], index_prefix="pipeline-metrics"
    )

    prometheus_pub = PrometheusPublisher(port=9090)

    grafana_pub = GrafanaCloudPublisher(
        api_key="your-api-key", org_slug="your-org", region="prod-us-east-0"
    )

    # Create composite publisher to send metrics to all backends
    publisher = CompositePublisher([elasticsearch_pub, prometheus_pub, grafana_pub])

    # Set up dashboards (uncomment and configure as needed)
    # setup_elasticsearch_dashboard()
    # setup_prometheus_dashboard()

    # Enable telemetry with publishers
    monitor_events([publisher])

    # Create and execute event
    event = MetricsPublishingEvent(execution_context={}, task_id="metrics-demo-1")

    # Process some test data
    test_data = {
        "name": "test",
        "value": 42,
        "status": "active",
        "tags": ["demo", "metrics"],
    }

    # Execute multiple times to generate metrics
    for i in range(5):
        try:
            success, result = event.process(test_data)
            print(f"Processing {i+1} completed: {success}")
            print(f"Result: {result}")
        except Exception as e:
            print(f"Processing {i+1} failed: {e}")


if __name__ == "__main__":
    run_metrics_example()
