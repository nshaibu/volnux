from volnux import EventBase, RetryPolicy
from volnux.executors.remote_executor import RemoteExecutor
from volnux.telemetry import monitor_events, get_metrics, get_slow_events


class DataProcessingEvent(EventBase):
    """Example event that demonstrates telemetry integration"""

    # Configure remote execution
    executor = RemoteExecutor
    executor_config = {
        "host": "localhost",
        "port": 8990,
        "use_encryption": True,
        "timeout": 30,
    }

    # Configure retry policy
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_factor=0.1,
        max_backoff=5.0,
        retry_on_exceptions=[ConnectionError, TimeoutError],
    )

    def process(self, data: dict) -> tuple[bool, dict]:
        """Process data with telemetry tracking"""
        # Processing will be automatically tracked by telemetry
        processed_data = {
            key: value.upper() if isinstance(value, str) else value
            for key, value in data.items()
        }
        return True, processed_data


def run_example():
    # Enable telemetry collection
    monitor_events()

    # Create and process events
    event = DataProcessingEvent(execution_context={}, task_id="data-processing-1")

    # Process some test data
    test_data = {"name": "test", "value": 42, "status": "active"}

    # Process multiple times to generate metrics
    for i in range(5):
        success, result = event.process(test_data)
        print(f"Processing {i+1} completed: {success}")

    # Get telemetry metrics
    metrics = get_metrics()
    print("\nAll metrics:")
    print(metrics)

    # Check for slow operations
    slow_ops = get_slow_events(threshold_seconds=0.1)
    print("\nSlow operations:")
    print(slow_ops)


if __name__ == "__main__":
    run_example()
