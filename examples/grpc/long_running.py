from volnux import EventBase
from volnux.executors.grpc_executor import GRPCExecutor


class LongRunningTask(EventBase):
    executor = GRPCExecutor
    executor_config = {
        "host": "localhost",
        "port": 8990,
        "max_workers": 4,
        "use_encryption": False,
    }

    def process(self, iterations: int) -> tuple[bool, list]:
        # Simulating a long running task that processes data in chunks
        results = []
        for i in range(iterations):
            # Do some work
            chunk_result = i * i
            results.append(chunk_result)

        return True, results


def run_example():
    # Create task
    task = LongRunningTask(execution_context={}, task_id="long-task-1")

    # Submit with streaming enabled
    future = task.executor.submit(
        task.process, iterations=10, long_running=True  # Enable streaming updates
    )

    # Add status callback
    def on_status_update(status, message):
        print(f"Task status: {status} - {message}")

    future.add_status_callback(on_status_update)

    # Wait for result
    result = future.result()
    print(f"Final result: {result}")


if __name__ == "__main__":
    run_example()
