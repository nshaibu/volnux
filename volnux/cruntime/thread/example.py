import time
from concurrent.futures import ProcessPoolExecutor

import threadpool_ext


def cpu_bound_task(n):
    """Simulates a CPU-bound task"""
    result = 0
    for i in range(n):
        result += i**2
    return result


def io_bound_task(duration):
    """Simulates an I/O-bound task"""
    time.sleep(duration)
    return f"Slept for {duration} seconds"


def task_with_exception():
    """Task that raises an exception"""
    raise ValueError("This is a test exception")


def main():
    # Create thread pool with 4 workers
    executor = threadpool_ext.ThreadPoolExecutor(max_workers=4)
    pexecutor = ProcessPoolExecutor(max_workers=4)

    print("=== Testing CPU-bound tasks ===")
    start = time.time()
    futures = []

    # Submit multiple CPU-bound tasks
    for i in range(8):
        future = executor.submit(cpu_bound_task, (10000000,))
        futures.append(future)

    # Wait for results
    results = [f.result() for f in futures]
    print(f"Completed 8 CPU-bound tasks in {time.time() - start:.2f} seconds")
    print(f"First result: {results[0]}")

    print("\n=== Testing I/O-bound tasks ===")
    start = time.time()
    futures = []

    # Submit multiple I/O-bound tasks
    for i in range(8):
        future = executor.submit(io_bound_task, (0.5,))
        futures.append(future)

    results = [f.result() for f in futures]
    print(f"Completed 8 I/O-bound tasks in {time.time() - start:.2f} seconds")
    print(f"Results: {results[0]}")

    print("\n=== Testing exception handling ===")
    future = executor.submit(task_with_exception, ())
    try:
        result = future.result()
    except ValueError as e:
        print(f"Caught exception: {e}")

    print("\n=== Testing with lambda ===")
    future = executor.submit(lambda x, y: x + y, (10, 20))
    print(f"Lambda result: {future.result()}")

    # Shutdown the executor
    executor.shutdown(wait=True)
    print("\nExecutor shut down successfully")


if __name__ == "__main__":
    main()
