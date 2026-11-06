"""Task functions for the pool tests: module-level, so a spawned worker can import them."""

import os
import time


def sleep_and_return(seconds, value):
    time.sleep(seconds)
    return value


def record(path, value, seconds=0.0):
    """Append ``value`` to a file (one line each) - tells the order tasks ran in."""
    time.sleep(seconds)
    with open(path, "a") as handle:
        handle.write(f"{value}\n")
    return value


def whoami():
    return os.getpid()


def boom():
    raise ValueError("task failed")
