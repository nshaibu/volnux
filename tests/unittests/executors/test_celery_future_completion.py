"""CeleryFuture must actually complete (callbacks included).

It used to override done()/result() to read the AsyncResult but never fired its
callbacks, which asyncio's wrap_future (and run_in_executor) depend on: a Celery
task that finished in milliseconds left an awaiting engine hanging forever.

Runs a real Celery app with an in-process worker on the in-memory broker.
"""

import asyncio
import operator
import threading
import time

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

from volnux.executors.celery import CeleryExecutor


def boom(message):
    raise ValueError(message)


@pytest.fixture(scope="module")
def executor():
    app = Celery("future-completion", broker="memory://", backend="cache+memory://")
    app.conf.task_serializer = app.conf.result_serializer = "pickle"
    app.conf.accept_content = ["pickle"]
    pool = CeleryExecutor(app)
    with start_worker(app, pool="solo", loglevel="WARNING", perform_ping_check=False):
        yield pool
    pool.shutdown(wait=False)


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_callbacks_fire_when_the_task_succeeds(executor):
    fired = []
    future = executor.submit(operator.add, 2, 3)
    future.add_done_callback(lambda f: fired.append(f.result()))

    assert wait_until(lambda: fired), "the done-callback never ran"
    assert fired == [5]


def test_callbacks_fire_when_the_task_fails(executor):
    fired = []
    future = executor.submit(boom, "worker said no")
    future.add_done_callback(lambda f: fired.append(f))

    assert wait_until(lambda: fired)
    assert isinstance(fired[0].exception(), ValueError)
    with pytest.raises(ValueError, match="worker said no"):
        fired[0].result()


def test_a_callback_runs_exactly_once(executor):
    count = []
    future = executor.submit(operator.add, 1, 1)
    future.add_done_callback(lambda f: count.append(1))
    assert wait_until(lambda: count)
    time.sleep(0.6)  # several more watcher ticks
    assert count == [1]


def test_a_callback_added_after_completion_runs_immediately(executor):
    future = executor.submit(operator.add, 1, 1)
    assert wait_until(future.done)
    wait_until(lambda: future._state == "FINISHED")  # watcher has recorded it

    fired = []
    future.add_done_callback(lambda f: fired.append(True))
    assert fired == [True]


@pytest.mark.asyncio
async def test_asyncio_can_await_a_celery_future(executor):
    loop = asyncio.get_running_loop()
    value = await asyncio.wait_for(
        loop.run_in_executor(executor, operator.mul, 6, 7), 10
    )
    assert value == 42


@pytest.mark.asyncio
async def test_asyncio_sees_a_celery_failure(executor):
    loop = asyncio.get_running_loop()
    with pytest.raises(ValueError, match="remote failure"):
        await asyncio.wait_for(
            loop.run_in_executor(executor, boom, "remote failure"), 10
        )


def test_the_watcher_stops_when_nothing_is_pending(executor):
    future = executor.submit(operator.add, 1, 2)
    assert wait_until(lambda: future._state == "FINISHED")

    assert wait_until(
        lambda: not any(
            t.name == "volnux-celery-watcher" for t in threading.enumerate()
        )
    ), "the watcher thread outlived its work"
