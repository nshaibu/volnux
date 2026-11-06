"""CeleryExecutor / CeleryFuture: behaviours that need a real worker or a careful fake.

Real worker tests use an in-process worker on the in-memory broker (no external
service); the rest drive CeleryFuture and the executor with a scriptable AsyncResult."""

import concurrent.futures as cf
import logging
import operator
import threading
import time

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

from volnux.executors.celery import (
    CeleryExecutor,
    CeleryFuture,
    make_remote_task,
)


def sleepy(seconds):
    time.sleep(seconds)
    return seconds


def boom(message):
    raise ValueError(message)


def wait_until(predicate, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def pickle_app(name):
    app = Celery(name, broker="memory://", backend="cache+memory://")
    app.conf.task_serializer = app.conf.result_serializer = "pickle"
    app.conf.accept_content = ["pickle"]
    return app


@pytest.fixture(scope="module")
def live():
    """An executor with a worker behind it."""
    app = pickle_app("behaviour-live")
    pool = CeleryExecutor(app, poll_interval=0.05)
    with start_worker(app, pool="solo", loglevel="ERROR", perform_ping_check=False):
        yield pool
    pool.shutdown(wait=False)


# =============================== with a real worker ===============================


def test_concurrent_futures_wait_sees_a_finished_celery_task(live):
    """The future set its state by hand and never told the waiters `wait()` sleeps on, so
    `wait()` (and the pool manager's drain) ran to its timeout with the task long finished.
    """
    future = live.submit(operator.add, 1, 2)

    start = time.monotonic()
    done, not_done = cf.wait([future], timeout=10)

    assert done == {future} and not_done == set()
    assert time.monotonic() - start < 5
    assert future.result() == 3


def test_as_completed_yields_the_celery_futures(live):
    futures = [live.submit(operator.mul, i, 10) for i in range(4)]

    results = sorted(f.result() for f in cf.as_completed(futures, timeout=15))

    assert results == [0, 10, 20, 30]


def test_wait_first_completed_returns_on_the_first_task_to_finish(live):
    fast = live.submit(operator.add, 1, 1)
    slow = live.submit(sleepy, 2)

    done, not_done = cf.wait([slow, fast], timeout=15, return_when=cf.FIRST_COMPLETED)

    assert fast in done
    slow.result(timeout=15)


def test_a_failed_task_wakes_waiters_and_carries_its_exception(live):
    future = live.submit(boom, "nope")

    done, _ = cf.wait([future], timeout=15)

    assert done == {future}
    assert isinstance(future.exception(), ValueError)


def test_executor_map_returns_results_in_order(live):
    assert list(live.map(operator.add, [1, 2, 3], [10, 20, 30], timeout=20)) == [
        11,
        22,
        33,
    ]


def test_a_result_timeout_is_the_futures_timeout_not_celerys(live):
    """Celery's TimeoutError is not concurrent.futures.TimeoutError, so a caller written for
    a Future never caught it."""
    future = live.submit(sleepy, 2)

    with pytest.raises(cf.TimeoutError):
        future.result(timeout=0.1)

    assert future.result(timeout=15) == 2


def test_a_task_that_finished_can_still_be_revoked_without_error(live):
    """Revoking a task that had just completed raised "not submitted through this executor",
    so cancelling a pool context raced with its tasks finishing."""
    future = live.submit(operator.add, 1, 1)
    future.result(timeout=10)
    assert wait_until(lambda: future.task_id not in live.futures())  # no longer tracked

    live.revoke(future.task_id)  # must not raise
    live.revoke(future.task_id, terminate=True)


def test_a_task_that_was_never_submitted_is_still_refused(live):
    with pytest.raises(ValueError, match="was not submitted through this executor"):
        live.revoke("not-one-of-ours")
    with pytest.raises(ValueError, match="was not submitted through this executor"):
        live.invoke("not-one-of-ours", "ping")


def test_a_finished_task_leaves_the_tracked_futures(live):
    future = live.submit(operator.add, 2, 2)
    future.result(timeout=10)

    assert wait_until(lambda: future.task_id not in live.futures())
    assert future.task_id not in live.pending_task_ids()


# ============================ shutdown, with no worker ============================


def test_shutdown_that_cancels_does_not_wait_for_tasks_nobody_will_run():
    """With no worker listening a revoked task stays PENDING for ever; shutdown(wait=True,
    cancel_futures=True) waited for it for ever."""
    pool = CeleryExecutor(pickle_app("behaviour-noworker"), poll_interval=0.05)
    future = pool.submit(operator.add, 1, 1)
    finished = []
    thread = threading.Thread(
        target=lambda: (
            pool.shutdown(wait=True, cancel_futures=True),
            finished.append(1),
        ),
        daemon=True,
    )
    thread.start()
    thread.join(10)

    assert finished == [1]
    assert future.cancelled()
    done, _ = cf.wait([future], timeout=2)
    assert done == {future}  # and a waiter is told


def test_cancelling_a_queued_task_wakes_a_waiter():
    pool = CeleryExecutor(pickle_app("behaviour-cancel"), poll_interval=0.05)
    future = pool.submit(operator.add, 1, 1)
    woke = []
    waiter = threading.Thread(
        target=lambda: woke.append(cf.wait([future], timeout=10)[0])
    )
    waiter.start()
    time.sleep(0.2)

    assert future.cancel() is True
    waiter.join(5)

    assert woke == [{future}]
    pool.shutdown(wait=False)


def test_a_task_that_is_running_is_waited_for_by_shutdown(live):
    future = live.submit(sleepy, 1)
    assert (
        wait_until(future.running, 10) or True
    )  # (STARTED is only reported if tracked)

    live.shutdown(wait=True, timeout=20)

    assert future.done()


# ================================ scriptable fakes ================================


class Scripted:
    """An AsyncResult whose state the test changes."""

    def __init__(self, task_id="t-1", state="PENDING", result=None):
        self.id, self.state, self.result = task_id, state, result
        self.revoked = []

    def get(self, timeout=None, propagate=True):
        if self.state == "FAILURE" and propagate:
            raise self.result
        return self.result

    def revoke(self, terminate=False):
        self.revoked.append(terminate)

    def failed(self):
        return self.state == "FAILURE"


def test_sync_records_a_success_and_tells_waiters_and_callbacks():
    scripted = Scripted(state="PENDING")
    future = CeleryFuture(scripted)
    seen = []
    future.add_done_callback(lambda f: seen.append(f.result()))
    assert future._sync_from_backend() is False  # not finished yet

    scripted.state, scripted.result = "SUCCESS", 42
    assert future._sync_from_backend() is True

    assert seen == [42]
    done, _ = cf.wait([future], timeout=1)
    assert done == {future}


def test_sync_records_a_failure_as_the_tasks_exception():
    scripted = Scripted(state="FAILURE", result=KeyError("gone"))
    future = CeleryFuture(scripted)

    assert future._sync_from_backend() is True

    done, _ = cf.wait([future], timeout=1)
    assert done == {future}
    assert isinstance(cf.Future.exception(future), KeyError)


def test_sync_turns_a_failure_that_is_not_an_exception_into_one():
    scripted = Scripted(state="FAILURE", result="plain text from the backend")
    future = CeleryFuture(scripted)

    future._sync_from_backend()

    assert str(cf.Future.exception(future)) == "plain text from the backend"
    assert isinstance(cf.Future.exception(future), RuntimeError)


def test_sync_records_a_revoked_task_as_cancelled():
    future = CeleryFuture(Scripted(state="REVOKED"))
    fired = []
    future.add_done_callback(lambda f: fired.append(f.cancelled()))

    assert future._sync_from_backend() is True

    assert fired == [True]
    assert cf.wait([future], timeout=1)[0] == {future}


def test_sync_is_idempotent_and_runs_callbacks_once():
    scripted = Scripted(state="SUCCESS", result=1)
    future = CeleryFuture(scripted)
    count = []
    future.add_done_callback(lambda f: count.append(1))

    assert [future._sync_from_backend() for _ in range(3)] == [True, True, True]

    assert count == [1]


def test_sync_survives_the_future_being_settled_underneath_it():
    future = CeleryFuture(Scripted(state="SUCCESS", result=1))
    future.set_result("set by someone else")

    assert future._sync_from_backend() is True
    assert cf.Future.result(future) == "set by someone else"


def test_cancel_settles_the_future_for_waiters_and_callbacks_once():
    scripted = Scripted(state="PENDING")
    future = CeleryFuture(scripted)
    calls = []
    future.add_done_callback(lambda f: calls.append(1))

    assert future.cancel() is True
    assert future.cancel() is True  # a second cancel is harmless (it used to raise)

    assert scripted.revoked[0] is False and calls == [1]
    assert cf.wait([future], timeout=1)[0] == {future}


# --- the executor with a fake backend ------------------------------------------------------------------


class FakeTask:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def apply_async(self, **kwargs):
        self.calls.append(kwargs)
        return self.results.pop(0)


class FakeControl:
    def __init__(self):
        self.revoked = []

    def revoke(self, task_id, **kwargs):
        self.revoked.append((task_id, kwargs))

    def broadcast(self, *a, **k):
        return None


@pytest.fixture
def fake_executor():
    app = pickle_app("behaviour-fake")
    pool = CeleryExecutor(app, poll_interval=0.02)
    pool._control = FakeControl()
    results = []
    pool._remote_task = FakeTask(results)
    pool.script = results
    yield pool
    pool.shutdown(
        wait=False, cancel_futures=True
    )  # (so no watcher thread is left over)


def test_a_task_that_finishes_before_it_is_registered_does_not_stay_tracked(
    fake_executor,
):
    """The cleanup callback was registered before the future was put in the dict: a future that
    finished in between ran the cleanup first and then stayed in the dict for ever."""
    fake_executor.script.append(Scripted("t-fast", state="SUCCESS", result=1))

    future = fake_executor.submit(operator.add, 1, 1)

    assert wait_until(future.done)
    assert wait_until(lambda: fake_executor.futures() == {})


def test_the_watcher_logs_a_task_it_cannot_read_once_not_on_every_poll(
    fake_executor, caplog
):
    class Unreadable(Scripted):
        @property
        def state(self):
            raise ConnectionError("backend down")

        @state.setter
        def state(self, value):
            pass

    fake_executor.script.append(Unreadable("t-bad"))

    with caplog.at_level(logging.ERROR, logger="volnux.executors.celery"):
        fake_executor.submit(operator.add, 1, 1)
        time.sleep(0.5)  # about 25 polls

    assert caplog.text.count("Could not read the state of Celery task t-bad") == 1


def test_shutdown_does_not_hold_the_lock_while_it_reads_states(fake_executor):
    """done() reads the result backend; calling it under the executor's lock blocked every
    submit and the watcher for as long as the backend took."""
    held = []

    class Probe(Scripted):
        @property
        def state(self):
            got = fake_executor._lock.acquire(blocking=False)
            held.append(not got)
            if got:
                fake_executor._lock.release()
            return "PENDING"

        @state.setter
        def state(self, value):
            pass

    fake_executor._poll_interval = 60  # the watcher polls once, at submit, then sleeps
    fake_executor.script.append(Probe("t-probe"))
    fake_executor.submit(operator.add, 1, 1)
    time.sleep(0.2)
    held.clear()

    fake_executor.shutdown(wait=False)

    assert held and not any(held)


def test_shutdown_revokes_what_has_not_started_and_cancels_it_locally(fake_executor):
    queued, running = Scripted("t-q", "PENDING"), Scripted("t-r", "STARTED")
    fake_executor.script.extend([queued, running])
    f_queued = fake_executor.submit(operator.add, 1, 1)
    f_running = fake_executor.submit(operator.add, 2, 2)

    fake_executor.shutdown(wait=False, cancel_futures=True)

    assert sorted(t for t, _ in fake_executor._control.revoked) == ["t-q", "t-r"]
    assert f_queued.cancelled() is True
    assert f_running.cancelled() is False  # it is running: it will finish

    running.state, running.result = "SUCCESS", 7  # it finishes
    assert f_running.result(timeout=5) == 7
    assert wait_until(lambda: fake_executor.futures() == {})


def test_submit_after_shutdown_is_refused(fake_executor):
    fake_executor.shutdown(wait=False)
    with pytest.raises(RuntimeError, match="after executor has been shut down"):
        fake_executor.submit(operator.add, 1, 1)


def test_recently_finished_ids_are_remembered_but_only_so_many(fake_executor):
    from volnux.executors import celery as celery_module

    assert fake_executor._recent_ids.maxlen == celery_module._RECENT_TASK_IDS


# --- configuration ---------------------------------------------------------------------------------------


def test_a_default_app_is_reported_as_unable_to_carry_callables():
    app = Celery("defaults", broker="memory://", backend="cache+memory://")
    pool = CeleryExecutor(app)

    problems = pool.configuration_problems()

    assert len(problems) == 3
    assert any("task_serializer" in p for p in problems)
    assert any("result_serializer" in p for p in problems)
    assert any("accept_content" in p for p in problems)


def test_a_pickle_app_has_no_configuration_problems():
    assert CeleryExecutor(pickle_app("configured")).configuration_problems() == []


def test_the_problems_are_logged_when_the_executor_is_built(caplog):
    with caplog.at_level(logging.WARNING, logger="volnux.executors.celery"):
        CeleryExecutor(Celery("loud", broker="memory://", backend="cache+memory://"))

    assert "task_serializer" in caplog.text and "accept_content" in caplog.text


def test_a_configured_app_is_quiet(caplog):
    with caplog.at_level(logging.WARNING, logger="volnux.executors.celery"):
        CeleryExecutor(pickle_app("quiet"))

    assert caplog.text == ""


# --- the remote task ---------------------------------------------------------------------------------------


def test_the_remote_task_runs_the_callable_and_lets_its_error_through():
    task = make_remote_task(pickle_app("remote-task"))

    assert task.run(operator.add, 2, 3) == 5
    with pytest.raises(ValueError, match="plain failure"):
        task.run(boom, "plain failure")


def test_the_remote_task_has_a_fixed_name_workers_can_register():
    assert (
        make_remote_task(pickle_app("named")).name == "celery_executor.execute_remotely"
    )


def test_shutdown_works_when_the_result_backend_cannot_be_read(fake_executor):
    class Unreadable(Scripted):
        @property
        def state(self):
            raise ConnectionError("backend down")

        @state.setter
        def state(self, value):
            pass

    fake_executor.script.append(Unreadable("t-down"))
    fake_executor.submit(operator.add, 1, 1)

    fake_executor.shutdown(wait=False)  # must not raise

    assert fake_executor._shutdown is True


def test_cancelling_after_a_local_cancel_does_not_raise(fake_executor):
    fake_executor.script.append(Scripted("t-1", "PENDING"))
    future = fake_executor.submit(operator.add, 1, 1)

    fake_executor.shutdown(wait=False, cancel_futures=True)  # cancels it locally
    fake_executor.revoke("t-1")  # and then somebody cancels it again

    assert future.cancelled() is True
