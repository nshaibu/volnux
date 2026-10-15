"""Tests for the background-thread publisher.

The sink is injected, so delivery is observed without a provisioned messaging
backend. What matters here is the threading contract: ``submit`` never blocks or
raises on the engine's hot path, buffered events still drain on stop, and no
single failure kills the delivery thread.
"""

import asyncio
import time

from volnux.governance.events import EventType
from volnux.governance.publisher import GovernanceEventPublisher


def drained(publisher, timeout=5.0):
    """Stop the publisher and wait for its thread to finish."""
    publisher.stop(timeout=timeout)


# --- Delivery ---------------------------------------------------------------


def test_events_are_delivered_in_order(make_event):
    seen = []

    async def sink(event):
        seen.append(event.event_type)

    publisher = GovernanceEventPublisher(sink=sink)
    publisher.start()
    for i in range(5):
        publisher.submit(make_event(event_type=f"task.step-{i}"))
    drained(publisher)

    assert seen == [f"task.step-{i}" for i in range(5)]


def test_one_event_loop_serves_the_whole_thread(make_event):
    # A loop per event would stop the messaging backend reusing its
    # (loop-bound) connections, so this is a real requirement, not a detail.
    loops = []

    async def sink(event):
        loops.append(id(asyncio.get_running_loop()))

    publisher = GovernanceEventPublisher(sink=sink)
    publisher.start()
    for _ in range(5):
        publisher.submit(make_event())
    drained(publisher)

    assert len(loops) == 5
    assert len(set(loops)) == 1


def test_buffered_events_are_flushed_on_stop(make_event):
    seen = []

    async def slow_sink(event):
        await asyncio.sleep(0.01)
        seen.append(event.event_type)

    publisher = GovernanceEventPublisher(sink=slow_sink)
    publisher.start()
    for _ in range(10):
        publisher.submit(make_event())
    drained(publisher, timeout=10)

    assert len(seen) == 10


# --- Reservoir flushing -----------------------------------------------------


def test_flush_source_is_drained_periodically(make_event):
    pending = [[make_event(event_type="task.started")]]
    seen = []

    async def sink(event):
        seen.append(event.event_type)

    publisher = GovernanceEventPublisher(
        sink=sink, flush_source=lambda: pending.pop(0) if pending else [],
        flush_interval=0.05,
    )
    publisher.start()
    time.sleep(0.3)
    drained(publisher)

    assert seen == ["task.started"]


# --- Failure isolation ------------------------------------------------------


def test_a_failing_sink_does_not_kill_the_thread(make_event):
    seen = []

    async def flaky(event):
        if event.event_type == "boom":
            raise RuntimeError("transport exploded")
        seen.append(event.event_type)

    publisher = GovernanceEventPublisher(sink=flaky)
    publisher.start()
    publisher.submit(make_event(event_type="ok-1"))
    publisher.submit(make_event(event_type="boom"))
    publisher.submit(make_event(event_type="ok-2"))
    drained(publisher)

    assert seen == ["ok-1", "ok-2"]


def test_a_failing_flush_source_does_not_kill_the_thread(make_event):
    seen = []

    async def sink(event):
        seen.append(event.event_type)

    def bad_flush():
        raise ValueError("reservoir broken")

    publisher = GovernanceEventPublisher(
        sink=sink, flush_source=bad_flush, flush_interval=0.05
    )
    publisher.start()
    time.sleep(0.15)
    publisher.submit(make_event(event_type=EventType.EXECUTION_COMPLETED))
    drained(publisher)

    assert seen == [EventType.EXECUTION_COMPLETED]


# --- Hot-path guarantees ----------------------------------------------------


def test_submit_drops_rather_than_blocking_when_full(make_event):
    # Never started, so nothing drains: the buffer fills and submit must still
    # return immediately rather than back-pressure a running workflow.
    publisher = GovernanceEventPublisher(sink=None, max_buffer=2)

    for _ in range(50):
        publisher.submit(make_event())

    assert publisher.started is False


def test_start_and_stop_are_idempotent():
    async def sink(event):
        pass

    publisher = GovernanceEventPublisher(sink=sink)
    publisher.start()
    publisher.start()
    assert publisher.started is True

    publisher.stop()
    publisher.stop()
    assert publisher.started is False


def test_stopping_a_publisher_that_never_started_is_a_noop():
    publisher = GovernanceEventPublisher(sink=None)

    publisher.stop()

    assert publisher.started is False
