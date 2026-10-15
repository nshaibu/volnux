"""Tests for reservoir sampling of governance telemetry.

Pure and deterministic: the RNG is injected, so the sampling decisions are
reproducible and no backend is involved.
"""

import random

from volnux.governance.events import EventType
from volnux.governance.sampling import EventSampler, ReservoirSampler


# --- Algorithm R ------------------------------------------------------------


def test_reservoir_never_exceeds_capacity():
    reservoir = ReservoirSampler(capacity=4, rng=random.Random(0))

    for i in range(100):
        reservoir.offer(i)

    assert len(reservoir) == 4
    assert reservoir.seen == 100


def test_reservoir_keeps_everything_below_capacity():
    reservoir = ReservoirSampler(capacity=10, rng=random.Random(0))

    for i in range(3):
        reservoir.offer(i)

    assert reservoir.drain() == [0, 1, 2]


def test_drain_resets_the_window():
    reservoir = ReservoirSampler(capacity=4, rng=random.Random(0))
    for i in range(10):
        reservoir.offer(i)

    reservoir.drain()

    assert len(reservoir) == 0
    assert reservoir.seen == 0


def test_capacity_must_be_positive():
    try:
        ReservoirSampler(capacity=0)
    except ValueError:
        return
    raise AssertionError("expected ValueError for a zero-capacity reservoir")


# --- Classification ---------------------------------------------------------


def test_state_critical_events_pass_straight_through(make_event):
    sampler = EventSampler(capacity=4, rng=random.Random(0))
    event = make_event(event_type=EventType.HITL_REQUESTED)

    assert sampler.offer(event) == [event]


def test_telemetry_is_held_back_for_the_flush(make_event):
    sampler = EventSampler(capacity=4, rng=random.Random(0))

    assert sampler.offer(make_event(event_type=EventType.TASK_STARTED)) == []


def test_drain_returns_the_sampled_telemetry(make_event):
    sampler = EventSampler(capacity=4, rng=random.Random(0))

    for _ in range(20):
        sampler.offer(make_event(event_type=EventType.TASK_STARTED))

    drained = sampler.drain()
    assert len(drained) == 4
    assert all(event.event_type == EventType.TASK_STARTED for event in drained)
    assert sampler.drain() == []  # the window reset


def test_a_terminal_event_flushes_its_run_first(make_event):
    # The run's sampled progress must arrive just before its ending, so the
    # projector never records a completed run with no trace of what it did.
    sampler = EventSampler(capacity=4, rng=random.Random(0))

    for _ in range(20):
        sampler.offer(make_event(event_type=EventType.TASK_STARTED))

    delivered = sampler.offer(make_event(event_type=EventType.EXECUTION_COMPLETED))

    assert len(delivered) == 5
    assert delivered[-1].event_type == EventType.EXECUTION_COMPLETED
    assert all(e.event_type == EventType.TASK_STARTED for e in delivered[:-1])


# --- Keying -----------------------------------------------------------------


def test_each_run_gets_its_own_fair_sample(make_event):
    # One busy run must not crowd another out of the sample.
    sampler = EventSampler(capacity=2, rng=random.Random(0))

    for _ in range(20):
        sampler.offer(make_event(event_type=EventType.TASK_STARTED, execution_id="a"))
    for _ in range(20):
        sampler.offer(make_event(event_type=EventType.TASK_STARTED, execution_id="b"))

    drained = sampler.drain()
    by_run = {event.execution_id for event in drained}
    assert len(drained) == 4
    assert by_run == {"a", "b"}


def test_uncorrelated_telemetry_is_keyed_by_node(make_event):
    # A heartbeat has no execution, so the node in its payload is the key.
    sampler = EventSampler(capacity=2, rng=random.Random(0))

    for node in ("node-a", "node-b"):
        for _ in range(10):
            sampler.offer(
                make_event(
                    event_type=EventType.NODE_HEARTBEAT,
                    execution_id=None,
                    payload={"node_id": node},
                )
            )

    drained = sampler.drain()
    assert len(drained) == 4
    assert {e.payload["node_id"] for e in drained} == {"node-a", "node-b"}
