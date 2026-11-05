"""`{}` cooperative groups and `|->` stream segments through the whole stack."""

import asyncio

import pytest

from tests.integration.support import Trace, make_event, results_of, run, step


def started(trace):
    return [name for kind, name in trace.log if kind == "start"]


@pytest.mark.asyncio
async def test_the_chains_of_a_group_run_at_the_same_time():
    trace = Trace()
    a = make_event("GA", step(trace, "a", delay=0.15))
    b = make_event("GB", step(trace, "b", delay=0.15))
    c = make_event("GC", step(trace, "c", delay=0.05))
    after = make_event("GAfter", step(trace, "after"))

    _, error = await run(
        f"{{{a.__name__}, {b.__name__} -> {c.__name__}}} -> {after.__name__}"
    )

    assert error is None
    log = trace.log
    assert log.index(("start", "b")) < log.index(("end", "a"))  # overlapped
    assert log.index(("start", "c")) > log.index(("end", "b"))  # a chain is in order
    assert log.index(("start", "after")) > max(log.index(("end", n)) for n in "ac")


@pytest.mark.asyncio
async def test_max_concurrency_limits_how_many_chains_run_at_once():
    running = peak = 0

    async def process(self, *args, **kwargs):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.05)
        running -= 1
        return True, "x"

    names = [make_event(f"M{i}", process).__name__ for i in range(6)]

    _, error = await run("{" + ", ".join(names) + "}[max_concurrency = 2]")

    assert error is None and peak == 2


@pytest.mark.asyncio
async def test_a_group_is_retried_as_a_unit_while_a_chain_fails():
    trace = Trace()
    attempts = []

    async def flaky(self, *args, **kwargs):
        attempts.append(1)
        return len(attempts) > 1, "flaky"

    a = make_event("Flaky", flaky)
    b = make_event("Steady", step(trace, "steady"))

    _, error = await run(f"{{{a.__name__}, {b.__name__}}}[retry_attempts = 3]")

    assert error is None
    assert len(attempts) == 2  # failed once, then succeeded: no third attempt
    assert trace.count(("start", "steady")) == 2  # the whole group ran again


@pytest.mark.asyncio
async def test_a_group_that_keeps_failing_takes_its_failure_branch():
    trace = Trace()
    bad = make_event("Bad", step(trace, "bad", ok=False))
    good = make_event("Good", step(trace, "good"))
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))

    _, error = await run(
        f"{{{bad.__name__}, {good.__name__}}} (failure -> {no.__name__}, success -> {yes.__name__})"
    )

    assert error is None
    assert "no" in started(trace) and "yes" not in started(trace)


# --- streams -------------------------------------------------------------------


def numbers(count):
    async def process(self, *args, **kwargs):
        for i in range(count):
            yield True, i

    return process


def collector(seen):
    async def process(self, *args, **kwargs):
        async for chunk in self.input_stream():
            seen.append(chunk.content)
        return True, "collected"

    return process


@pytest.mark.asyncio
async def test_a_stream_segment_passes_items_through_a_transform():
    seen = []

    async def double(self, *args, **kwargs):
        async for chunk in self.input_stream():
            yield True, chunk.content * 2

    source = make_event("Src", numbers(5))
    stage = make_event("Stage", double)
    sink = make_event("Sink", collector(seen))

    _, error = await run(f"{source.__name__} |-> {stage.__name__} |-> {sink.__name__}")

    assert error is None and seen == [0, 2, 4, 6, 8]


@pytest.mark.asyncio
async def test_fan_out_gives_every_consumer_every_item():
    one, two = [], []
    source = make_event("Src", numbers(3))
    first = make_event("First", collector(one))
    second = make_event("Second", collector(two))

    _, error = await run(
        f"{source.__name__} |-> ({first.__name__} || {second.__name__})"
    )

    assert error is None and one == [0, 1, 2] and two == [0, 1, 2]


@pytest.mark.asyncio
async def test_a_failing_producer_takes_the_consumers_failure_branch():
    trace = Trace()

    async def broken(self, *args, **kwargs):
        yield True, 1
        yield False, "producer broke"

    source = make_event("Src", broken)
    sink = make_event("Sink", collector([]))
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))

    _, error = await run(
        f"{source.__name__} |-> {sink.__name__} (failure -> {no.__name__}, success -> {yes.__name__})"
    )

    assert error is None and started(trace) == ["no"]


@pytest.mark.asyncio
async def test_a_group_can_feed_a_stream_consumer():
    seen = []
    one = make_event("One", numbers(2))
    two = make_event("Two", numbers(2))
    sink = make_event("Sink", collector(seen))

    _, error = await run(f"{{{one.__name__}, {two.__name__}}} |-> {sink.__name__}")

    assert error is None and sorted(seen) == [0, 0, 1, 1]
