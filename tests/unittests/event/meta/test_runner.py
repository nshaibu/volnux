"""The bounded concurrent pipeline under the meta events."""

import asyncio

import pytest

from volnux.event.meta.runner import run_concurrently
from volnux.exceptions import SwitchTask


async def counting(n, log):
    for i in range(n):
        log.append(i)
        yield i


@pytest.mark.asyncio
async def test_the_input_is_read_only_as_fast_as_results_are_consumed():
    """Backpressure: a long input is never read far ahead of the consumer."""
    read = []
    consumed = 0
    worst_lead = 0

    async def call(x):
        await asyncio.sleep(0)
        return x

    async for outcome in run_concurrently(
        counting(500, read), call, workers=2, buffer=2
    ):
        consumed += 1
        worst_lead = max(worst_lead, len(read) - consumed)
        await asyncio.sleep(0.001)  # a slow consumer

    assert consumed == 500
    assert worst_lead <= 2 + 2 + 1  # workers + buffer (+1 being read)


@pytest.mark.asyncio
async def test_ordered_results_hold_at_most_the_window():
    held_peak = 0
    finished = 0

    async def call(x):
        nonlocal finished
        await asyncio.sleep(
            0.05 if x == 0 else 0
        )  # item 0 is slow; the rest wait behind it
        finished += 1
        return x

    seen = []
    async for outcome in run_concurrently(counting(100, []), call, workers=4, buffer=4):
        held_peak = max(held_peak, finished - len(seen))
        seen.append(outcome.value)

    assert seen == list(range(100))
    assert held_peak <= 8  # never more than workers + buffer waiting


@pytest.mark.asyncio
async def test_a_failed_call_is_an_outcome_not_an_exception():
    async def call(x):
        raise ValueError(x)

    outcomes = [o async for o in run_concurrently(counting(2, []), call, workers=1)]

    assert [o.ok for o in outcomes] == [False, False]
    assert isinstance(outcomes[0].value, ValueError) and outcomes[1].item == 1


@pytest.mark.asyncio
async def test_control_flow_exceptions_are_raised_to_the_caller():
    async def call(x):
        raise SwitchTask("t", 1, None)

    with pytest.raises(SwitchTask):
        async for _ in run_concurrently(counting(1, []), call, workers=1):
            pass


@pytest.mark.asyncio
async def test_an_error_in_the_input_is_raised_and_workers_are_cancelled():
    cancelled = []

    async def bad_input():
        yield 1
        yield 2
        raise RuntimeError("input broke")

    async def call(x):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled.append(x)
            raise

    with pytest.raises(RuntimeError, match="input broke"):
        async for _ in run_concurrently(bad_input(), call, workers=4):
            pass

    assert sorted(cancelled) == [1, 2]


@pytest.mark.asyncio
async def test_stopping_early_cancels_what_is_running():
    cancelled = []

    async def call(x):
        try:
            await asyncio.sleep(0 if x == 0 else 5)
        except asyncio.CancelledError:
            cancelled.append(x)
            raise
        return x

    gen = run_concurrently(counting(10, []), call, workers=3)
    first = await gen.__anext__()
    await gen.aclose()

    assert first.value == 0 and cancelled  # the slow ones were cancelled, not leaked


@pytest.mark.asyncio
async def test_no_input_no_outcomes():
    async def call(x):
        return x

    assert [o async for o in run_concurrently(counting(0, []), call, workers=2)] == []
