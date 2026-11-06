"""Branches and descriptors: how the engine picks the next task from real results."""

import pytest

from tests.integration.support import Trace, contents, make_event, results_of, run, step


def ran(trace):
    return [name for kind, name in trace.log if kind == "start"]


@pytest.mark.asyncio
async def test_a_successful_event_takes_the_success_branch():
    trace = Trace()
    a = make_event("A", step(trace, "a", ok=True))
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))

    _, error = await run(
        f"{a.__name__} (failure -> {no.__name__}, success -> {yes.__name__})"
    )

    assert error is None and ran(trace) == ["a", "yes"]


@pytest.mark.asyncio
async def test_a_failing_event_takes_the_failure_branch():
    trace = Trace()
    a = make_event("A", step(trace, "a", ok=False))
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))

    _, error = await run(
        f"{a.__name__} (failure -> {no.__name__}, success -> {yes.__name__})"
    )

    assert error is None and ran(trace) == ["a", "no"]


@pytest.mark.asyncio
async def test_an_event_that_raises_takes_the_failure_branch():
    trace = Trace()

    async def boom(self, *args, **kwargs):
        raise ValueError("bad")

    a = make_event("A", boom, max_attempts=1) if False else make_event("A", boom)
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))

    _, error = await run(
        f"{a.__name__} (failure -> {no.__name__}, success -> {yes.__name__})"
    )

    assert error is None and ran(trace) == ["no"]


@pytest.mark.asyncio
async def test_a_lone_failure_branch_runs_only_when_the_event_fails():
    """`A (failure -> X)` used to run X never: with one branch the task was not
    conditional, so the failure was not looked at."""
    ran_when = {}
    for ok in (True, False):
        trace = Trace()
        a = make_event("A", step(trace, "a", ok=ok))
        x = make_event("X", step(trace, "x"))

        _, error = await run(f"{a.__name__} (failure -> {x.__name__})")

        assert error is None
        ran_when[ok] = ran(trace)

    assert ran_when == {True: ["a"], False: ["a", "x"]}


@pytest.mark.asyncio
async def test_a_plain_arrow_continues_after_a_failed_event():
    """`->` is sequence, not a condition: the failure is a result; use a branch to react."""
    trace = Trace()
    a = make_event("A", step(trace, "a", ok=False))
    b = make_event("B", step(trace, "b"))

    pipeline, error = await run(f"{a.__name__} -> {b.__name__}")

    assert error is None and ran(trace) == ["a", "b"]
    assert [r.error for r in results_of(pipeline)] == [True, False]


@pytest.mark.asyncio
async def test_branch_chains_continue_after_the_branch():
    trace = Trace()
    a = make_event("A", step(trace, "a"))
    y1 = make_event("Y1", step(trace, "y1"))
    y2 = make_event("Y2", step(trace, "y2"))
    n = make_event("N", step(trace, "n"))

    _, error = await run(
        f"{a.__name__} (failure -> {n.__name__}, success -> {y1.__name__} -> {y2.__name__})"
    )

    assert error is None and ran(trace) == ["a", "y1", "y2"]


@pytest.mark.asyncio
async def test_goto_switches_to_the_chosen_descriptor():
    trace = Trace()

    async def chooser(self, *args, **kwargs):
        trace(("start", "chooser"))
        self.goto(descriptor=2, result_success=True, result="picked two")

    a = make_event("A", chooser)
    zero = make_event("Zero", step(trace, "zero"))
    one = make_event("One", step(trace, "one"))
    two = make_event("Two", step(trace, "two"))

    _, error = await run(
        f"{a.__name__} (0 -> {zero.__name__}, 1 -> {one.__name__}, 2 -> {two.__name__})"
    )

    assert error is None and ran(trace) == ["chooser", "two"]


@pytest.mark.asyncio
async def test_a_switch_to_a_descriptor_that_is_not_configured_is_ignored():
    trace = Trace()

    async def chooser(self, *args, **kwargs):
        trace(("start", "chooser"))
        self.goto(descriptor=5, result_success=True, result="nowhere")

    a = make_event("A", chooser)
    zero = make_event("Zero", step(trace, "zero"))
    one = make_event("One", step(trace, "one"))

    _, error = await run(f"{a.__name__} (0 -> {zero.__name__}, 1 -> {one.__name__})")

    # the switch is dropped (logged), and neither branch is taken
    assert error is None and ran(trace) == ["chooser"]
