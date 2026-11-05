"""`A -> B -> C`: the chain from the engine down to the executor and back up."""

import pytest

from tests.integration.support import (
    Trace,
    contents,
    contexts,
    make_event,
    results_of,
    run,
    step,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", ["default", "thread"])
async def test_events_run_in_order_and_each_leaves_a_result(executor):
    trace = Trace()
    a = make_event("A", step(trace, "a"), executor=executor)
    b = make_event("B", step(trace, "b"), executor=executor)
    c = make_event("C", step(trace, "c"), executor=executor)

    pipeline, error = await run(f"{a.__name__} -> {b.__name__} -> {c.__name__}")

    assert error is None
    assert [e for kind, e in trace.log if kind == "start"] == ["a", "b", "c"]
    assert [r.content for r in results_of(pipeline)] == ["a", "b", "c"]
    assert contents(pipeline) == ["c"]


@pytest.mark.asyncio
async def test_each_task_gets_a_context_linked_both_ways():
    trace = Trace()
    names = [make_event(n, step(trace, n)).__name__ for n in "ABC"]

    pipeline, error = await run(" -> ".join(names))

    assert error is None
    chain = contexts(pipeline)
    assert len(chain) == 3
    for before, after in zip(chain, chain[1:]):
        assert before.next_context is after and after.previous_context is before
    assert pipeline.execution_context is chain[0]
    assert pipeline.execution_context.get_latest_context() is chain[-1]


@pytest.mark.asyncio
async def test_the_next_event_receives_the_previous_result():
    seen = []

    async def first(self, *args, **kwargs):
        return True, {"n": 41}

    async def second(self, *args, **kwargs):
        seen.append([r.content for r in self.previous_result])
        return True, self.previous_result[0].content["n"] + 1

    a, b = make_event("A", first), make_event("B", second)

    pipeline, error = await run(f"{a.__name__} -> {b.__name__}")

    assert error is None
    assert seen == [[{"n": 41}]]
    assert contents(pipeline) == [42]


@pytest.mark.asyncio
async def test_a_single_task_workflow_completes():
    trace = Trace()
    a = make_event("Solo", step(trace, "solo"))

    pipeline, error = await run(a.__name__)

    assert error is None and contents(pipeline) == ["solo"]


@pytest.mark.asyncio
async def test_two_pipelines_run_concurrently_without_disturbing_each_other():
    """They used to share one engine, whose queues and counters are per run."""
    import asyncio

    trace_a, trace_b = Trace(), Trace()
    first = [
        make_event(f"X{i}", step(trace_a, f"a{i}", delay=0.02)).__name__
        for i in range(4)
    ]
    second = [
        make_event(f"Y{i}", step(trace_b, f"b{i}", delay=0.02)).__name__
        for i in range(4)
    ]

    (one, err_one), (two, err_two) = await asyncio.gather(
        run(" -> ".join(first)), run(" -> ".join(second))
    )

    assert err_one is None and err_two is None
    assert [r.content for r in results_of(one)] == ["a0", "a1", "a2", "a3"]
    assert [r.content for r in results_of(two)] == ["b0", "b1", "b2", "b3"]
