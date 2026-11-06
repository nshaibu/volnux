"""FILTER, FLATMAP, FOREACH, REDUCE and FANOUT."""

import asyncio
import itertools
from unittest.mock import AsyncMock

import pytest

from volnux.event.meta import (
    FanoutEvent,
    FilterEvent,
    FlatMapEvent,
    ForeachEvent,
    ReduceEvent,
)
from volnux.event.meta.flat_map import flatten


def is_even(x):
    return x % 2 == 0


# --- FILTER --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_keeps_the_original_items_the_predicate_accepts(run_meta):
    items, _, _ = await run_meta(FilterEvent, is_even, collection=[1, 2, 3, 4])
    assert items == [2, 4]


@pytest.mark.asyncio
async def test_filter_invert_keeps_the_rejected_ones(run_meta):
    items, _, _ = await run_meta(
        FilterEvent, is_even, collection=[1, 2, 3, 4], invert=True
    )
    assert items == [1, 3]


@pytest.mark.asyncio
async def test_filter_keeps_objects_not_predicate_results(run_meta):
    rows = [{"n": 1}, {"n": 5}, {"n": 9}]
    items, _, _ = await run_meta(FilterEvent, lambda r: r["n"] > 3, collection=rows)
    assert items == [{"n": 5}, {"n": 9}]


@pytest.mark.asyncio
async def test_filter_a_failing_predicate_fails_the_stream_unless_keep_on_error(
    run_meta,
):
    def picky(x):
        if x == 2:
            raise ValueError("cannot tell")
        return True

    _, _, strict = await run_meta(FilterEvent, picky, collection=[1, 2, 3])
    items, _, lenient = await run_meta(
        FilterEvent, picky, collection=[1, 2, 3], keep_on_error=True
    )

    assert not strict.success
    assert lenient.success and items == [1, 2, 3]


@pytest.mark.asyncio
async def test_filter_batches_pair_each_item_with_its_flag(run_meta):
    items, _, _ = await run_meta(
        FilterEvent,
        lambda batch: [is_even(x) for x in batch],
        collection=[1, 2, 3, 4, 5],
        batch_size=2,
    )
    assert items == [2, 4]


@pytest.mark.asyncio
async def test_filter_a_batch_with_the_wrong_number_of_flags_is_an_error(run_meta):
    _, _, result = await run_meta(
        FilterEvent, lambda batch: [True], collection=[1, 2, 3], batch_size=3
    )
    assert not result.success


# --- FLATMAP -----------------------------------------------------------------------


def test_flatten_opens_the_requested_depth_and_keeps_strings_whole():
    assert list(flatten([1, [2, 3]], 1)) == [1, [2, 3]]
    assert list(flatten([1, [2, [3, 4]]], 2)) == [1, 2, [3, 4]]
    assert list(flatten([1, [2, [3, [4]]]], -1)) == [1, 2, 3, 4]
    assert list(flatten(["ab", ("c",)], -1)) == ["ab", "c"]
    assert list(flatten("abc", 1)) == ["abc"]
    assert list(flatten({"a": 1}, 1)) == [{"a": 1}]
    assert list(flatten(5, 1)) == [5]


@pytest.mark.asyncio
async def test_flatmap_emits_the_elements_of_each_result_in_order_with_duplicates(
    run_meta,
):
    items, _, _ = await run_meta(FlatMapEvent, lambda x: [x, x], collection=[1, 2, 1])
    assert items == [1, 1, 2, 2, 1, 1]


@pytest.mark.asyncio
async def test_flatmap_depth(run_meta):
    items, _, _ = await run_meta(
        FlatMapEvent, lambda x: [[x, [x]]], collection=[1], flatten_depth=-1
    )
    assert items == [1, 1]


@pytest.mark.asyncio
async def test_flatmap_skip_errors(run_meta):
    def explode(x):
        if x == 2:
            raise ValueError
        return [x]

    items, _, ok = await run_meta(
        FlatMapEvent, explode, collection=[1, 2, 3], skip_errors=True
    )
    _, _, strict = await run_meta(FlatMapEvent, explode, collection=[1, 2, 3])

    assert items == [1, 3] and ok.success and not strict.success


# --- FOREACH -----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_foreach_runs_the_template_and_passes_the_items_through(run_meta):
    seen = []
    items, _, _ = await run_meta(
        ForeachEvent, lambda x: seen.append(x) or "ignored", collection=[3, 1, 2]
    )

    assert items == [3, 1, 2] and sorted(seen) == [1, 2, 3]


@pytest.mark.asyncio
async def test_foreach_carries_on_after_a_failure_by_default(run_meta):
    def flaky(x):
        raise RuntimeError

    items, _, result = await run_meta(ForeachEvent, flaky, collection=[1, 2])
    _, _, strict = await run_meta(
        ForeachEvent, flaky, collection=[1, 2], continue_on_error=False
    )

    assert items == [1, 2] and result.success and not strict.success


# --- REDUCE ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reduce_folds_with_the_accumulator(run_meta):
    items, _, _ = await run_meta(
        ReduceEvent, lambda acc, x: acc + x, collection=[1, 2, 3, 4], initial_value=10
    )
    assert items == [20]


@pytest.mark.asyncio
async def test_reduce_is_a_real_fold_not_the_last_result(run_meta):
    items, _, _ = await run_meta(
        ReduceEvent,
        lambda acc, x: acc + [x],
        collection=["a", "b", "c"],
        initial_value=[],
    )
    assert items == [["a", "b", "c"]]


@pytest.mark.asyncio
async def test_reduce_direction_right_folds_from_the_end(run_meta):
    items, _, _ = await run_meta(
        ReduceEvent,
        lambda acc, x: acc + x,
        collection=["a", "b", "c"],
        initial_value="",
        direction="right",
    )
    assert items == ["cba"]


@pytest.mark.asyncio
async def test_reduce_of_nothing_is_the_initial_value(run_meta):
    items, _, _ = await run_meta(
        ReduceEvent, lambda a, x: a + x, collection=[], initial_value=7
    )
    assert items == [7]


@pytest.mark.asyncio
async def test_reduce_with_an_event_class_gets_accumulator_and_item(run_meta):
    from ..conftest import make_event_class

    async def process(self, *a, **k):
        acc, item = (r.content for r in self.previous_result)
        return True, acc * item

    items, _, _ = await run_meta(
        ReduceEvent,
        make_event_class(process=process),
        collection=[2, 3, 4],
        initial_value=1,
    )
    assert items == [24]


@pytest.mark.asyncio
async def test_reduce_skip_errors_and_failure(run_meta):
    def step(acc, x):
        if x == 2:
            raise ValueError
        return acc + x

    items, _, ok = await run_meta(
        ReduceEvent, step, collection=[1, 2, 3], initial_value=0, skip_errors=True
    )
    _, _, bad = await run_meta(ReduceEvent, step, collection=[1, 2, 3], initial_value=0)

    assert items == [4] and ok.success and not bad.success


@pytest.mark.asyncio
async def test_reduce_resumes_from_a_restored_accumulator_and_checkpoints_it(run_meta):
    saved = []

    def configure(event):
        event._reduce_acc, event._reduce_started = (
            100,
            True,
        )  # as a checkpoint restore sets them
        event.stream_policy = type(event.stream_policy)(checkpoint_every=2)

        async def checkpoint():
            saved.append(event._reduce_acc)

        event.enqueue_checkpoint = checkpoint

    items, _, _ = await run_meta(
        ReduceEvent,
        lambda a, x: a + x,
        collection=[1, 2, 3, 4],
        initial_value=0,
        configure=configure,
    )

    assert items == [110]  # started from 100, not from initial_value
    assert 103 in saved and 110 in saved  # the accumulator is saved with the position


@pytest.mark.asyncio
async def test_reduce_requires_an_initial_value(run_meta):
    _, _, result = await run_meta(ReduceEvent, lambda a, x: a, collection=[1])
    assert not result.success


# --- FANOUT ------------------------------------------------------------------------


def scripted(*answers):
    """A template that answers from a script, in call order."""
    it = itertools.count()

    async def ask(item):
        n = next(it)
        answer = answers[n % len(answers)]
        if isinstance(answer, Exception):
            raise answer
        return answer

    return ask


@pytest.mark.asyncio
async def test_fanout_all_collects_every_answer(run_meta):
    items, _, _ = await run_meta(
        FanoutEvent,
        lambda x: x + 1,
        collection=[1, 10],
        count=3,
        concurrency_mode="async",
    )
    assert items == [[2, 2, 2], [11, 11, 11]]


@pytest.mark.asyncio
async def test_fanout_majority(run_meta):
    items, _, result = await run_meta(
        FanoutEvent,
        scripted("a", "a", "b"),
        collection=[0],
        count=3,
        aggregation_strategy="majority",
        max_workers=1,
    )
    assert items == ["a"] and result.success


@pytest.mark.asyncio
async def test_fanout_no_majority_fails(run_meta):
    _, _, result = await run_meta(
        FanoutEvent,
        scripted("a", "b", "c"),
        collection=[0],
        count=3,
        aggregation_strategy="majority",
        max_workers=1,
    )
    assert not result.success


@pytest.mark.asyncio
async def test_fanout_consensus(run_meta):
    same, _, ok = await run_meta(
        FanoutEvent,
        scripted({"k": 1}),
        collection=[0],
        count=3,
        aggregation_strategy="consensus",
    )
    _, _, split = await run_meta(
        FanoutEvent,
        scripted(1, 2),
        collection=[0],
        count=2,
        aggregation_strategy="consensus",
        max_workers=1,
    )
    assert same == [{"k": 1}] and ok.success and not split.success


@pytest.mark.asyncio
async def test_fanout_first_is_the_first_replica_not_the_first_to_finish(run_meta):
    calls = itertools.count()

    async def ask(item):
        n = next(calls)
        await asyncio.sleep(0.05 if n == 0 else 0)
        return n

    items, _, _ = await run_meta(
        FanoutEvent,
        ask,
        collection=[0],
        count=3,
        aggregation_strategy="first",
        concurrency_mode="async",
    )
    assert items == [0]


@pytest.mark.asyncio
async def test_fanout_fastest_takes_the_first_answer_and_cancels_the_rest(run_meta):
    calls = itertools.count()
    cancelled = []

    async def ask(item):
        n = next(calls)
        try:
            await asyncio.sleep(5 if n != 1 else 0)
        except asyncio.CancelledError:
            cancelled.append(n)
            raise
        return n

    items, _, _ = await run_meta(
        FanoutEvent,
        ask,
        collection=[0],
        count=3,
        aggregation_strategy="fastest",
        concurrency_mode="async",
    )

    assert items == [1] and sorted(cancelled) == [0, 2]


@pytest.mark.asyncio
async def test_fanout_a_failed_replica_fails_the_item_unless_tolerated(run_meta):
    strict, _, bad = await run_meta(
        FanoutEvent,
        scripted(1, ValueError("x")),
        collection=[0],
        count=2,
        max_workers=1,
    )
    lenient, _, ok = await run_meta(
        FanoutEvent,
        scripted(1, ValueError("x")),
        collection=[0],
        count=2,
        max_workers=1,
        continue_on_error=True,
    )
    short, _, not_enough = await run_meta(
        FanoutEvent,
        scripted(1, ValueError("x")),
        collection=[0],
        count=2,
        max_workers=1,
        continue_on_error=True,
        min_success_count=2,
    )

    assert (
        not bad.success and ok.success and lenient == [[1]] and not not_enough.success
    )
