import asyncio
import random
import threading

import pytest

from volnux.event.meta import MapEvent
from volnux.result import EventResult

from ..conftest import make_event_class


def double(x):
    return x * 2


@pytest.mark.asyncio
async def test_map_applies_a_function_to_every_item(run_meta):
    items, _, result = await run_meta(MapEvent, double, collection=[1, 2, 3])
    assert items == [2, 4, 6]
    assert result.success


@pytest.mark.asyncio
async def test_an_async_function_is_awaited(run_meta):
    async def slow_double(x):
        await asyncio.sleep(0)
        return x * 2

    items, _, _ = await run_meta(MapEvent, slow_double, collection=[1, 2])
    assert items == [2, 4]


@pytest.mark.asyncio
async def test_an_event_class_is_the_template_and_gets_the_item_as_previous_result(
    run_meta,
):
    async def process(self, *args, **kwargs):
        return True, self.previous_result[0].content + 100

    template = make_event_class(process=process)

    items, _, _ = await run_meta(MapEvent, template, collection=[1, 2, 3])

    assert items == [101, 102, 103]


@pytest.mark.asyncio
async def test_each_item_gets_a_fresh_event_instance(run_meta):
    """An event runs once and caches its result: reusing one would repeat the first."""
    seen = set()

    async def process(self, *args, **kwargs):
        seen.add(self._task_id)
        return True, self.previous_result[0].content

    template = make_event_class(process=process)

    items, _, _ = await run_meta(MapEvent, template, collection=[7, 8, 9])

    assert items == [7, 8, 9] and len(seen) == 3


@pytest.mark.asyncio
async def test_a_failing_template_event_fails_the_stream(run_meta):
    async def process(self, *args, **kwargs):
        raise ValueError("no")

    template = make_event_class(process=process)

    items, event, result = await run_meta(MapEvent, template, collection=[1, 2])

    assert not result.success
    assert len(items) == 1 and "no" in items[0]


@pytest.mark.asyncio
async def test_results_keep_input_order_though_workers_finish_out_of_order(run_meta):
    delays = {i: random.Random(i).random() * 0.02 for i in range(40)}

    async def jittered(i):
        await asyncio.sleep(delays[i])
        return i

    items, _, _ = await run_meta(
        MapEvent,
        jittered,
        collection=list(range(40)),
        max_workers=8,
        concurrency_mode="async",
    )

    assert items == list(range(40))


@pytest.mark.asyncio
async def test_unordered_emits_as_items_finish(run_meta):
    async def by_delay(i):
        await asyncio.sleep(0.15 if i == 0 else 0.0)
        return i

    items, _, _ = await run_meta(
        MapEvent,
        by_delay,
        collection=[0, 1, 2],
        max_workers=3,
        ordered=False,
        concurrency_mode="async",
    )

    assert sorted(items) == [0, 1, 2] and items[-1] == 0  # the slow one is last


@pytest.mark.asyncio
async def test_items_really_run_concurrently(run_meta):
    running = peak = 0

    async def probe(i):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1
        return i

    await run_meta(
        MapEvent,
        probe,
        collection=list(range(12)),
        max_workers=4,
        concurrency_mode="async",
    )

    assert peak == 4


@pytest.mark.asyncio
async def test_concurrent_false_runs_one_at_a_time(run_meta):
    running = peak = 0

    async def probe(i):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.005)
        running -= 1
        return i

    await run_meta(
        MapEvent,
        probe,
        collection=list(range(6)),
        concurrent=False,
        concurrency_mode="async",
    )

    assert peak == 1


@pytest.mark.asyncio
async def test_a_sync_function_runs_on_a_worker_thread(run_meta):
    main = threading.get_ident()

    items, _, _ = await run_meta(
        MapEvent, lambda x: threading.get_ident(), collection=[1]
    )

    assert items[0] != main


@pytest.mark.asyncio
async def test_a_failure_stops_the_stream_and_is_reported(run_meta):
    def boom(x):
        if x == 3:
            raise ValueError("bad item")
        return x

    items, _, result = await run_meta(
        MapEvent, boom, collection=[1, 2, 3, 4, 5], max_workers=1
    )

    assert not result.success
    assert items[:2] == [1, 2] and "ValueError: bad item" in items[2]
    assert len(items) == 3  # fail_fast: nothing after the failure


@pytest.mark.asyncio
async def test_partial_success_emits_none_for_a_failed_item(run_meta):
    def boom(x):
        if x == 2:
            raise ValueError("bad")
        return x

    items, _, result = await run_meta(
        MapEvent, boom, collection=[1, 2, 3], partial_success=True
    )

    assert items == [1, None, 3] and result.success


@pytest.mark.asyncio
async def test_skip_errors_leaves_a_failed_item_out(run_meta):
    def boom(x):
        if x == 2:
            raise ValueError("bad")
        return x

    items, _, _ = await run_meta(MapEvent, boom, collection=[1, 2, 3], skip_errors=True)

    assert items == [1, 3]


@pytest.mark.asyncio
async def test_batches_give_the_template_a_list_and_emit_each_result(run_meta):
    calls = []

    def per_batch(batch):
        calls.append(list(batch))
        return [x * 10 for x in batch]

    items, _, _ = await run_meta(
        MapEvent, per_batch, collection=[1, 2, 3, 4, 5], batch_size=2
    )

    assert items == [10, 20, 30, 40, 50]
    assert sorted(calls) == [[1, 2], [3, 4], [5]]


@pytest.mark.asyncio
async def test_a_per_call_timeout_fails_the_item(run_meta):
    async def slow(x):
        await asyncio.sleep(1)

    items, _, result = await run_meta(MapEvent, slow, collection=[1], timeout=0.05)

    assert not result.success and "TimeoutError" in items[0]


@pytest.mark.asyncio
async def test_empty_input_gives_an_empty_successful_stream(run_meta):
    items, _, result = await run_meta(MapEvent, double, collection=[])

    assert items == [] and result.success


# --- where the input comes from -------------------------------------------------


def results(*contents):
    return [
        EventResult(
            error=False, event_name="up", content=c, workflow_id=None, task_id="up"
        )
        for c in contents
    ]


@pytest.mark.asyncio
async def test_previous_result_collections_are_taken_item_by_item(run_meta):
    items, _, _ = await run_meta(MapEvent, double, previous=results([1, 2], [3]))
    assert items == [2, 4, 6]


@pytest.mark.asyncio
async def test_a_scalar_previous_result_is_one_item_and_a_string_is_not_split(run_meta):
    items, _, _ = await run_meta(
        MapEvent, lambda s: s + "!", previous=results("abc", "de")
    )
    assert items == ["abc!", "de!"]


@pytest.mark.asyncio
async def test_it_reads_the_upstream_stream_when_linked_with_a_pipe(run_meta):
    async def process(self, *args, **kwargs):
        for i in range(5):
            yield True, i

    producer = make_event_class(process=process)
    from unittest.mock import MagicMock

    up = producer(execution_context=MagicMock(), task_id="producer-1")
    await up()

    items, _, result = await run_meta(
        MapEvent,
        double,
        configure=lambda e: setattr(e, "_upstream_stream_id", up.stream_id),
    )

    assert items == [0, 2, 4, 6, 8] and result.success


@pytest.mark.asyncio
async def test_a_failed_upstream_stream_fails_the_map(run_meta):
    async def process(self, *args, **kwargs):
        yield True, 1
        yield False, "producer broke"

    producer = make_event_class(process=process)
    from unittest.mock import MagicMock

    up = producer(execution_context=MagicMock(), task_id="producer-2")
    await up()

    items, _, result = await run_meta(
        MapEvent,
        double,
        configure=lambda e: setattr(e, "_upstream_stream_id", up.stream_id),
    )

    assert not result.success


# --- configuration -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_template_comes_from_the_pointy_options_by_name(run_meta, bus):
    from unittest.mock import MagicMock

    from volnux.parser.options import Options

    async def process(self, *args, **kwargs):
        return True, self.previous_result[0].content + 1

    named = make_event_class(process=process)
    cls = type("MapByName", (MapEvent,), {})
    event = cls(
        execution_context=MagicMock(),
        task_id="by-name",
        options=Options(
            extras={"template_class": named.__name__, "collection": [1, 2]}
        ),
    )
    await event()

    from .conftest import stream_of

    assert [
        c.content for c in await stream_of(event.stream_id) if not c.is_sentinel
    ] == [2, 3]


@pytest.mark.asyncio
async def test_no_template_is_a_clear_error(bus):
    from unittest.mock import MagicMock

    from volnux.parser.options import Options

    cls = type("MapNoTemplate", (MapEvent,), {})
    event = cls(
        execution_context=MagicMock(),
        task_id="none",
        options=Options(extras={"collection": [1]}),
    )
    from volnux.exceptions import StopProcessingError

    with pytest.raises(StopProcessingError, match="no template"):
        await event()


@pytest.mark.asyncio
async def test_a_meta_event_cannot_be_a_template(run_meta):
    from volnux.event.meta import FilterEvent

    items, _, result = await run_meta(MapEvent, FilterEvent, collection=[1])

    assert not result.success


@pytest.mark.asyncio
async def test_bad_option_values_are_rejected(run_meta):
    for bad in ({"max_workers": 0}, {"concurrency_mode": "gpu"}, {"batch_size": "x"}):
        _, _, result = await run_meta(MapEvent, double, collection=[1], **bad)
        assert not result.success, bad


@pytest.mark.asyncio
async def test_a_function_can_run_on_the_shared_process_pool(run_meta):
    import math

    items, _, _ = await run_meta(
        MapEvent,
        math.sqrt,
        collection=[4, 9, 16],
        concurrency_mode="process",
        max_workers=2,
    )

    assert items == [2.0, 3.0, 4.0]
