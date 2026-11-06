import asyncio
import random
import warnings

import pytest

from volnux.execution.remote.context import MiniExecutionContext, ReservoirSample
from volnux.execution.remote.delta import ContextDelta


def ok(i=0):
    return {"error": False, "content": {"value": i}}


async def filled(n, callback=True, **kw):
    ctx = MiniExecutionContext(state_id="s1", **kw)
    got = []
    if callback:
        await ctx.set_flush_callback(got.append)
    for i in range(n):
        await ctx.add_result(ok(i))
    return ctx, got


class TestReservoirSample:
    def test_keeps_everything_until_full(self):
        r = ReservoirSample(k=5)
        for i in range(3):
            r.add({"i": i})
        assert [x["i"] for x in r.sample()] == [0, 1, 2]
        assert not r.is_full

    def test_never_exceeds_k_and_counts_all(self):
        r = ReservoirSample(k=5, _rng=random.Random(1))
        for i in range(1000):
            r.add({"i": i})
        assert len(r.sample()) == 5 and r.is_full
        assert r.total_seen == 1000

    def test_sample_is_a_copy(self):
        r = ReservoirSample(k=2)
        r.add({"i": 1})
        r.sample().clear()
        assert len(r.sample()) == 1

    def test_is_roughly_uniform(self):
        hits = [0] * 20
        for seed in range(400):
            r = ReservoirSample(k=4, _rng=random.Random(seed))
            for i in range(20):
                r.add({"i": i})
            for x in r.sample():
                hits[x["i"]] += 1
        # expected 400 * 4 / 20 = 80 per item
        assert all(40 < h < 130 for h in hits), hits

    def test_round_trip(self):
        r = ReservoirSample(k=3)
        for i in range(10):
            r.add({"i": i})
        again = ReservoirSample.from_dict(r.to_dict())
        assert again.sample() == r.sample() and again.total_seen == 10


class TestModes:
    @pytest.mark.asyncio
    async def test_small_keeps_results_in_memory(self):
        ctx, got = await filled(MiniExecutionContext.SMALL_THRESHOLD - 1)
        assert ctx.mode == "small" and len(ctx.results) == 99 and got == []

    @pytest.mark.asyncio
    async def test_switch_to_batch_flushes_what_was_held(self):
        ctx, got = await filled(MiniExecutionContext.SMALL_THRESHOLD + 1)
        assert ctx.mode == "batch"
        assert got[0]["type"] == "result_batch" and got[0]["count"] == 100
        assert ctx.results == []

    @pytest.mark.asyncio
    async def test_no_result_is_lost_across_the_switch(self):
        # Items after the switch used to go to the small-mode list and were lost.
        ctx, got = await filled(450)
        await ctx.finalize()
        data = [r for b in got for r in b["data"]]
        assert [r["content"]["value"] for r in data] == list(range(450))
        assert ctx.total_processed == 450 == ctx.total_succeeded

    @pytest.mark.asyncio
    async def test_batches_are_flushed_in_chunks(self):
        ctx, got = await filled(350)
        # 100 (switch) + 100 + 100 flushed so far; 50 still buffered
        assert [b["count"] for b in got] == [100, 100, 100]
        assert len(ctx.result_buffer) == 50
        await ctx.finalize()
        assert got[-1]["count"] == 50 and ctx.result_buffer == []

    @pytest.mark.asyncio
    async def test_aggregate_mode_is_reachable(self):
        ctx, got = await filled(MiniExecutionContext.BATCH_THRESHOLD + 5)
        await ctx.finalize()
        assert ctx.mode == "aggregate"
        assert sum(b["count"] for b in got) == MiniExecutionContext.BATCH_THRESHOLD
        assert ctx.total_processed == MiniExecutionContext.BATCH_THRESHOLD + 5

    @pytest.mark.asyncio
    async def test_aggregate_statistics(self):
        ctx, _ = await filled(MiniExecutionContext.BATCH_THRESHOLD + 10)
        n = MiniExecutionContext.BATCH_THRESHOLD
        # items 0..n-1 were streamed; only the 10 after the switch are aggregated
        assert ctx.min_value == n and ctx.max_value == n + 9
        assert ctx.sum_values == sum(range(n, n + 10))
        assert ctx.sample_results.total_seen == 10

    @pytest.mark.asyncio
    async def test_non_numeric_value_does_not_break_aggregation(self):
        ctx, _ = await filled(MiniExecutionContext.BATCH_THRESHOLD)
        for bad in ("abc", None, [1], float("nan"), float("inf")):
            await ctx.add_result({"error": False, "content": {"value": bad}})
        assert ctx.sum_values == 0.0 and ctx.min_value == float("inf")
        assert ctx.total_succeeded == MiniExecutionContext.BATCH_THRESHOLD + 5

    @pytest.mark.asyncio
    async def test_error_flagged_results_count_as_failed_in_every_mode(self):
        for n in (3, 150, MiniExecutionContext.BATCH_THRESHOLD + 1):
            ctx, _ = await filled(n)
            await ctx.add_result({"error": True, "content": "boom"})
            assert ctx.total_failed == 1, n
            assert ctx.total_succeeded == n, n
            assert ctx.total_processed == n + 1, n

    @pytest.mark.asyncio
    async def test_errors_are_buffered_and_flushed_like_results(self):
        ctx = MiniExecutionContext(state_id="s")
        got = []
        await ctx.set_flush_callback(got.append)
        for i in range(250):
            await ctx.add_error({"content": f"e{i}"})
        await ctx.finalize()
        assert {b["type"] for b in got} == {"error_batch"}
        assert sum(b["count"] for b in got) == 250
        assert ctx.total_failed == 250 == ctx.total_processed

    @pytest.mark.asyncio
    async def test_batch_mode_without_callback_raises(self):
        ctx = MiniExecutionContext(state_id="s")
        with pytest.raises(RuntimeError, match="no"):
            for i in range(MiniExecutionContext.SMALL_THRESHOLD + 1):
                await ctx.add_result(ok(i))

    @pytest.mark.asyncio
    async def test_failing_callback_propagates(self):
        ctx = MiniExecutionContext(state_id="s")

        def boom(_):
            raise OSError("redis down")

        await ctx.set_flush_callback(boom)
        with pytest.raises(OSError):
            for i in range(MiniExecutionContext.SMALL_THRESHOLD + 1):
                await ctx.add_result(ok(i))


class TestFinalizeAndSerialization:
    @pytest.mark.asyncio
    async def test_finalize_is_idempotent_and_keeps_the_first_end_time(self):
        ctx, got = await filled(150)
        await ctx.finalize()
        first = ctx.end_time
        await asyncio.sleep(0.01)
        await ctx.finalize()
        assert ctx.end_time == first and first > 0
        assert sum(b["count"] for b in got) == 150

    @pytest.mark.asyncio
    async def test_small_round_trip_keeps_data(self):
        ctx, _ = await filled(5)
        again = MiniExecutionContext.from_dict(await ctx.to_dict())
        assert again.results == ctx.results and again.total_processed == 5

    @pytest.mark.asyncio
    async def test_batch_round_trip_keeps_counts_not_bulk(self):
        ctx, _ = await filled(300)
        data = await ctx.to_dict()
        assert data["results"] == [] and data["total_processed"] == 300
        again = MiniExecutionContext.from_dict(data)
        assert again.mode == "batch" and again.total_succeeded == 300

    @pytest.mark.asyncio
    async def test_min_max_survive_when_empty(self):
        ctx = MiniExecutionContext(state_id="s")
        data = await ctx.to_dict()
        assert data["min_value"] is None
        again = MiniExecutionContext.from_dict(data)
        assert again.min_value == float("inf") and again.max_value == float("-inf")

    @pytest.mark.asyncio
    async def test_concurrent_adds_lose_nothing(self):
        ctx = MiniExecutionContext(state_id="s")
        got = []
        await ctx.set_flush_callback(got.append)
        await asyncio.gather(*(ctx.add_result(ok(i)) for i in range(1000)))
        await ctx.finalize()
        assert ctx.total_processed == 1000
        assert sum(b["count"] for b in got) == 1000


class TestContextDelta:
    @pytest.mark.asyncio
    async def test_small_mode_carries_the_data(self):
        ctx, _ = await filled(3)
        await ctx.finalize()
        d = ContextDelta.compute(ctx, 1.5, {"hostname": "h", "pid": 7}, "k")
        assert d.mode == "small" and len(d.new_results) == 3
        assert d.final_status == "COMPLETED" and d.batch_key == "k"
        assert (d.worker_hostname, d.worker_pid, d.execution_duration) == ("h", 7, 1.5)
        assert d.end_time == ctx.end_time

    @pytest.mark.asyncio
    async def test_batch_mode_carries_no_data_but_the_right_status(self):
        # status used to be None here: the counters never moved in batch mode
        ctx, _ = await filled(300)
        await ctx.finalize()
        d = ContextDelta.compute(ctx, batch_key="k")
        assert d.mode == "batch" and d.new_results == []
        assert d.final_status == "COMPLETED"

    @pytest.mark.asyncio
    async def test_failure_status_in_every_mode(self):
        for n in (2, 300):
            ctx, _ = await filled(n)
            await ctx.add_error({"content": "x"})
            await ctx.finalize()
            assert ContextDelta.compute(ctx).final_status == "FAILED", n

    @pytest.mark.asyncio
    async def test_empty_context_has_no_status(self):
        ctx = MiniExecutionContext(state_id="s")
        await ctx.finalize()
        assert ContextDelta.compute(ctx).final_status is None

    @pytest.mark.asyncio
    async def test_round_trip_and_tolerant_from_dict(self):
        ctx, _ = await filled(2)
        await ctx.finalize()
        d = ContextDelta.compute(ctx)
        assert ContextDelta.from_dict(d.to_dict()) == d
        data = d.to_dict() | {"from_a_newer_worker": 1}
        assert ContextDelta.from_dict(data) == d
        assert ContextDelta.from_dict({"state_id": "s"}).mode == "small"


class _FakeContext:
    def __init__(self):
        self.added = []
        self.statuses = []
        self.checkpoint = None

        class M:
            end_time = None

        self.metrics = M()

    async def add_result_async(self, r):
        self.added.append(r)

    async def update_status_async(self, s):
        self.statuses.append(s)

    def set_task_checkpoint(self, c):
        self.checkpoint = c


class TestApplyToContext:
    @pytest.mark.asyncio
    async def test_applies_results_errors_status_and_times(self):
        from volnux.execution.status import ExecutionStatus

        d = ContextDelta(
            state_id="s",
            new_results=[
                {"error": False, "content": "a", "event_name": "E", "task_id": "t"}
            ],
            new_errors=[{"content": "bad", "task_id": "t", "type": "ValueError"}],
            final_status="FAILED",
            end_time=12.5,
            checkpoint_data={"cursor": 3},
        )
        ctx = _FakeContext()
        await d.apply_to_context(ctx)
        assert [r.error for r in ctx.added] == [False, True]
        assert ctx.added[1].event_name == "ValueError"
        assert ctx.statuses == [ExecutionStatus.FAILED]
        assert ctx.metrics.end_time == 12.5 and ctx.checkpoint == {"cursor": 3}

    @pytest.mark.asyncio
    async def test_unknown_status_is_ignored(self):
        ctx = _FakeContext()
        await ContextDelta(state_id="s", final_status="NOPE").apply_to_context(ctx)
        assert ctx.statuses == []

    @pytest.mark.asyncio
    async def test_malformed_result_is_kept_as_a_result(self):
        ctx = _FakeContext()
        d = ContextDelta(state_id="s", new_results=[{"unexpected_field": 1}])
        await d.apply_to_context(ctx)
        assert len(ctx.added) == 1
