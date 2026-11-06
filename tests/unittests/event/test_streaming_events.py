"""Streaming events: a generator ``process()`` writes a stream, ``input_stream()`` reads it."""

import asyncio

import pytest

from volnux import EventBase
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.decorators import event
from volnux.mixins.stream.mixin import (
    StreamChunkLimitExceeded,
    StreamIdleTimeout,
    StreamPolicy,
    StreamResultRef,
    StreamingNotAllowed,
    UpstreamStreamFailed,
)
from volnux.mixins.stream.queue import SENTINEL_ORDER, StreamChunk

from .conftest import make_event_class


@pytest.fixture
def bus():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="stream-tests")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


def producer_class(items, **attrs):
    async def process(self, *args, **kwargs):
        for item in items:
            yield item

    return make_event_class(process=process, **attrs)


async def stream_of(stream_id):
    """Everything on a stream, read by a throwaway group."""
    route = StreamChunk.route(stream_id)
    await StreamChunk.stream_ensure_consumer_group("probe", route_override=route)
    entries = await StreamChunk.stream_read(
        "probe", count=1000, block_ms=None, route_override=route
    )
    return [e.record for e in entries]


def data_of(chunks):
    return [c.content for c in chunks if not c.is_sentinel]


class TestDetection:
    def test_async_generator_is_streaming(self):
        assert producer_class([(True, 1)])._volnux_streaming is True

    def test_sync_generator_is_streaming(self):
        def process(self):
            yield True, 1

        assert make_event_class(process=process)._volnux_streaming is True

    def test_plain_process_is_not(self):
        assert make_event_class()._volnux_streaming is False

    def test_the_base_class_is_not(self):
        assert EventBase._volnux_streaming is False

    def test_the_decorator_keeps_a_generator_a_generator(self):
        @event()
        async def deco(self):
            yield True, 1

        @event()
        def deco_sync(self):
            yield True, 1

        @event()
        def deco_plain(self):
            return True, 1

        assert deco._volnux_streaming and deco_sync._volnux_streaming
        assert not deco_plain._volnux_streaming

    def test_a_subclass_inherits_a_generator_process(self):
        parent = producer_class([(True, 1)])
        child = type("StreamChild", (parent,), {})
        assert child._volnux_streaming is True

    def test_overriding_with_a_plain_process_turns_streaming_off(self):
        parent = producer_class([(True, 1)])

        async def process(self):
            return True, 1

        child = type("PlainChild", (parent,), {"process": process})
        assert child._volnux_streaming is False


@pytest.mark.asyncio
class TestProducer:
    async def test_chunks_and_sentinel_are_written_in_order(self, bus, build):
        cls = producer_class([(True, "a"), (True, "b"), (True, "c")])
        await build(cls)()
        chunks = await stream_of("task-1")
        assert data_of(chunks) == ["a", "b", "c"]
        assert [c.order for c in chunks] == [0, 1, 2, SENTINEL_ORDER]
        assert chunks[-1].is_sentinel and chunks[-1].success is True

    async def test_the_result_is_a_reference_not_the_data(self, bus, build):
        cls = producer_class([(True, 1), (True, 2)])
        result = await build(cls)()
        assert result.error is False
        ref = result.content
        assert StreamResultRef.is_ref(ref)
        assert (ref["task_id"], ref["stream_id"]) == ("task-1", "task-1")
        assert ref["chunk_count"] == 2 and ref["complete"] is True
        assert ref["summary"]["count"] == 2

    async def test_the_stream_id_defaults_to_the_task_and_can_be_wired(
        self, bus, build
    ):
        cls = producer_class([(True, 1)])
        event_ = build(cls)
        event_._stream_id = "custom"
        await event_()
        assert data_of(await stream_of("custom")) == [1]
        assert await stream_of("task-1") == []

    async def test_a_failing_chunk_fails_the_event_and_stops_the_stream(
        self, bus, build
    ):
        cls = producer_class([(True, 1), (False, 2), (True, 3)])
        result = await build(cls)()
        assert result.error is True
        chunks = await stream_of("task-1")
        assert data_of(chunks) == [1, 2]  # fail_fast: 3 never produced
        assert chunks[-1].is_sentinel and chunks[-1].success is False

    async def test_without_fail_fast_the_whole_stream_is_written(self, bus, build):
        cls = producer_class(
            [(True, 1), (False, 2), (True, 3)],
            stream_policy=StreamPolicy(fail_fast=False),
        )
        result = await build(cls)()
        assert result.error is True
        chunks = await stream_of("task-1")
        assert data_of(chunks) == [1, 2, 3] and chunks[-1].success is False

    async def test_a_raising_generator_closes_the_stream_as_failed(self, bus, build):
        async def process(self):
            yield True, 1
            raise RuntimeError("source broke")

        result = await build(make_event_class(process=process))()
        assert result.error is True
        chunks = await stream_of("task-1")
        assert data_of(chunks) == [1]
        assert chunks[-1].is_sentinel and chunks[-1].success is False

    async def test_a_bad_yield_is_an_error_and_closes_the_stream(self, bus, build):
        async def process(self):
            yield "not a tuple"

        result = await build(make_event_class(process=process))()
        assert result.error is True
        assert (await stream_of("task-1"))[-1].success is False

    async def test_non_bool_status_is_refused(self, bus, build):
        async def process(self):
            yield "yes", 1

        assert (await build(make_event_class(process=process))()).error is True

    async def test_max_chunks_is_enforced(self, bus, build):
        cls = producer_class(
            [(True, i) for i in range(10)], stream_policy=StreamPolicy(max_chunks=3)
        )
        result = await build(cls)()
        assert result.error is True
        chunks = await stream_of("task-1")
        assert data_of(chunks) == [0, 1, 2] and chunks[-1].success is False

    async def test_a_sync_generator_streams_without_blocking(self, bus, build):
        def process(self):
            for i in range(3):
                yield True, i

        await build(make_event_class(process=process))()
        assert data_of(await stream_of("task-1")) == [0, 1, 2]

    async def test_an_empty_stream_is_just_a_sentinel(self, bus, build):
        result = await build(producer_class([]))()
        assert result.error is False and result.content["chunk_count"] == 0
        chunks = await stream_of("task-1")
        assert len(chunks) == 1 and chunks[0].is_sentinel and chunks[0].success

    async def test_the_summary_describes_numeric_data(self, bus, build):
        cls = producer_class([(True, 1), (True, 2), (True, 3)])
        summary = (await build(cls)()).content["summary"]
        assert summary["type"] == "numeric" and summary["count"] == 3

    async def test_a_cancelled_producer_still_closes_the_stream(self, bus, build):
        started = asyncio.Event()

        async def process(self):
            yield True, 1
            started.set()
            await asyncio.sleep(30)
            yield True, 2

        task = asyncio.create_task(build(make_event_class(process=process))())
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        chunks = await stream_of("task-1")
        assert data_of(chunks) == [1]
        assert chunks[-1].is_sentinel and chunks[-1].success is False

    async def test_a_streaming_event_is_not_retried_on_its_own(self, bus, build):
        from volnux.mixins.event.retry import RetryPolicy

        calls = []

        async def process(self):
            calls.append(1)
            yield False, "bad"

        cls = make_event_class(
            process=process, retry_policy=RetryPolicy(max_attempts=3)
        )
        await build(cls)()
        assert len(calls) == 1

    async def test_a_plain_event_is_unaffected(self, bus, build):
        result = await build(make_event_class())()
        assert result.content == "ok" and result.error is False
        assert await stream_of("task-1") == []

    async def test_agents_may_not_stream(self, bus, build):
        cls = producer_class([(True, 1)], _volnux_agent=True)
        result = await build(cls)()
        assert result.error is True

    async def test_backpressure_makes_the_producer_wait_for_the_consumer(
        self, bus, build
    ):
        cls = producer_class(
            [(True, i) for i in range(6)], stream_policy=StreamPolicy(max_in_flight=2)
        )
        producer = asyncio.create_task(build(cls)())
        await asyncio.sleep(0.15)
        assert (
            await StreamChunk.stream_length(route_override=StreamChunk.route("task-1"))
            == 2
        )
        assert not producer.done()

        # drain: trimming stands in for a consumer that has caught up
        for _ in range(60):
            await StreamChunk.stream_trim(0, route_override=StreamChunk.route("task-1"))
            if producer.done():
                break
            await asyncio.sleep(0.02)
        assert (await asyncio.wait_for(producer, 2)).error is False

    async def test_release_stream_deletes_it(self, bus, build):
        event_ = build(producer_class([(True, 1)]))
        await event_()
        assert await event_.release_stream() is True
        assert await stream_of("task-1") == []


@pytest.mark.asyncio
class TestResume:
    async def test_chunks_already_pushed_are_not_pushed_again(self, bus, build):
        cls = producer_class([(True, i) for i in range(5)])
        first = build(cls)
        await first()
        before = data_of(await stream_of("task-1"))
        assert before == [0, 1, 2, 3, 4]

        # a restart of the same task after a crash that left 3 chunks pushed
        await StreamChunk.stream_delete(route_override=StreamChunk.route("task-1"))
        for i in range(3):
            await first._push(i, True, i)
        restarted = build(cls)
        restarted._stream_pushed = 3
        result = await restarted()
        assert data_of(await stream_of("task-1")) == [0, 1, 2, 3, 4]
        assert result.content["chunk_count"] == 5  # the summary still sees them all

    async def test_progress_is_part_of_the_checkpointed_attributes(self, bus, build):
        from volnux.execution.rehydrator.event.serializer import StateSerializer

        event_ = build(producer_class([(True, 1), (True, 2)]))
        await event_()
        assert StateSerializer.serialize_attribs(event_)["_stream_pushed"] == 2


def consumer_of(build, ctx_task, upstream="task-1", **attrs):
    seen = []

    async def process(self):
        async for chunk in self.input_stream():
            seen.append(chunk.content)
        return True, list(seen)

    cls = make_event_class(process=process, **attrs)
    consumer = build(cls, task_id=ctx_task)
    consumer._upstream_stream_id = upstream
    return consumer, seen


@pytest.mark.asyncio
class TestConsumer:
    async def test_reads_the_whole_stream_in_order(self, bus, build):
        await build(producer_class([(True, i) for i in range(5)]))()
        consumer, seen = consumer_of(build, "c1")
        result = await consumer()
        assert seen == [0, 1, 2, 3, 4] and result.content == [0, 1, 2, 3, 4]

    async def test_reads_while_the_producer_is_still_running(self, bus, build):
        gate = asyncio.Event()

        async def slow(self):
            yield True, "first"
            await gate.wait()
            yield True, "second"

        producer = asyncio.create_task(build(make_event_class(process=slow))())
        consumer, seen = consumer_of(build, "c1")
        reading = asyncio.create_task(consumer())

        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert seen == ["first"] and not producer.done()  # got it before the end
        gate.set()
        await asyncio.wait_for(producer, 2)
        assert (await asyncio.wait_for(reading, 2)).content == ["first", "second"]

    async def test_a_failed_stream_fails_the_consumer(self, bus, build):
        await build(producer_class([(True, 1), (False, 2)]))()
        consumer, seen = consumer_of(build, "c1")
        result = await consumer()
        assert result.error is True and seen == [1, 2]

    async def test_upstream_failure_surfaces_as_a_named_error(self, bus, build):
        await build(producer_class([(False, 1)]))()
        consumer, _ = consumer_of(build, "c1")

        with pytest.raises(UpstreamStreamFailed):
            async for _chunk in consumer.input_stream():
                pass

    async def test_every_consumer_sees_every_chunk(self, bus, build):
        await build(producer_class([(True, i) for i in range(3)]))()
        (one, seen_one), (two, seen_two) = (
            consumer_of(build, "c1"),
            consumer_of(build, "c2"),
        )
        await one()
        await two()
        assert seen_one == seen_two == [0, 1, 2]

    async def test_input_data_yields_payloads(self, bus, build):
        await build(producer_class([(True, "x"), (True, "y")]))()
        consumer = build(make_event_class(), task_id="c1")
        consumer._upstream_stream_id = "task-1"
        assert [d async for d in consumer.input_data()] == ["x", "y"]

    async def test_a_redelivered_chunk_is_skipped(self, bus, build):
        await build(producer_class([(True, 1), (True, 2)]))()
        route = StreamChunk.route("task-1")
        # the producer restarted and pushed chunk 0 a second time, then the sentinel
        await StreamChunk.stream_delete(route_override=route)
        for order, value in ((0, 1), (0, 1), (1, 2)):
            await StreamChunk.stream_append(
                StreamChunk.make("task-1", order, True, value, "E"),
                route_override=route,
            )
        await StreamChunk.stream_append(
            StreamChunk.make_sentinel("task-1", True, "E"), route_override=route
        )
        consumer, seen = consumer_of(build, "c1")
        await consumer()
        assert seen == [1, 2]

    async def test_chunks_are_acknowledged_after_they_are_handled(self, bus, build):
        await build(producer_class([(True, 1), (True, 2)]))()
        consumer = build(
            make_event_class(stream_policy=StreamPolicy(read_batch=1)), task_id="c1"
        )
        consumer._upstream_stream_id = "task-1"
        route = StreamChunk.route("task-1")

        stream = consumer.input_stream()
        first = await stream.__anext__()
        assert first.content == 1
        # the loop body has not finished with chunk 1 yet: it is still pending
        pending = await StreamChunk.stream_claim_pending(
            "consumer:c1", "other", min_idle_ms=0, route_override=route
        )
        assert [p.record.content for p in pending] == [1]
        await stream.aclose()

    async def test_an_idle_stream_times_out(self, bus, build):
        consumer = build(
            make_event_class(
                stream_policy=StreamPolicy(read_block_ms=20, idle_timeout=0.1)
            ),
            task_id="c1",
        )
        consumer._upstream_stream_id = "nothing-here"
        with pytest.raises(StreamIdleTimeout):
            async for _chunk in consumer.input_stream():
                pass

    async def test_no_upstream_is_a_clear_error(self, bus, build):
        consumer = build(make_event_class(), task_id="c1")
        with pytest.raises(ValueError, match="no upstream"):
            async for _chunk in consumer.input_stream():
                pass

    async def test_a_consumer_that_started_first_waits_for_the_producer(
        self, bus, build
    ):
        consumer, seen = consumer_of(build, "c1")
        reading = asyncio.create_task(consumer())
        await asyncio.sleep(0.1)
        assert not reading.done()
        await build(producer_class([(True, "late")]))()
        assert (await asyncio.wait_for(reading, 3)).content == ["late"]

    async def test_a_restarted_consumer_does_not_handle_a_chunk_twice(self, bus, build):
        await build(producer_class([(True, i) for i in range(4)]))()
        first = build(make_event_class(), task_id="c1")
        first._upstream_stream_id = "task-1"

        got = []
        stream = first.input_stream()
        got.append(
            (await stream.__anext__()).content
        )  # chunk 0 handled + acked on next pull
        got.append((await stream.__anext__()).content)  # chunk 1 delivered, never acked
        await stream.aclose()
        assert first._stream_consumed == {"task-1": 0}

        # the same task restarts with its checkpointed progress
        again = build(make_event_class(), task_id="c1")
        again._upstream_stream_id = "task-1"
        again._stream_consumed = dict(first._stream_consumed)
        rest = [chunk.content async for chunk in again.input_stream()]
        assert rest == [1, 2, 3]


def fan_in_consumer(build, ids, task_id="c1", **attrs):
    consumer = build(make_event_class(**attrs), task_id=task_id)
    consumer._upstream_stream_ids = list(ids)
    return consumer


async def produce(build, task_id, items):
    await build(producer_class(items), task_id=task_id)()


@pytest.mark.asyncio
class TestFanInReading:
    async def test_chunks_of_every_source_arrive_and_the_stream_ends_after_all(
        self, bus, build
    ):
        await produce(build, "p1", [(True, "a1"), (True, "a2")])
        await produce(build, "p2", [(True, "b1")])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        got = [chunk async for chunk in consumer.input_stream()]
        assert sorted(c.content for c in got) == ["a1", "a2", "b1"]
        assert {c.task_id for c in got} == {"p1", "p2"}

    async def test_each_sources_order_is_kept(self, bus, build):
        await produce(build, "p1", [(True, i) for i in range(5)])
        await produce(build, "p2", [(True, i + 100) for i in range(5)])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        got = [(c.task_id, c.content) async for c in consumer.input_stream()]
        assert [v for t, v in got if t == "p1"] == [0, 1, 2, 3, 4]
        assert [v for t, v in got if t == "p2"] == [100, 101, 102, 103, 104]

    async def test_it_waits_for_a_source_that_has_not_ended(self, bus, build):
        await produce(build, "p1", [(True, "early")])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        reading = asyncio.create_task(_collect(consumer))
        await asyncio.sleep(0.1)
        assert not reading.done()  # p2 has not ended
        await produce(build, "p2", [(True, "late")])
        got = await asyncio.wait_for(reading, 3)
        assert sorted(c.content for c in got) == ["early", "late"]

    async def test_a_failing_source_fails_the_read(self, bus, build):
        await produce(build, "p1", [(True, 1)])
        await produce(build, "p2", [(True, 1), (False, 2)])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        with pytest.raises(UpstreamStreamFailed):
            async for _chunk in consumer.input_stream():
                pass

    async def test_progress_is_kept_per_stream(self, bus, build):
        await produce(build, "p1", [(True, 1), (True, 2)])
        await produce(build, "p2", [(True, 9)])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        async for _chunk in consumer.input_stream():
            pass
        assert consumer._stream_consumed == {"p1": 1, "p2": 0}

    async def test_a_restart_skips_what_each_source_already_delivered(self, bus, build):
        await produce(build, "p1", [(True, 1), (True, 2), (True, 3)])
        await produce(build, "p2", [(True, 10), (True, 20)])
        again = fan_in_consumer(build, ["p1", "p2"])
        again._stream_consumed = {"p1": 1, "p2": 0}  # restored from a checkpoint
        got = [(c.task_id, c.content) async for c in again.input_stream()]
        assert sorted(got) == [("p1", 3), ("p2", 20)]

    async def test_breaking_out_early_stops_the_readers(self, bus, build):
        await produce(build, "p1", [(True, i) for i in range(5)])
        await produce(build, "p2", [(True, i) for i in range(5)])
        consumer = fan_in_consumer(build, ["p1", "p2"])
        stream = consumer.input_stream()
        await stream.__anext__()
        await stream.aclose()  # must not hang or leak reader tasks

    async def test_an_idle_source_times_out(self, bus, build):
        await produce(build, "p1", [(True, 1)])
        consumer = fan_in_consumer(
            build,
            ["p1", "never"],
            stream_policy=StreamPolicy(read_block_ms=20, idle_timeout=0.1),
        )
        with pytest.raises(StreamIdleTimeout):
            async for _chunk in consumer.input_stream():
                pass

    async def test_replay_merges_the_retained_streams(self, bus, build):
        await produce(build, "p1", [(True, "a")])
        await produce(build, "p2", [(True, "b")])
        consumer = fan_in_consumer(build, ["p1", "p2"], task_id="after")
        first = [c.content async for c in consumer.replay()]
        second = [c.content async for c in consumer.replay()]
        assert sorted(first) == sorted(second) == ["a", "b"]

    async def test_references_from_several_predecessors(self, bus, build):
        from volnux.result import ResultSet

        r1 = await build(producer_class([(True, 1)]), task_id="p1")()
        r2 = await build(producer_class([(True, 2)]), task_id="p2")()
        consumer = build(
            make_event_class(), task_id="next", previous_result=ResultSet([r1, r2])
        )
        assert sorted(ref.stream_id for ref in consumer._upstream_refs()) == [
            "p1",
            "p2",
        ]
        assert sorted([c.content async for c in consumer.replay()]) == [1, 2]

    async def test_a_single_upstream_still_works_through_the_old_attribute(
        self, bus, build
    ):
        await produce(build, "p1", [(True, 5)])
        consumer = build(make_event_class(), task_id="c1")
        consumer._upstream_stream_id = "p1"
        assert [c.content async for c in consumer.input_stream()] == [5]

    async def test_a_plain_event_publishes_a_single_chunk(self, bus, build):
        plain = build(make_event_class(), task_id="plain")
        await plain.publish_single(True, "value")
        chunks = await stream_of("plain")
        assert [c.content for c in chunks if not c.is_sentinel] == ["value"]
        assert chunks[-1].is_sentinel and chunks[-1].success is True

    async def test_a_failed_plain_event_closes_its_stream_as_failed(self, bus, build):
        plain = build(make_event_class(), task_id="plain")
        await plain.publish_single(False, "boom")
        assert (await stream_of("plain"))[-1].success is False


async def _collect(consumer):
    return [chunk async for chunk in consumer.input_stream()]


@pytest.mark.asyncio
class TestChains:
    async def test_a_transform_stage_is_a_generator_that_consumes(self, bus, build):
        await build(producer_class([(True, 1), (True, 2), (True, 3)]))()

        async def double(self):
            async for chunk in self.input_stream():
                yield True, chunk.content * 2

        stage = build(make_event_class(process=double), task_id="stage")
        stage._upstream_stream_id = "task-1"
        await stage()
        assert data_of(await stream_of("stage")) == [2, 4, 6]

        consumer, seen = consumer_of(build, "sink", upstream="stage")
        await consumer()
        assert seen == [2, 4, 6]

    async def test_the_failure_travels_down_the_chain(self, bus, build):
        await build(producer_class([(True, 1), (False, 2)]))()

        async def passthrough(self):
            async for chunk in self.input_stream():
                yield True, chunk.content

        stage = build(make_event_class(process=passthrough), task_id="stage")
        stage._upstream_stream_id = "task-1"
        result = await stage()
        assert result.error is True
        assert (await stream_of("stage"))[-1].success is False


@pytest.mark.asyncio
class TestArrowAfterAGenerator:
    """``A -> B``: B receives the reference, and may replay the retained log."""

    async def test_the_next_task_gets_the_reference(self, bus, build):
        result = await build(producer_class([(True, 1), (True, 2)]))()
        consumer = build(make_event_class(), task_id="next", previous_result=[result])
        ref = consumer._upstream_ref()
        assert ref is not None and ref.stream_id == "task-1" and ref.chunk_count == 2

    async def test_replay_reads_the_retained_stream_from_the_start(self, bus, build):
        result = await build(producer_class([(True, "a"), (True, "b")]))()
        from volnux.result import ResultSet

        consumer = build(
            make_event_class(), task_id="next", previous_result=ResultSet([result])
        )
        assert [c.content async for c in consumer.replay()] == ["a", "b"]
        # and it can be replayed again, by a fresh group each time
        assert [c.content async for c in consumer.replay()] == ["a", "b"]

    async def test_input_stream_falls_back_to_the_reference(self, bus, build):
        from volnux.result import ResultSet

        result = await build(producer_class([(True, 5)]))()
        consumer = build(
            make_event_class(), task_id="next", previous_result=ResultSet([result])
        )
        assert [c.content async for c in consumer.input_stream()] == [5]

    async def test_a_plain_predecessor_has_no_reference(self, bus, build):
        result = await build(make_event_class())()
        consumer = build(make_event_class(), task_id="next", previous_result=[result])
        assert consumer._upstream_ref() is None


class TestResultRef:
    def test_round_trips_through_a_dict(self):
        ref = StreamResultRef(task_id="t", stream_id="s", chunk_count=3, complete=True)
        back = StreamResultRef.from_dict(ref.to_dict())
        assert (back.task_id, back.stream_id, back.chunk_count, back.complete) == (
            "t",
            "s",
            3,
            True,
        )

    def test_is_ref_recognises_only_references(self):
        assert StreamResultRef.is_ref(StreamResultRef("t", "s").to_dict())
        assert not StreamResultRef.is_ref({"task_id": "t"})
        assert not StreamResultRef.is_ref("text")
        assert not StreamResultRef.is_ref(None)
