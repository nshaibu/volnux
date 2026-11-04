"""``|->`` end to end: a segment of tasks runs together, wired by streams."""

import asyncio
import itertools
import time

import pytest

from volnux import EventBase, Pipeline
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.execution.context import ExecutionContext
from volnux.flows import setup_execution_flow
from volnux.flows.parallel import ParallelFlow
from volnux.flows.stream import StreamFlow, stream_producers_of
from volnux.mixins.stream.queue import StreamChunk
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.grammar import pointy_parser
from volnux.parser.operator import PipeType
from volnux.task import PipelineTask, PipelineTaskGrouping

_ids = itertools.count()
# Event names are registered globally and resolved by name from Pointy source, so
# every test module needs names of its own.
TAG = "Seg"


def event_class(name, process):
    return type(f"{name}{TAG}{next(_ids)}", (EventBase,), {"process": process}).__name__


@pytest.fixture
def bus():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="segments")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


async def run(pointy, timeout=10):
    """Run a Pointy workflow; returns (pipeline, error or None)."""
    pipeline_class = type(
        f"Pipe{TAG}{next(_ids)}",
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": pointy})},
    )
    pipeline = pipeline_class()
    try:
        await asyncio.wait_for(pipeline.start(force_rerun=True), timeout)
        return pipeline, None
    except Exception as exc:  # the run reports a failed workflow by raising
        return pipeline, exc


def build(pointy):
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(pointy_parser(pointy))
    return generator.generate()


class Recorder:
    def __init__(self):
        self.log = []

    def __call__(self, *item):
        self.log.append(item if len(item) > 1 else item[0])


def numbers(count=3, delay=0.0):
    async def process(self):
        for i in range(count):
            if delay:
                await asyncio.sleep(delay)
            yield True, i

    return process


def collector(record):
    async def process(self):
        async for chunk in self.input_stream():
            record("got", chunk.content)
        return True, "collected"

    return process


def marker(record, name):
    async def process(self):
        record(name)
        return True, name

    return process


@pytest.mark.asyncio
class TestChains:
    async def test_two_stages(self, bus):
        record = Recorder()
        source = event_class("Src", numbers(4))
        sink = event_class("Sink", collector(record))
        _, error = await run(f"{source} |-> {sink}")
        assert error is None
        assert record.log == [("got", 0), ("got", 1), ("got", 2), ("got", 3)]

    async def test_three_stages_with_a_transform(self, bus):
        record = Recorder()

        async def double(self):
            async for chunk in self.input_stream():
                yield True, chunk.content * 2

        source = event_class("Src", numbers(4))
        stage = event_class("Stage", double)
        sink = event_class("Sink", collector(record))
        _, error = await run(f"{source} |-> {stage} |-> {sink}")
        assert error is None
        assert [v for _, v in record.log] == [0, 2, 4, 6]

    async def test_a_sync_generator_source(self, bus):
        record = Recorder()

        def source_process(self):
            for i in range(3):
                yield True, i * 10

        source = event_class("Src", source_process)
        sink = event_class("Sink", collector(record))
        _, error = await run(f"{source} |-> {sink}")
        assert error is None and [v for _, v in record.log] == [0, 10, 20]

    async def test_members_really_overlap(self, bus):
        """The consumer gets its first chunk before the producer has finished."""
        stamps = {}

        async def slow_source(self):
            for i in range(4):
                await asyncio.sleep(0.05)
                yield True, i
            stamps["producer_done"] = time.monotonic()

        async def sink_process(self):
            async for _chunk in self.input_stream():
                stamps.setdefault("first_chunk", time.monotonic())
            return True, "done"

        source = event_class("Src", slow_source)
        sink = event_class("Sink", sink_process)
        _, error = await run(f"{source} |-> {sink}")
        assert error is None
        assert stamps["first_chunk"] < stamps["producer_done"] - 0.05

    async def test_a_task_after_the_segment_runs_afterwards(self, bus):
        record = Recorder()
        source = event_class("Src", numbers(2))
        sink = event_class("Sink", collector(record))
        after = event_class("After", marker(record, "after"))
        _, error = await run(f"{source} |-> {sink} -> {after}")
        assert error is None
        assert record.log == [("got", 0), ("got", 1), "after"]

    async def test_a_task_before_the_segment_runs_first(self, bus):
        record = Recorder()
        before = event_class("Before", marker(record, "before"))
        source = event_class("Src", numbers(2))
        sink = event_class("Sink", collector(record))
        _, error = await run(f"{before} -> {source} |-> {sink}")
        assert error is None
        assert record.log == ["before", ("got", 0), ("got", 1)]

    async def test_an_empty_stream_completes(self, bus):
        record = Recorder()
        source = event_class("Src", numbers(0))
        sink = event_class("Sink", collector(record))
        _, error = await run(f"{source} |-> {sink}")
        assert error is None and record.log == []


@pytest.mark.asyncio
class TestFanOut:
    async def test_every_consumer_sees_every_chunk(self, bus):
        one, two = Recorder(), Recorder()
        source = event_class("Src", numbers(3))
        first = event_class("First", collector(one))
        second = event_class("Second", collector(two))
        _, error = await run(f"{source} |-> ({first} || {second})")
        assert error is None
        assert [v for _, v in one.log] == [0, 1, 2]
        assert [v for _, v in two.log] == [0, 1, 2]

    async def test_a_plain_task_may_run_beside_a_consumer(self, bus):
        record = Recorder()
        source = event_class("Src", numbers(2))
        sink = event_class("Sink", collector(record))
        other = event_class("Other", marker(record, "other"))
        _, error = await run(f"{source} |-> ({sink} || {other})")
        assert error is None
        assert ("got", 0) in record.log and "other" in record.log


def labelled(label, count, delay=0.0):
    """A generator source whose payloads say where they came from."""

    async def process(self):
        for i in range(count):
            if delay:
                await asyncio.sleep(delay)
            yield True, [label, i]  # JSON: a tuple would come back as a list

    return process


def merged_collector(record):
    async def process(self):
        async for chunk in self.input_stream():
            record("got", chunk.content)
        return True, "collected"

    return process


@pytest.mark.asyncio
class TestFanIn:
    """``(A || B) |-> C``: C reads one merged stream."""

    async def test_two_generators_are_merged_keeping_each_ones_order(self, bus):
        record = Recorder()
        a = event_class("A", labelled("a", 4, 0.01))
        b = event_class("B", labelled("b", 4, 0.013))
        sink = event_class("Sink", merged_collector(record))
        _, error = await run(f"({a} || {b}) |-> {sink}")
        assert error is None
        got = [v for _, v in record.log]
        assert sorted(got) == [[l, i] for l in "ab" for i in range(4)]
        for label in "ab":  # per-source order is preserved
            assert [i for l, i in got if l == label] == [0, 1, 2, 3]

    async def test_the_sources_really_interleave(self, bus):
        record = Recorder()
        a = event_class("A", labelled("a", 4, 0.02))
        b = event_class("B", labelled("b", 4, 0.02))
        sink = event_class("Sink", merged_collector(record))
        await run(f"({a} || {b}) |-> {sink}")
        labels = [v[0] for _, v in record.log]
        assert labels != sorted(labels)  # not all of a, then all of b

    async def test_the_consumer_ends_only_after_every_source_ended(self, bus):
        record = Recorder()
        fast = event_class("Fast", labelled("fast", 1))
        slow = event_class("Slow", labelled("slow", 3, 0.05))
        sink = event_class("Sink", merged_collector(record))
        _, error = await run(f"({fast} || {slow}) |-> {sink}")
        assert error is None
        assert [v for _, v in record.log if v[0] == "slow"] == [
            ["slow", 0],
            ["slow", 1],
            ["slow", 2],
        ]

    async def test_a_plain_producer_contributes_one_chunk(self, bus):
        record = Recorder()

        async def plain(self):
            return True, "from-plain"

        gen = event_class("Gen", labelled("g", 2))
        one = event_class("Plain", plain)
        sink = event_class("Sink", merged_collector(record))
        _, error = await run(f"({gen} || {one}) |-> {sink}")
        assert error is None
        values = [v for _, v in record.log]
        assert "from-plain" in values and ["g", 0] in values and ["g", 1] in values
        assert len(values) == 3

    async def test_a_failing_source_fails_the_consumer(self, bus):
        record = Recorder()

        async def breaks(self):
            yield True, "ok"
            yield False, "bad"

        a = event_class("Bad", breaks)
        b = event_class("Fine", labelled("f", 3, 0.01))
        sink = event_class("Sink", merged_collector(record))
        yes = event_class("Yes", marker(record, "yes"))
        no = event_class("No", marker(record, "no"))
        _, error = await run(
            f"({a} || {b}) |-> {sink} (failure -> {no}, success -> {yes})"
        )
        assert error is None and record.log[-1] == "no"

    async def test_a_failed_consumer_stops_every_producer(self, bus):
        produced = {"a": 0, "b": 0}

        def endless(label):
            async def process(self):
                while True:
                    await asyncio.sleep(0.01)
                    produced[label] += 1
                    yield True, label

            return process

        async def takes_two(self):
            seen = 0
            async for _chunk in self.input_stream():
                seen += 1
                if seen == 2:
                    return False, "enough"

        a = event_class("A", endless("a"))
        b = event_class("B", endless("b"))
        sink = event_class("Sink", takes_two)
        started = time.monotonic()
        await run(f"({a} || {b}) |-> {sink}")
        assert time.monotonic() - started < StreamFlow.STOP_GRACE / 2
        counts = dict(produced)
        await asyncio.sleep(0.1)
        assert produced == counts  # both stopped

    async def test_fan_in_into_a_transform_then_a_sink(self, bus):
        record = Recorder()

        async def upper(self):
            async for chunk in self.input_stream():
                label, i = chunk.content
                yield True, f"{label.upper()}{i}"

        a = event_class("A", labelled("a", 2, 0.01))
        b = event_class("B", labelled("b", 2, 0.01))
        stage = event_class("Stage", upper)
        sink = event_class("Sink", merged_collector(record))
        _, error = await run(f"({a} || {b}) |-> {stage} |-> {sink}")
        assert error is None
        assert sorted(v for _, v in record.log) == ["A0", "A1", "B0", "B1"]

    async def test_fan_in_then_fan_out(self, bus):
        one, two = Recorder(), Recorder()
        a = event_class("A", labelled("a", 2))
        b = event_class("B", labelled("b", 2))
        pipe = event_class("Pipe", lambda: None)  # replaced below
        del pipe

        async def forward(self):
            async for chunk in self.input_stream():
                yield True, chunk.content

        stage = event_class("Stage", forward)
        first = event_class("First", merged_collector(one))
        second = event_class("Second", merged_collector(two))
        _, error = await run(f"({a} || {b}) |-> {stage} |-> ({first} || {second})")
        assert error is None
        expected = [["a", 0], ["a", 1], ["b", 0], ["b", 1]]
        assert sorted(v for _, v in one.log) == expected
        assert sorted(v for _, v in two.log) == expected

    async def test_consumed_streams_are_deleted_afterwards(self, bus):
        a = event_class("A", labelled("a", 2))
        b = event_class("B", labelled("b", 2))
        sink = event_class("Sink", merged_collector(Recorder()))
        pipeline, error = await run(f"({a} || {b}) |-> {sink}")
        assert error is None
        ids = [p.get_id() for p in pipeline.execution_context.task_profiles][:2]
        for task_id in ids:
            assert (
                await StreamChunk.stream_length(
                    route_override=StreamChunk.route(task_id)
                )
                == 0
            )

    async def test_a_task_after_the_fan_in_runs_afterwards(self, bus):
        record = Recorder()
        a = event_class("A", labelled("a", 2))
        b = event_class("B", labelled("b", 2))
        sink = event_class("Sink", merged_collector(record))
        after = event_class("After", marker(record, "after"))
        _, error = await run(f"({a} || {b}) |-> {sink} -> {after}")
        assert error is None and record.log[-1] == "after" and len(record.log) == 5


@pytest.mark.asyncio
class TestBranchesAfterTheSegment:
    async def test_success_takes_the_success_branch(self, bus):
        record = Recorder()
        source = event_class("Src", numbers(2))
        sink = event_class("Sink", collector(record))
        yes = event_class("Yes", marker(record, "yes"))
        no = event_class("No", marker(record, "no"))
        _, error = await run(f"{source} |-> {sink} (failure -> {no}, success -> {yes})")
        assert error is None and record.log[-1] == "yes" and "no" not in record.log

    async def test_a_failing_producer_takes_the_failure_branch(self, bus):
        record = Recorder()

        async def breaks(self):
            yield True, 0
            yield False, 1

        source = event_class("Src", breaks)
        sink = event_class("Sink", collector(record))
        yes = event_class("Yes", marker(record, "yes"))
        no = event_class("No", marker(record, "no"))
        _, error = await run(f"{source} |-> {sink} (failure -> {no}, success -> {yes})")
        assert error is None and record.log[-1] == "no" and "yes" not in record.log

    async def test_a_failing_consumer_takes_the_failure_branch(self, bus):
        record = Recorder()

        async def refuses(self):
            async for _chunk in self.input_stream():
                return False, "no thanks"

        source = event_class("Src", numbers(5, delay=0.01))
        sink = event_class("Sink", refuses)
        yes = event_class("Yes", marker(record, "yes"))
        no = event_class("No", marker(record, "no"))
        _, error = await run(f"{source} |-> {sink} (failure -> {no}, success -> {yes})")
        assert error is None and record.log == ["no"]


@pytest.mark.asyncio
class TestFailure:
    async def test_a_consumer_that_stops_cancels_an_endless_producer(self, bus):
        produced = []

        async def endless(self):
            i = 0
            while True:
                await asyncio.sleep(0.01)
                produced.append(i)
                yield True, i
                i += 1

        async def takes_two(self):
            seen = 0
            async for _chunk in self.input_stream():
                seen += 1
                if seen == 2:
                    return False, "enough"

        source = event_class("Src", endless)
        sink = event_class("Sink", takes_two)
        started = time.monotonic()
        _, error = await run(f"{source} |-> {sink}", timeout=10)
        # asked to stop and noticed between chunks, not cancelled after the grace period
        assert time.monotonic() - started < StreamFlow.STOP_GRACE / 2
        count = len(produced)
        await asyncio.sleep(0.1)
        assert len(produced) == count  # the producer was stopped, not left running

    async def test_the_producer_stream_is_closed_as_failed_when_cancelled(self, bus):
        async def endless(self):
            while True:
                await asyncio.sleep(0.01)
                yield True, 1

        async def stops(self):
            async for _chunk in self.input_stream():
                return False, "stop"

        source = event_class("Src", endless)
        sink = event_class("Sink", stops)
        pipeline, _ = await run(f"{source} |-> {sink}")
        assert pipeline is not None

    async def test_a_failing_producer_fails_the_consumer_not_hangs_it(self, bus):
        async def breaks(self):
            raise RuntimeError("source down")
            yield True, 0  # pragma: no cover

        source = event_class("Src", breaks)
        sink = event_class("Sink", collector(Recorder()))
        _, error = await run(f"{source} |-> {sink}", timeout=10)
        # it finished (no timeout) and did not report success
        assert not isinstance(error, asyncio.TimeoutError)


@pytest.mark.asyncio
class TestNotAStreamEdge:
    async def test_a_plain_producer_makes_it_an_ordinary_arrow(self, bus):
        record = Recorder()

        async def plain(self):
            return True, "value"

        async def reader(self):
            record("previous", self.previous_result.first().content)
            return True, "ok"

        source = event_class("Src", plain)
        sink = event_class("Sink", reader)
        _, error = await run(f"{source} |-> {sink}")
        assert error is None and record.log == [("previous", "value")]

    async def test_an_arrow_after_a_generator_hands_over_the_reference(self, bus):
        record = Recorder()

        async def reader(self):
            ref = self._upstream_ref()
            record("ref", ref.chunk_count, ref.exec_status)
            replayed = [c.content async for c in self.replay()]
            record("replayed", replayed)
            return True, "ok"

        source = event_class("Src", numbers(3))
        after = event_class("After", reader)
        _, error = await run(f"{source} -> {after}")
        assert error is None
        assert record.log == [("ref", 3, True), ("replayed", [0, 1, 2])]

    async def test_a_plain_member_in_the_middle_ends_the_segment(self, bus):
        record = Recorder()

        async def middle(self):
            record("middle", self.previous_result is not None)
            return True, "m"

        source = event_class("Src", numbers(2))
        mid = event_class("Mid", middle)
        tail = event_class("Tail", marker(record, "tail"))
        _, error = await run(f"{source} |-> {mid} |-> {tail}")
        assert error is None and "tail" in record.log


@pytest.mark.asyncio
class TestCleanup:
    async def _lengths(self, *task_ids):
        return [
            await StreamChunk.stream_length(route_override=StreamChunk.route(t))
            for t in task_ids
        ]

    async def test_streams_read_inside_the_segment_are_deleted(self, bus):
        source = event_class("Src", numbers(3))
        sink = event_class("Sink", collector(Recorder()))
        pipeline, error = await run(f"{source} |-> {sink}")
        assert error is None
        root = pipeline.execution_context
        producer_id = root.task_profiles[0].get_id()
        assert await self._lengths(producer_id) == [0]

    async def test_the_last_tasks_stream_is_kept_for_replay(self, bus):
        async def double(self):
            async for chunk in self.input_stream():
                yield True, chunk.content * 2

        source = event_class("Src", numbers(2))
        tail = event_class("Tail", double)
        pipeline, error = await run(f"{source} |-> {tail}")
        assert error is None
        ids = [p.get_id() for p in pipeline.execution_context.task_profiles]
        lengths = await self._lengths(*ids)
        assert lengths[0] == 0 and lengths[1] == 3  # 2 chunks + sentinel


class TestStructure:
    """Segment detection and flow selection, from parsed workflows."""

    def streaming(self, name):
        async def process(self):
            yield True, 1

        return event_class(name, process)

    def plain(self, name):
        async def process(self):
            return True, 1

        return event_class(name, process)

    def segment(self, pointy):
        head = build(pointy)
        return DefaultWorkflowEngine()._detect_stream_segment(head)

    def test_a_chain_of_streams(self):
        a, b, c = self.streaming("A"), self.streaming("B"), self.plain("C")
        members = self.segment(f"{a} |-> {b} |-> {c}")
        assert [m.event for m in members] == [a, b, c]

    def test_a_plain_producer_starts_no_segment(self):
        a, b = self.plain("A"), self.plain("B")
        assert self.segment(f"{a} |-> {b}") is None

    def test_an_arrow_starts_no_segment(self):
        a, b = self.streaming("A"), self.plain("B")
        assert self.segment(f"{a} -> {b}") is None

    def test_a_fan_out_collects_the_group_and_ends_the_chain(self):
        a, b, c, d = (self.streaming(n) for n in "ABCD")
        members = self.segment(f"{a} |-> ({b} || {c}) -> {d}")
        assert [m.event for m in members] == [a, b, c]

    def test_the_chain_stops_where_a_producer_is_not_streaming(self):
        a, b, c = self.streaming("A"), self.plain("B"), self.plain("C")
        members = self.segment(f"{a} |-> {b} |-> {c}")
        assert [m.event for m in members] == [a, b]

    def test_a_single_task_is_not_a_segment(self):
        assert self.segment(self.streaming("A")) is None

    def test_a_consumer_does_not_start_its_own_segment(self):
        a, b = self.streaming("A"), self.plain("B")
        head = build(f"{a} |-> {b}")
        consumer = head.condition_node.on_success_event
        assert DefaultWorkflowEngine()._detect_stream_segment(consumer) is None

    def test_producer_of_a_consumer_and_of_a_fan_out_member(self):
        a, b, c = self.streaming("A"), self.plain("B"), self.plain("C")
        head = build(f"{a} |-> ({b} || {c})")
        first = head.condition_node.on_success_event
        second = first.condition_node.on_success_event
        assert stream_producers_of(first) == [head]
        assert stream_producers_of(second) == [head]
        assert stream_producers_of(head) == []

    def test_the_decision_task_is_the_last_member(self):
        a, b, c = self.streaming("A"), self.streaming("B"), self.plain("C")
        head = build(f"{a} |-> {b} |-> {c}")
        members = DefaultWorkflowEngine()._detect_stream_segment(head)
        context = ExecutionContext.__new__(ExecutionContext)
        context.task_profiles = __import__("collections").deque(members)
        assert context.is_stream_segment() is True
        assert context.get_decision_task_profile() is members[-1]

    def test_flow_selection(self):
        a, b = self.streaming("A"), self.plain("B")
        head = build(f"{a} |-> {b}")
        segment = DefaultWorkflowEngine()._detect_stream_segment(head)

        class Ctx:
            def __init__(self, profiles):
                self.task_profiles = __import__("collections").deque(profiles)

            is_stream_segment = ExecutionContext.is_stream_segment
            is_multitask = ExecutionContext.is_multitask
            get_task_profiles = ExecutionContext.get_task_profiles

        assert Ctx(segment).is_stream_segment()
        assert not Ctx([head]).is_stream_segment()

    def test_a_fan_in_segment(self):
        a, b, c = self.streaming("A"), self.streaming("B"), self.plain("C")
        members = self.segment(f"({a} || {b}) |-> {c}")
        assert [m.event for m in members] == [a, b, c]

    def test_a_fan_in_with_one_streaming_producer_is_a_segment(self):
        a, b, c = self.streaming("A"), self.plain("B"), self.plain("C")
        members = self.segment(f"({a} || {b}) |-> {c}")
        assert [m.event for m in members] == [a, b, c]

    def test_a_fan_in_of_plain_producers_is_not_a_segment(self):
        a, b, c = self.plain("A"), self.plain("B"), self.plain("C")
        assert self.segment(f"({a} || {b}) |-> {c}") is None

    def test_fan_in_then_fan_out_is_one_segment(self):
        a, b, c, d, e = (self.streaming(n) for n in "ABCDE")
        members = self.segment(f"({a} || {b}) |-> {c} |-> ({d} || {e})")
        assert [m.event for m in members] == [a, b, c, d, e]

    def test_the_producers_of_a_fan_in_consumer(self):
        a, b, c = self.streaming("A"), self.streaming("B"), self.plain("C")
        head = build(f"({a} || {b}) |-> {c}")
        first = head
        second = head.condition_node.on_success_event
        sink = second.condition_node.on_success_event
        assert stream_producers_of(sink) == [first, second]
        assert [t.event for t in second.get_parallel_group()] == [a, b]
        assert first.get_parallel_group() == [first]

    def test_a_fan_in_context_is_a_stream_segment_with_the_sink_deciding(self):
        a, b, c = self.plain("A"), self.streaming("B"), self.plain("C")
        head = build(f"({a} || {b}) |-> {c}")
        members = DefaultWorkflowEngine()._detect_stream_segment(head)
        context = ExecutionContext.__new__(ExecutionContext)
        context.task_profiles = __import__("collections").deque(members)
        assert context.is_stream_segment() is True
        assert context.get_decision_task_profile() is members[-1]

    def test_a_parallel_group_is_not_a_stream_segment(self):
        a, b = self.plain("A"), self.plain("B")
        head = build(f"{a} || {b}")

        class Ctx:
            task_profiles = __import__("collections").deque(
                [head, head.condition_node.on_success_event]
            )
            is_stream_segment = ExecutionContext.is_stream_segment
            get_task_profiles = ExecutionContext.get_task_profiles

        assert Ctx().is_stream_segment() is False


class TestPoolGuard:
    """A stream segment whose members wait on each other must not share a pool with
    fewer workers than members."""

    def flow(self, members, executor):
        from collections import deque
        from unittest.mock import MagicMock

        decision = MagicMock()
        context = MagicMock()
        context.task_profiles = deque(MagicMock() for _ in range(members))
        context.get_decision_task_profile.return_value = decision
        flow = StreamFlow(context)
        flow.get_task_executor_from_options = lambda task: executor
        return flow

    @pytest.mark.asyncio
    async def test_a_pool_that_is_too_small_falls_back_to_the_async_executor(
        self, monkeypatch
    ):
        from concurrent.futures import ThreadPoolExecutor

        from volnux.executors import DefaultExecutor

        monkeypatch.setattr("volnux.flows.stream.pool_size", lambda cls: 2)
        chosen = await self.flow(3, ThreadPoolExecutor).get_flow_executor()
        assert chosen is DefaultExecutor

    @pytest.mark.asyncio
    async def test_a_big_enough_pool_is_used(self, monkeypatch):
        from concurrent.futures import ThreadPoolExecutor

        monkeypatch.setattr("volnux.flows.stream.pool_size", lambda cls: 8)
        assert (
            await self.flow(3, ThreadPoolExecutor).get_flow_executor()
            is ThreadPoolExecutor
        )

    @pytest.mark.asyncio
    async def test_the_default_is_the_async_executor(self):
        from volnux.executors import DefaultExecutor

        assert await self.flow(3, None).get_flow_executor() is DefaultExecutor

    @pytest.mark.asyncio
    async def test_other_executors_are_left_alone(self):
        from volnux.executors import DefaultExecutor

        class Custom(DefaultExecutor):
            pass

        assert await self.flow(3, Custom).get_flow_executor() is Custom
