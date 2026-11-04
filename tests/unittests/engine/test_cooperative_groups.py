"""``{ ... }``: the cooperative execution paradigm.

A brace group holds one or more chains separated by commas. Every chain runs
concurrently (one sub-engine each, as async tasks on the engine's loop); a chain may be
a task or a whole subgraph. ``||`` is the parallel paradigm and takes tasks only.
"""

import asyncio
import itertools
import time

import pytest

from volnux import EventBase, Pipeline
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins.stream.queue import StreamChunk
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.grammar import pointy_parser
from volnux.parser.protocols import GroupingStrategy
from volnux.parser.sema import Sema
from volnux.task import PipelineTask, PipelineTaskGrouping

_ids = itertools.count()
# Event names are registered globally and resolved by name from Pointy source, so
# every test module needs names of its own.
TAG = "Coop"


def event_class(name, process):
    return type(f"{name}{TAG}{next(_ids)}", (EventBase,), {"process": process}).__name__


@pytest.fixture
def bus():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="cooperative")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


async def run(pointy, timeout=10):
    pipeline_class = type(
        f"Pipe{TAG}{next(_ids)}",
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": pointy})},
    )
    pipeline = pipeline_class()
    try:
        await asyncio.wait_for(pipeline.start(force_rerun=True), timeout)
        return pipeline, None
    except Exception as exc:
        return pipeline, exc


def build(pointy):
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(pointy_parser(pointy))
    return generator.generate()


def errors(pointy):
    return [d.message for d in Sema().analyse(pointy_parser(pointy)).errors]


def step(record, name, delay=0.0, ok=True):
    async def process(self):
        record.append(("start", name, time.monotonic()))
        if delay:
            await asyncio.sleep(delay)
        record.append(("end", name, time.monotonic()))
        return ok, name

    return process


class TestSyntax:
    def test_several_chains_in_braces(self):
        node = pointy_parser("{A, B -> C, D -> Z |-> M}").chain
        assert len(node.expressions) == 3

    def test_one_chain_in_braces_is_unchanged(self):
        node = pointy_parser("{A -> B}").chain
        assert len(node.expressions) == 1

    def test_attributes_after_the_brace(self):
        program = pointy_parser("{A, B}[retry_attempts = 2] -> C")
        assert program.chain.left.options

    def test_braces_nest(self):
        node = pointy_parser("{ {A, B}, C }").chain
        assert len(node.expressions) == 2

    def test_braces_inside_a_branch(self):
        assert errors("A (failure -> {B, C}, success -> D)") == []

    def test_chains_may_have_branches(self):
        assert errors("{A (failure -> X, success -> Y), B}") == []

    def test_a_trailing_or_empty_item_is_a_syntax_error(self):
        for source in ["{A, }", "{, A}", "{A,, B}", "{}"]:
            with pytest.raises(Exception):
                pointy_parser(source)

    def test_streams_inside_a_member_are_fine(self):
        assert errors("{A, B -> C, D -> Z |-> M}") == []

    def test_parallel_takes_tasks_but_a_bare_chain_is_not_one(self):
        assert errors("A || B || C") == []
        with pytest.raises(Exception):
            build("A || (B -> C) || D")

    def test_parallel_may_sit_beside_a_cooperative_group(self):
        assert build("A || {B -> C}") is not None


class TestStructure:
    def test_several_chains_make_a_multipath_group(self):
        group = build("{A, B -> C, D}")
        assert isinstance(group, PipelineTaskGrouping)
        assert group.strategy == GroupingStrategy.MULTIPATH_CHAINS
        assert [c.event for c in group.chains] == ["A", "B", "D"]

    def test_one_chain_is_a_single_chain_group(self):
        group = build("{A -> B}")
        assert group.strategy == GroupingStrategy.SINGLE_CHAIN
        assert len(group.chains) == 1

    def test_a_group_continues_like_any_task(self):
        group = build("{A, B} -> C")
        assert group.condition_node.on_success_event.event == "C"

    def test_nested_groups(self):
        group = build("{ {A, B}, C }")
        inner, other = group.chains
        assert isinstance(inner, PipelineTaskGrouping) and other.event == "C"


@pytest.mark.asyncio
class TestExecution:
    async def test_every_chain_runs_concurrently(self, bus):
        record = []
        a = event_class("A", step(record, "a", 0.2))
        b = event_class("B", step(record, "b", 0.2))
        c = event_class("C", step(record, "c", 0.2))
        started = time.monotonic()
        _, error = await run(f"{{{a}, {b}, {c}}}")
        elapsed = time.monotonic() - started
        assert error is None
        assert {n for kind, n, _ in record if kind == "end"} == {"a", "b", "c"}
        assert elapsed < 0.5  # three 0.2s chains overlapped (sequential would be 0.6)

    async def test_a_chain_may_be_a_subgraph(self, bus):
        record = []
        a = event_class("A", step(record, "a"))
        b = event_class("B", step(record, "b"))
        c = event_class("C", step(record, "c"))
        d = event_class("D", step(record, "d"))
        _, error = await run(f"{{{a}, {b} -> {c} -> {d}}}")
        assert error is None
        order = [n for kind, n, _ in record if kind == "start"]
        assert order.index("b") < order.index("c") < order.index("d")
        assert set(order) == {"a", "b", "c", "d"}

    async def test_the_workflow_continues_after_the_group(self, bus):
        record = []
        a = event_class("A", step(record, "a", 0.05))
        b = event_class("B", step(record, "b", 0.1))
        after = event_class("After", step(record, "after"))
        _, error = await run(f"{{{a}, {b}}} -> {after}")
        assert error is None
        names = [n for kind, n, _ in record if kind == "end"]
        assert names[-1] == "after" and set(names[:2]) == {"a", "b"}

    async def test_every_chain_succeeding_takes_the_success_branch(self, bus):
        record = []
        a = event_class("A", step(record, "a"))
        b = event_class("B", step(record, "b"))
        yes = event_class("Yes", step(record, "yes"))
        no = event_class("No", step(record, "no"))
        _, error = await run(f"{{{a}, {b}}} (failure -> {no}, success -> {yes})")
        assert error is None
        ran = {n for kind, n, _ in record if kind == "end"}
        assert "yes" in ran and "no" not in ran

    async def test_any_failing_chain_takes_the_failure_branch(self, bus):
        record = []
        a = event_class("A", step(record, "a"))
        b = event_class("B", step(record, "b", ok=False))
        yes = event_class("Yes", step(record, "yes"))
        no = event_class("No", step(record, "no"))
        _, error = await run(f"{{{a}, {b}}} (failure -> {no}, success -> {yes})")
        ran = {n for kind, n, _ in record if kind == "end"}
        assert {"a", "b", "no"} <= ran and "yes" not in ran

    async def test_a_stream_segment_runs_inside_a_member(self, bus):
        seen = []

        async def source(self):
            for i in range(3):
                await asyncio.sleep(0.01)
                yield True, i

        async def sink(self):
            async for chunk in self.input_stream():
                seen.append(chunk.content)
            return True, "done"

        record = []
        s = event_class("Src", source)
        k = event_class("Snk", sink)
        o = event_class("Other", step(record, "other"))
        _, error = await run(f"{{{s} |-> {k}, {o}}}")
        assert error is None
        assert seen == [0, 1, 2] and ("end", "other") in [(a, b) for a, b, _ in record]

    async def test_nested_groups_run(self, bus):
        record = []
        names = {n: event_class(n.upper(), step(record, n)) for n in "abc"}
        _, error = await run(f"{{ {{{names['a']}, {names['b']}}}, {names['c']} }}")
        assert error is None
        assert {n for kind, n, _ in record if kind == "end"} == {"a", "b", "c"}

    async def test_a_single_chain_group_runs_like_a_plain_chain(self, bus):
        record = []
        a = event_class("A", step(record, "a"))
        b = event_class("B", step(record, "b"))
        _, error = await run(f"{{{a} -> {b}}}")
        assert error is None
        assert [n for kind, n, _ in record if kind == "start"] == ["a", "b"]


def streamer(label, count, delay=0.0):
    async def process(self):
        for i in range(count):
            if delay:
                await asyncio.sleep(delay)
            yield True, [label, i]

    return process


def reader(record, tag="got"):
    async def process(self):
        async for chunk in self.input_stream():
            record.append((tag, chunk.content))
        return True, "read"

    return process


class TestStructureOfGroupStreams:
    def _tasks(self, pointy):
        return build(pointy)

    def test_chain_tails_of_a_group(self):
        group = self._tasks("{A, B -> C, D}")
        tails = [t.event for chain in group.chains for t in chain.get_chain_tails()]
        assert tails == ["A", "C", "D"]

    def test_a_chain_ending_in_a_parallel_group_has_all_its_members_as_tails(self):
        group = self._tasks("{X -> (A || B), C}")
        tails = [t.event for chain in group.chains for t in chain.get_chain_tails()]
        assert tails == ["A", "B", "C"]

    def test_nested_groups_contribute_their_tails(self):
        group = self._tasks("{ {A, B -> C}, D }")
        tails = [t.event for chain in group.chains for t in chain.get_chain_tails()]
        assert tails == ["A", "C", "D"]

    def test_the_consumer_of_a_group_reads_every_chains_tail(self):
        group = self._tasks("{A, B -> C} |-> D")
        consumer = group.condition_node.on_success_event
        assert [t.event for t in consumer.get_stream_producers()] == ["A", "C"]

    def test_a_task_that_reads_nothing_has_no_producers(self):
        group = self._tasks("{A, B} -> D")
        assert group.condition_node.on_success_event.get_stream_producers() == []

    def test_branches_on_a_chain_tail_are_refused_by_semantic_analysis(self):
        found = errors("{A, B (failure -> X, success -> Y)} |-> D")
        assert any("has branches" in m for m in found), found

    def test_a_group_feeding_a_consumer_is_valid(self):
        assert errors("{A, B -> C} |-> D") == []
        assert errors("{A, B -> C} |-> D |-> E") == []
        assert errors("{A, B} |-> (D || E)") == []


@pytest.mark.asyncio
class TestGroupAsProducer:
    async def test_the_consumer_reads_every_chains_tail(self, bus):
        record = []

        async def plain(self):
            return True, "from-plain"

        a = event_class("A", streamer("a", 3, 0.01))
        b = event_class("B", plain)
        c = event_class("C", step([], "c"))  # a plain tail after B
        d = event_class("D", reader(record))
        _, error = await run(f"{{{a}, {b} -> {c}}} |-> {d}")
        assert error is None
        values = [v for _, v in record]
        assert ["a", 0] in values and ["a", 1] in values and ["a", 2] in values
        assert "c" in values  # the plain tail's result, as one chunk
        assert len(values) == 4

    async def test_two_streaming_chains_are_merged(self, bus):
        record = []
        a = event_class("A", streamer("a", 3, 0.01))
        b = event_class("B", streamer("b", 3, 0.013))
        d = event_class("D", reader(record))
        _, error = await run(f"{{{a}, {b}}} |-> {d}")
        assert error is None
        got = [v for _, v in record]
        for label in "ab":
            assert [i for l, i in got if l == label] == [0, 1, 2]

    async def test_the_consumer_starts_while_the_chains_still_run(self, bus):
        stamps = {}

        async def slow(self):
            for i in range(4):
                await asyncio.sleep(0.05)
                yield True, i
            stamps["chains_done"] = time.monotonic()

        async def first_chunk(self):
            async for _chunk in self.input_stream():
                stamps.setdefault("first", time.monotonic())
            return True, "done"

        a = event_class("A", slow)
        d = event_class("D", first_chunk)
        _, error = await run(f"{{{a}}} |-> {d}")
        assert error is None
        assert stamps["first"] < stamps["chains_done"] - 0.05

    async def test_a_failing_chain_fails_the_consumer(self, bus):
        record = []

        async def breaks(self):
            yield True, "ok"
            yield False, "bad"

        a = event_class("A", breaks)
        b = event_class("B", streamer("b", 2))
        d = event_class("D", reader(record))
        yes = event_class("Yes", step(record, "yes"))
        no = event_class("No", step(record, "no"))
        _, error = await run(
            f"{{{a}, {b}}} |-> {d} (failure -> {no}, success -> {yes})"
        )
        assert error is None
        ran = [r for r in record if isinstance(r, tuple) and r[0] in ("end",)]
        names = {r[1] for r in ran}
        assert "no" in names and "yes" not in names

    async def test_a_failed_consumer_stops_producers_in_other_sub_engines(self, bus):
        produced = {"n": 0}

        async def endless(self):
            while True:
                await asyncio.sleep(0.01)
                produced["n"] += 1
                yield True, 1

        async def takes_two(self):
            seen = 0
            async for _chunk in self.input_stream():
                seen += 1
                if seen == 2:
                    return False, "enough"

        a = event_class("A", endless)
        d = event_class("D", takes_two)
        started = time.monotonic()
        await run(f"{{{a}, {a}}} |-> {d}")
        assert time.monotonic() - started < 5
        count = produced["n"]
        await asyncio.sleep(0.1)
        assert produced["n"] == count

    async def test_the_workflow_continues_after_the_consumer(self, bus):
        record = []
        a = event_class("A", streamer("a", 2))
        d = event_class("D", reader(record))
        after = event_class("After", step(record, "after"))
        _, error = await run(f"{{{a}}} |-> {d} -> {after}")
        assert error is None
        assert [r for r in record if r[0] == "end"][-1][1] == "after"

    async def test_a_consumer_group_fans_out(self, bus):
        one, two = [], []
        a = event_class("A", streamer("a", 2))
        d = event_class("D", reader(one))
        e = event_class("E", reader(two))
        _, error = await run(f"{{{a}}} |-> ({d} || {e})")
        assert error is None
        assert len(one) == 2 and len(two) == 2

    async def test_the_consumer_may_stream_on(self, bus):
        record = []

        async def forward(self):
            async for chunk in self.input_stream():
                yield True, chunk.content

        a = event_class("A", streamer("a", 3))
        d = event_class("D", forward)
        e = event_class("E", reader(record))
        _, error = await run(f"{{{a}}} |-> {d} |-> {e}")
        assert error is None
        assert [v for _, v in record] == [["a", 0], ["a", 1], ["a", 2]]

    async def test_a_group_of_plain_tasks_followed_by_a_stream_edge_is_an_arrow(
        self, bus
    ):
        record = []

        async def plain(self):
            return True, "p"

        async def consumer(self):
            record.append("ran")
            return True, "d"

        a = event_class("A", plain)
        d = event_class("D", consumer)
        _, error = await run(f"{{{a}, {a}}} |-> {d}")
        assert error is None and record == ["ran"]

    async def test_a_group_first_in_the_workflow_can_feed_a_consumer(self, bus):
        record = []
        a = event_class("A", streamer("a", 1))
        d = event_class("D", reader(record))
        _, error = await run(f"{{{a}}} |-> {d}")
        assert error is None and record == [("got", ["a", 0])]

    async def test_a_group_after_a_task_can_feed_a_consumer(self, bus):
        record = []
        before = event_class("Before", step(record, "before"))
        a = event_class("A", streamer("a", 1))
        d = event_class("D", reader(record))
        _, error = await run(f"{before} -> {{{a}}} |-> {d}")
        assert error is None
        assert record[0][:2] == ("start", "before") and ("got", ["a", 0]) in record


def gauge(record, delay=0.05):
    """A step that records how many steps are running at the same moment."""

    async def process(self):
        record["now"] += 1
        record["peak"] = max(record["peak"], record["now"])
        await asyncio.sleep(delay)
        record["now"] -= 1
        record["done"] += 1
        return True, "ok"

    return process


def six_chains(record):
    names = [event_class(f"W{i}", gauge(record)) for i in range(6)]
    return ", ".join(names)


@pytest.mark.asyncio
class TestResourceLimits:
    async def test_max_concurrency_limits_the_chains_running_at_once(self, bus):
        record = {"now": 0, "peak": 0, "done": 0}
        _, error = await run(f"{{{six_chains(record)}}}[max_concurrency = 2]")
        assert error is None
        assert record["peak"] == 2 and record["done"] == 6  # none dropped

    async def test_one_runs_the_chains_one_at_a_time(self, bus):
        record = {"now": 0, "peak": 0, "done": 0}
        await run(f"{{{six_chains(record)}}}[max_concurrency = 1]")
        assert record["peak"] == 1 and record["done"] == 6

    async def test_the_configured_default_applies_without_the_option(
        self, bus, monkeypatch
    ):
        from volnux.engine.default_engine import DefaultWorkflowEngine

        monkeypatch.setattr(
            DefaultWorkflowEngine, "_cooperative_limit", staticmethod(lambda: 3)
        )
        record = {"now": 0, "peak": 0, "done": 0}
        _, error = await run(f"{{{six_chains(record)}}}")
        assert error is None and record["peak"] == 3 and record["done"] == 6

    async def test_zero_means_no_limit(self, bus, monkeypatch):
        from volnux.engine.default_engine import DefaultWorkflowEngine

        monkeypatch.setattr(
            DefaultWorkflowEngine, "_cooperative_limit", staticmethod(lambda: 0)
        )
        record = {"now": 0, "peak": 0, "done": 0}
        await run(f"{{{six_chains(record)}}}")
        assert record["peak"] == 6

    async def test_the_option_beats_the_default(self, bus, monkeypatch):
        from volnux.engine.default_engine import DefaultWorkflowEngine

        monkeypatch.setattr(
            DefaultWorkflowEngine, "_cooperative_limit", staticmethod(lambda: 1)
        )
        record = {"now": 0, "peak": 0, "done": 0}
        await run(f"{{{six_chains(record)}}}[max_concurrency = 4]")
        assert record["peak"] == 4

    async def test_nested_groups_each_keep_to_their_own_limit(self, bus):
        record = {"now": 0, "peak": 0, "done": 0}
        inner_a = ", ".join(event_class(f"I{i}", gauge(record)) for i in range(3))
        inner_b = ", ".join(event_class(f"J{i}", gauge(record)) for i in range(3))
        _, error = await run(
            f"{{ {{{inner_a}}}[max_concurrency = 1], {{{inner_b}}}[max_concurrency = 1] }}"
        )
        assert error is None and record["done"] == 6 and record["peak"] == 2

    async def test_a_stream_segment_inside_a_limited_group_still_completes(self, bus):
        seen = []

        async def source(self):
            for i in range(3):
                yield True, i

        async def sink(self):
            async for chunk in self.input_stream():
                seen.append(chunk.content)
            return True, "done"

        s = event_class("Src", source)
        k = event_class("Snk", sink)
        o = event_class("Oth", gauge({"now": 0, "peak": 0, "done": 0}))
        _, error = await run(f"{{{s} |-> {k}, {o}}}[max_concurrency = 1]")
        assert error is None and seen == [0, 1, 2]


class TestOptionValidation:
    def test_valid_on_a_group(self):
        assert errors("{A, B}[max_concurrency = 4]") == []

    @pytest.mark.parametrize("value", [0, -1])
    def test_must_be_at_least_one(self, value):
        found = errors(f"{{A, B}}[max_concurrency = {value}]")
        assert any("max_concurrency" in m and ">= 1" in m for m in found), found

    def test_a_task_gets_a_warning_it_has_no_effect(self):
        result = Sema().analyse(pointy_parser("A[max_concurrency = 2]"))
        assert result.errors == []
        assert any("no effect" in d.message for d in result.warnings)

    def test_the_wrong_type_is_reported(self):
        result = Sema().analyse(pointy_parser('{A, B}[max_concurrency = "many"]'))
        assert any(
            "max_concurrency" in d.message for d in result.warnings + result.errors
        )

    def test_the_option_reaches_the_group(self):
        group = build("{A, B}[max_concurrency = 3]")
        assert group.options.max_concurrency == 3
        assert build("{A, B}").options is None
