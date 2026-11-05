"""Meta events inside Pointy workflows: ``Source |-> MAP<F> |-> FILTER<G> |-> Sink``."""

import asyncio
import itertools

import pytest

from volnux import EventBase, Pipeline

_ids = itertools.count()
TAG = "MetaPointy"


def event_class(name, process):
    return type(f"{name}{TAG}{next(_ids)}", (EventBase,), {"process": process}).__name__


async def run(pointy, timeout=15):
    pipeline_class = type(
        f"Pipe{TAG}{next(_ids)}",
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": pointy})},
    )
    pipeline = pipeline_class()
    try:
        await asyncio.wait_for(pipeline.start(force_rerun=True), timeout)
        return None
    except Exception as exc:
        return exc


def numbers(count):
    async def process(self):
        for i in range(count):
            yield True, i

    return process


def collector(seen):
    async def process(self):
        async for chunk in self.input_stream():
            seen.append(chunk.content)
        return True, "collected"

    return process


async def double(self, *args, **kwargs):
    return True, self.previous_result[0].content * 2


async def big(self, *args, **kwargs):
    return True, self.previous_result[0].content > 4


@pytest.mark.asyncio
async def test_map_between_a_source_and_a_sink(bus):
    seen = []
    source = event_class("Src", numbers(5))
    dbl = event_class("Dbl", double)
    sink = event_class("Sink", collector(seen))

    error = await run(f"{source} |-> MAP<{dbl}> |-> {sink}")

    assert error is None and seen == [0, 2, 4, 6, 8]


@pytest.mark.asyncio
async def test_map_then_filter_in_a_chain(bus):
    seen = []
    source = event_class("Src", numbers(6))
    dbl = event_class("Dbl", double)
    gt = event_class("Big", big)
    sink = event_class("Sink", collector(seen))

    error = await run(f"{source} |-> MAP<{dbl}> |-> FILTER<{gt}> |-> {sink}")

    assert error is None and seen == [6, 8, 10]


@pytest.mark.asyncio
async def test_a_plain_task_feeds_a_map_by_previous_result(bus):
    seen = []

    async def listing(self):
        return True, [1, 2, 3]

    source = event_class("List", listing)
    dbl = event_class("Dbl", double)
    sink = event_class("Sink", collector(seen))

    error = await run(f"{source} -> MAP<{dbl}> -> {sink}")

    assert error is None and seen == [2, 4, 6]


@pytest.mark.asyncio
async def test_a_failing_template_takes_the_failure_branch(bus):
    async def boom(self, *args, **kwargs):
        raise ValueError("bad")

    seen, branch = [], []

    def marker(name):
        async def process(self):
            branch.append(name)
            return True, name

        return process

    source = event_class("Src", numbers(3))
    bad = event_class("Boom", boom)
    sink = event_class("Sink", collector(seen))
    no = event_class("No", marker("failure"))
    yes = event_class("Yes", marker("success"))

    await run(f"{source} |-> MAP<{bad}> |-> {sink} (failure -> {no}, success -> {yes})")

    assert branch == ["failure"]
