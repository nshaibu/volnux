"""Helpers for the integration tests: real Pipelines from inline Pointy, nothing mocked."""

import asyncio
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional

from volnux import EventBase, Pipeline


def unique(name: str) -> str:
    return f"{name}{uuid.uuid4().hex[:8]}"


def make_event(name: str, process: Callable, executor: str = "default", **attrs: Any):
    """An event class with a unique name (the event registry is global, by name)."""
    return type(
        unique(name), (EventBase,), {"process": process, "executor": executor, **attrs}
    )


def make_pipeline(pointy: str):
    return type(
        unique("Pipe"), (Pipeline,), {"Meta": type("Meta", (), {"pointy": pointy})}
    )


async def run(pointy: str, timeout: float = 20.0):
    """Run ``pointy``; returns ``(pipeline, error or None)``."""
    pipeline = make_pipeline(pointy)()
    try:
        await asyncio.wait_for(pipeline.start(force_rerun=True), timeout)
        return pipeline, None
    except Exception as exc:  # a failed run raises out of start()
        return pipeline, exc


def contexts(pipeline) -> list:
    """Every context of the chain, in execution order."""
    return list(pipeline.execution_context)


def results_of(pipeline) -> List[Any]:
    out: List[Any] = []
    for context in contexts(pipeline):
        out.extend(context.get_results())
    return out


def contents(pipeline) -> List[Any]:
    """Result contents of the last context."""
    return [
        r.content for r in pipeline.execution_context.get_latest_context().get_results()
    ]


class Trace:
    """Order of events across threads: ``trace('A')`` from any worker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.log: List[Any] = []

    def __call__(self, item: Any) -> None:
        with self._lock:
            self.log.append(item)

    def count(self, item: Any) -> int:
        return self.log.count(item)


def step(
    trace: Trace, name: str, ok: bool = True, delay: float = 0.0, content: Any = None
):
    """A ``process`` that records itself, then succeeds or fails."""

    async def process(self, *args: Any, **kwargs: Any):
        trace(("start", name))
        if delay:
            await asyncio.sleep(delay)
        trace(("end", name))
        return ok, name if content is None else content

    return process
