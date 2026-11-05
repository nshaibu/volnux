"""Fixtures for the meta event tests: an in-memory stream bus and a runner."""

import itertools
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins.stream.queue import StreamChunk
from volnux.parser.options import Options

_names = itertools.count()


@pytest.fixture
def bus():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="meta-tests")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


async def stream_of(stream_id: str) -> List[StreamChunk]:
    route = StreamChunk.route(stream_id)
    await StreamChunk.stream_ensure_consumer_group("probe", route_override=route)
    entries = await StreamChunk.stream_read(
        "probe", count=100000, block_ms=None, route_override=route
    )
    return [e.record for e in entries]


@pytest.fixture
def run_meta(bus):
    """``await run_meta(MapEvent, template, collection=[...], **options)`` -> the
    items the event wrote to its stream, plus the event and its result.

    ``template`` is a function, or an event class."""

    async def run(
        meta_class: type,
        template: Any,
        *,
        collection: Optional[list] = None,
        previous: Any = None,
        configure: Optional[Callable[[Any], None]] = None,
        **options: Any,
    ):
        extras: Dict[str, Any] = dict(options)
        if collection is not None:
            extras["collection"] = collection
        cls = type(
            "%sTest%d" % (meta_class.__name__, next(_names)),
            (meta_class,),
            {
                "template_class": (
                    staticmethod(template)
                    if not isinstance(template, type)
                    else template
                )
            },
        )
        kwargs = {}
        if previous is not None:
            kwargs["previous_result"] = previous
        event = cls(
            execution_context=MagicMock(),
            task_id="meta-%d" % next(_names),
            options=Options(extras=extras),
            **kwargs,
        )
        if configure:
            configure(event)
        result = await event()
        chunks = await stream_of(event.stream_id)
        items = [c.content for c in chunks if not c.is_sentinel]
        return items, event, result

    return run
