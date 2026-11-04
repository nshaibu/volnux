"""A ``|->`` segment over a real Redis stream.

Skipped unless Redis answers at ``VOLNUX_TEST_REDIS_HOST:VOLNUX_TEST_REDIS_PORT``
(default 127.0.0.1:6379). The same checks run on the in-memory backend in
``test_stream_segments.py``; this one proves the Redis stream capability carries
the segment (consumer groups, ack, blocking reads, delete).
"""

import asyncio
import itertools
import os
import uuid

import pytest

from volnux import EventBase, Pipeline
from volnux.mixins.stream.queue import StreamChunk

HOST = os.environ.get("VOLNUX_TEST_REDIS_HOST", "127.0.0.1")
PORT = int(os.environ.get("VOLNUX_TEST_REDIS_PORT", "6379"))
_ids = itertools.count()


@pytest.fixture
def redis_streams():
    from volnux.backends.stores.redis import RedisStoreBackend

    try:
        backend = RedisStoreBackend(
            host=HOST,
            port=PORT,
            socket_connect_timeout=1,
            namespace_prefix=f"stream-test-{uuid.uuid4().hex[:8]}",
        )
        backend.connector.get_cursor().ping()
    except Exception as exc:
        pytest.skip(f"Redis not reachable at {HOST}:{PORT}: {exc}")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


def event_class(name, process):
    return type(f"{name}{next(_ids)}", (EventBase,), {"process": process}).__name__


@pytest.mark.asyncio
async def test_a_three_stage_stream_over_redis(redis_streams):
    seen = []

    async def source(self):
        for i in range(5):
            await asyncio.sleep(0.01)
            yield True, i

    async def double(self):
        async for chunk in self.input_stream():
            yield True, chunk.content * 2

    async def sink(self):
        async for chunk in self.input_stream():
            seen.append(chunk.content)
        return True, "done"

    a = event_class("Src", source)
    b = event_class("Dbl", double)
    c = event_class("Snk", sink)
    pipeline_class = type(
        "Pipe" + uuid.uuid4().hex[:8],
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": f"{a} |-> {b} |-> {c}"})},
    )
    await asyncio.wait_for(pipeline_class().start(force_rerun=True), 30)
    assert seen == [0, 2, 4, 6, 8]
