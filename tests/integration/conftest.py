"""Integration fixtures: every backend in memory, and global state put back."""

import pytest

from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.executors import shared
from volnux.mixins.stream.queue import StreamChunk


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/integration" in str(item.fspath).replace("\\", "/"):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(autouse=True)
def bus():
    """Streams (``|->``) on an in-memory backend."""
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="integration")
    previous = (StreamChunk._backend_store, StreamChunk._backend_initialized)
    StreamChunk._backend_store, StreamChunk._backend_initialized = backend, True
    yield backend
    StreamChunk._backend_store, StreamChunk._backend_initialized = previous
    backend.close()


@pytest.fixture(autouse=True)
def fresh_pools():
    """The shared thread/process pools belong to the test that created them."""
    yield
    shared.shutdown_shared_pools()
