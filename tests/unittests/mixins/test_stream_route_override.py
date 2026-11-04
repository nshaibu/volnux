"""``route_override``: one model, one stream per entity."""

import pytest
from formax import BaseModel, InitStrategy

from volnux.backends.storage_route import StorageRoute
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

pytestmark = pytest.mark.asyncio


class Tick(MessagingBackendIntegrationMixin, KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    n: int

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return StorageRoute(
            components=["ticks", "{stream_id}"],
            routing_keys=lambda: {"stream_id": "default"},
        )


@pytest.fixture
def bound():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="route-test")
    previous = (Tick._backend_store, Tick._backend_initialized)
    Tick._backend_store, Tick._backend_initialized = backend, True
    yield backend
    Tick._backend_store, Tick._backend_initialized = previous


def over(stream_id):
    return {"stream_id": stream_id}


async def drain(group, stream_id=None):
    entries = await Tick.stream_read(
        group,
        count=50,
        block_ms=None,
        route_override=over(stream_id) if stream_id else None,
    )
    return [e.record.n for e in entries]


async def test_the_default_route_is_used_without_an_override(bound):
    await Tick.stream_append(Tick(n=1))
    assert await Tick.stream_length() == 1
    assert await Tick.stream_length(route_override=over("other")) == 0


async def test_each_override_is_its_own_stream(bound):
    await Tick.stream_append(Tick(n=1), route_override=over("a"))
    await Tick.stream_append(Tick(n=2), route_override=over("a"))
    await Tick.stream_append(Tick(n=9), route_override=over("b"))
    assert await Tick.stream_length(route_override=over("a")) == 2
    assert await Tick.stream_length(route_override=over("b")) == 1
    assert await Tick.stream_length() == 0


async def test_groups_read_the_stream_they_name(bound):
    await Tick.stream_append(Tick(n=1), route_override=over("a"))
    await Tick.stream_append(Tick(n=2), route_override=over("b"))
    await Tick.stream_ensure_consumer_group("g", route_override=over("a"))
    await Tick.stream_ensure_consumer_group("g", route_override=over("b"))
    assert await drain("g", "a") == [1]
    assert await drain("g", "b") == [2]


async def test_ack_claim_trim_and_delete_follow_the_override(bound):
    for n in range(4):
        await Tick.stream_append(Tick(n=n), route_override=over("a"))
    await Tick.stream_ensure_consumer_group("g", route_override=over("a"))
    entries = await Tick.stream_read(
        "g", count=2, block_ms=None, route_override=over("a")
    )
    ids = [e.entry_id for e in entries]
    assert await Tick.stream_ack("g", *ids, route_override=over("a")) == 2
    assert await Tick.stream_trim(1, route_override=over("a")) == 3
    assert await Tick.stream_length(route_override=over("a")) == 1
    assert await Tick.stream_delete(route_override=over("a")) is True
    assert await Tick.stream_length(route_override=over("a")) == 0


async def test_claim_and_read_and_ack_accept_the_override(bound):
    await Tick.stream_append(Tick(n=1), route_override=over("a"))
    await Tick.stream_ensure_consumer_group("g", route_override=over("a"))
    await Tick.stream_read("g", count=1, block_ms=None, route_override=over("a"))
    claimed = await Tick.stream_claim_pending(
        "g", "rescuer", min_idle_ms=0, route_override=over("a")
    )
    assert [e.record.n for e in claimed] == [1]
    await Tick.stream_append(Tick(n=2), route_override=over("a"))
    got = await Tick.stream_read_and_ack(
        "g", count=5, block_ms=None, route_override=over("a")
    )
    assert [t.n for t in got] == [2]


async def test_an_empty_override_means_the_default_route(bound):
    await Tick.stream_append(Tick(n=1), route_override={})
    assert await Tick.stream_length() == 1
