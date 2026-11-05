"""Queues: a ``ttl`` that drops them, a per-key ``route_override``, ``queue_delete``."""

import asyncio

import pytest
from formax import BaseModel, InitStrategy

from volnux.backends.messaging.base import PushPopCapabilityMixin
from volnux.backends.storage_route import StorageRoute
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

pytestmark = pytest.mark.asyncio


class Job(MessagingBackendIntegrationMixin, KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    n: int

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return StorageRoute(
            components=["jobs", "{job_id}"],
            routing_keys=lambda: {"job_id": "default"},
        )


@pytest.fixture
def bound():
    backend = InMemoryKeyValueStoreBackend(namespace_prefix="ttl-test")
    previous = (Job._backend_store, Job._backend_initialized)
    Job._backend_store, Job._backend_initialized = backend, True
    yield backend
    Job._backend_store, Job._backend_initialized = previous


def over(job_id):
    return {"job_id": job_id}


class TestRouteOverride:
    async def test_each_override_is_its_own_queue(self, bound):
        await Job.push(Job(n=1), route_override=over("a"))
        await Job.push(Job(n=2), Job(n=3), route_override=over("b"))
        assert await Job.queue_length(route_override=over("a")) == 1
        assert await Job.queue_length(route_override=over("b")) == 2
        assert await Job.queue_length() == 0

    async def test_pop_reads_the_queue_it_names(self, bound):
        await Job.push(Job(n=1), route_override=over("a"))
        await Job.push(Job(n=2), route_override=over("b"))
        assert (await Job.pop(route_override=over("b"))).n == 2
        assert await Job.pop(route_override=over("b")) is None
        assert (await Job.pop(route_override=over("a"))).n == 1

    async def test_the_other_operations_follow_the_override(self, bound):
        await Job.enqueue(Job(n=1), route_override=over("a"))
        await Job.enqueue(Job(n=2), route_override=over("a"))
        assert [j.n for j in await Job.queue_range(route_override=over("a"))] == [1, 2]
        assert (await Job.queue_peek(route_override=over("a"))).n == 1
        assert (await Job.dequeue(route_override=over("a"))).n == 1
        assert [j.n for j in await Job.pop_many(route_override=over("a"))] == [2]

    async def test_a_blocking_pop_waits_for_a_push_to_that_queue(self, bound):
        async def producer():
            await asyncio.sleep(0.05)
            await Job.push(Job(n=7), route_override=over("late"))

        task = asyncio.create_task(producer())
        got = await Job.pop(timeout=2, route_override=over("late"))
        await task
        assert got.n == 7

    async def test_without_an_override_the_default_route_is_used(self, bound):
        await Job.push(Job(n=1))
        assert await Job.queue_length() == 1
        assert await Job.queue_length(route_override=over("default")) == 1


class TestQueueDelete:
    async def test_it_removes_the_queue_and_reports_it(self, bound):
        await Job.push(Job(n=1), route_override=over("a"))
        assert await Job.queue_delete(route_override=over("a")) is True
        assert await Job.queue_length(route_override=over("a")) == 0
        assert await Job.queue_delete(route_override=over("a")) is False

    async def test_it_leaves_the_other_queues(self, bound):
        await Job.push(Job(n=1), route_override=over("a"))
        await Job.push(Job(n=2), route_override=over("b"))
        await Job.queue_delete(route_override=over("a"))
        assert await Job.queue_length(route_override=over("b")) == 1


class TestTtl:
    async def test_the_queue_is_dropped_after_the_ttl(self, bound):
        await Job.push(Job(n=1), ttl=0.05, route_override=over("a"))
        assert await Job.queue_length(route_override=over("a")) == 1
        await asyncio.sleep(0.1)
        assert await Job.queue_length(route_override=over("a")) == 0
        assert await Job.pop(route_override=over("a")) is None
        assert await Job.queue_range(route_override=over("a")) == []

    async def test_a_queue_without_a_ttl_stays(self, bound):
        await Job.push(Job(n=1), route_override=over("a"))
        await asyncio.sleep(0.05)
        assert await Job.queue_length(route_override=over("a")) == 1

    async def test_every_push_moves_the_deadline(self, bound):
        await Job.push(Job(n=1), ttl=0.15, route_override=over("a"))
        await asyncio.sleep(0.1)
        await Job.push(Job(n=2), ttl=0.15, route_override=over("a"))
        await asyncio.sleep(0.1)  # 0.2s after the first push, 0.1s after the second
        assert await Job.queue_length(route_override=over("a")) == 2
        await asyncio.sleep(0.1)
        assert await Job.queue_length(route_override=over("a")) == 0

    async def test_a_push_after_expiry_starts_a_new_queue(self, bound):
        await Job.push(Job(n=1), ttl=0.03, route_override=over("a"))
        await asyncio.sleep(0.08)
        await Job.push(Job(n=2), route_override=over("a"))
        assert [j.n for j in await Job.queue_range(route_override=over("a"))] == [2]
        await asyncio.sleep(0.05)
        assert (
            await Job.queue_length(route_override=over("a")) == 1
        )  # (the old deadline is gone)

    @pytest.mark.parametrize("ttl", [0, -1])
    async def test_a_ttl_must_be_positive(self, bound, ttl):
        with pytest.raises(ValueError, match="ttl"):
            await Job.push(Job(n=1), ttl=ttl)
        assert await Job.queue_length() == 0

    async def test_pop_before_expiry_returns_the_item(self, bound):
        await Job.push(Job(n=5), ttl=5, route_override=over("a"))
        assert (await Job.pop(route_override=over("a"))).n == 5

    async def test_delete_clears_the_deadline(self, bound):
        await Job.push(Job(n=1), ttl=0.05, route_override=over("a"))
        await Job.queue_delete(route_override=over("a"))
        await Job.push(Job(n=2), route_override=over("a"))
        await asyncio.sleep(0.1)
        assert await Job.queue_length(route_override=over("a")) == 1


class TestBackendWithoutExpiry:
    async def test_the_ttl_is_refused_before_anything_is_pushed(
        self, bound, monkeypatch
    ):
        # a backend that did not implement queue_expire
        monkeypatch.setattr(
            type(bound), "queue_expire", PushPopCapabilityMixin.queue_expire
        )
        with pytest.raises(NotImplementedError, match="expiring a queue"):
            await Job.push(Job(n=1), ttl=5, route_override=over("a"))
        assert await Job.queue_length(route_override=over("a")) == 0

    async def test_a_push_without_ttl_still_works(self, bound, monkeypatch):
        monkeypatch.setattr(
            type(bound), "queue_expire", PushPopCapabilityMixin.queue_expire
        )
        assert await Job.push(Job(n=1), route_override=over("a")) == 1
