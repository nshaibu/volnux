"""The ``*_async`` aliases on KeyValueStoreIntegrationMixin, and ordering/paging in the
in-memory backends that back them."""

import pytest
from formax import BaseModel

from tests.unittests.support import SerializingMemoryBackend
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.exceptions import ObjectDoesNotExist
from volnux.mixins import KeyValueStoreIntegrationMixin


class Item(KeyValueStoreIntegrationMixin, BaseModel):
    name: str
    rank: int


@pytest.fixture(params=[InMemoryKeyValueStoreBackend, SerializingMemoryBackend])
def backend(request, monkeypatch):
    b = request.param()
    monkeypatch.setattr(Item, "_backend_store", b, raising=False)
    monkeypatch.setattr(Item, "_backend_initialized", True, raising=False)
    yield b
    b.close()


async def seed(*ranks):
    return [await Item(name=f"n{r}", rank=r).save_async() or None for r in ranks]


@pytest.mark.asyncio
class TestAsyncApi:
    async def test_save_get_roundtrip(self, backend):
        item = Item(name="a", rank=1)
        await item.save_async()
        assert (await Item.get_async(item.id)).name == "a"

    async def test_get_async_raises_when_missing(self, backend):
        with pytest.raises(ObjectDoesNotExist):
            await Item.get_async("nope")

    async def test_get_or_none_async(self, backend):
        assert await Item.get_or_none_async("nope") is None
        item = Item(name="a", rank=1)
        await item.save_async()
        assert (await Item.get_or_none_async(item.id)).id == item.id

    async def test_filter_async_returns_a_list(self, backend):
        await seed(1, 2, 3)
        found = await Item.filter_async(rank=2)
        assert isinstance(found, list) and [i.rank for i in found] == [2]

    async def test_all_async_and_count_async(self, backend):
        await seed(1, 2, 3)
        assert len(await Item.all_async()) == 3
        assert await Item.count_async() == 3
        assert await Item.count_async(rank=3) == 1

    async def test_order_by_ascending_and_descending(self, backend):
        await seed(2, 3, 1)
        assert [i.rank for i in await Item.all_async(order_by="rank")] == [1, 2, 3]
        assert [i.rank for i in await Item.all_async(order_by="-rank")] == [3, 2, 1]

    async def test_limit_and_offset(self, backend):
        await seed(1, 2, 3, 4)
        page = await Item.filter_async(order_by="rank", offset=1, limit=2)
        assert [i.rank for i in page] == [2, 3]

    async def test_latest_pattern_used_by_callers(self, backend):
        await seed(5, 9, 7)
        results = await Item.filter_async(order_by="-rank", limit=1)
        assert results[0].rank == 9

    async def test_update_async_and_delete_async(self, backend):
        item = Item(name="a", rank=1)
        await item.save_async()
        item.rank = 5
        await item.update_async()
        assert (await Item.get_async(item.id)).rank == 5
        await item.delete_async()
        assert await Item.get_or_none_async(item.id) is None
