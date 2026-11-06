from unittest.mock import MagicMock

import pytest

from volnux.exceptions import ObjectDoesNotExist
from volnux.result.stream import ResultStream


class FakeModel:
    """Minimal stand-in for a KeyValueStoreIntegrationMixin model."""

    def __init__(self, id, **fields):
        self.id = id
        self.__dict__.update(fields)
        self.hook_calls = 0

    def change_object_id(self, new_id):
        self.id = new_id

    async def _on_delete_hook(self):
        self.hook_calls += 1

    @classmethod
    async def get_backend(cls):
        raise RuntimeError("not used")

    @classmethod
    async def get_schema_name(cls):
        return "schema"

    @classmethod
    def get_storage_route(cls):
        return None

    @classmethod
    def hydrate_formax_model(cls, state):
        return cls(state["id"], **{k: v for k, v in state.items() if k != "id"})


def _backend(records=None):
    backend = MagicMock()
    backend.resolve_physical_target.return_value = "schema"
    store = dict(records or {})

    def get(schema, key, klass):
        if key not in store:
            raise ObjectDoesNotExist(key)
        return store[key]

    def delete(schema, key):
        if key not in store:
            raise ObjectDoesNotExist(key)
        del store[key]

    def get_many(schema, keys, klass):
        found = {}
        for key in keys:
            try:
                found[str(key)] = get(schema, key, klass)
            except ObjectDoesNotExist:
                pass
        return found

    backend.get.side_effect = get
    backend.get_many.side_effect = get_many
    backend.delete.side_effect = delete
    backend.store = store
    return backend


def _stream(backend, keys, memory_capacity=100):
    return ResultStream._make(
        model_klass=FakeModel,
        transaction_id="tx-test",
        keys=keys,
        predicates=[],
        chunk_size=10,
        memory_capacity=memory_capacity,
        persisted_backend=backend,
    )


@pytest.mark.asyncio
async def test_missing_record_is_skipped_but_backend_errors_propagate():
    backend = _backend({"a": FakeModel("a")})
    stream = _stream(backend, ["a", "gone"], memory_capacity=0)
    assert [o.id async for o in stream] == ["a"]

    backend.get_many.side_effect = ConnectionError("db down")
    with pytest.raises(ConnectionError):
        await _stream(backend, ["a"], memory_capacity=0).to_list()


@pytest.mark.asyncio
async def test_bulk_update_works_without_memory_backend():
    backend = _backend({"a": FakeModel("a", status="x")})
    stream = _stream(backend, ["a"], memory_capacity=0)
    assert stream.memory_backend is None

    assert await stream.bulk_update(status="y") == 1
    backend.upsert.assert_called_once()
    assert backend.store["a"].status == "y"


@pytest.mark.asyncio
async def test_bulk_delete_counts_only_real_deletes_and_runs_hook():
    a = FakeModel("a")
    backend = _backend({"a": a})
    stream = _stream(backend, ["a"], memory_capacity=0)

    assert await stream.bulk_delete() == 1
    assert a.hook_calls == 1
    assert "a" not in backend.store
    assert len(stream) == 0


@pytest.mark.asyncio
async def test_bulk_delete_does_not_report_success_when_persisted_delete_fails():
    backend = _backend({"a": FakeModel("a")})
    backend.delete.side_effect = RuntimeError("write failed")
    stream = _stream(backend, ["a"])  # memory tier enabled

    with pytest.raises(RuntimeError):
        await stream.bulk_delete()
    assert "a" in stream._key_set


@pytest.mark.asyncio
async def test_bulk_delete_with_no_backends_deletes_nothing():
    stream = _stream(None, [], memory_capacity=0)
    stream.persisted_backend = None
    assert await stream.bulk_delete() == 0


@pytest.mark.asyncio
async def test_join_pushdown_runs_in_executor_and_respects_stream_keys():
    backend = _backend()
    backend.filter_with_joins.return_value = [
        {"id": "a", "n": 1},
        {"id": "b", "n": 2},
        {"id": "c", "n": 3},
    ]
    stream = _stream(backend, ["c", "a"], memory_capacity=0)
    joined = await stream.resolve_joins("owner")

    result = await joined.to_list()
    assert [o.id for o in result] == ["c", "a"]  # only own keys, in key order
    backend.filter_with_joins.assert_called_once()


@pytest.mark.asyncio
async def test_join_pushdown_falls_back_when_unsupported():
    backend = _backend({"a": FakeModel("a")})
    backend.filter_with_joins.side_effect = NotImplementedError
    joined = await _stream(backend, ["a"], memory_capacity=0).resolve_joins("owner")
    assert [o.id async for o in joined] == ["a"]


# ---------------------------------------------------------------------------
# Medium-severity semantics
# ---------------------------------------------------------------------------


def _records(**by_id):
    return {k: FakeModel(k, **v) for k, v in by_id.items()}


@pytest.mark.asyncio
async def test_exists_evaluates_filters():
    backend = _backend(_records(a={"n": 1}))
    stream = _stream(backend, ["a"], memory_capacity=0)
    assert await stream.exists() is True
    assert await stream.where(lambda o: o.n == 99).exists() is False


@pytest.mark.asyncio
async def test_paginate_total_counts_matches_not_keys():
    backend = _backend(_records(**{k: {"n": i} for i, k in enumerate("abcde")}))
    stream = _stream(backend, list("abcde"), memory_capacity=0).where(
        lambda o: o.n % 2 == 0
    )  # a, c, e
    page, total = await stream.paginate(page=1, page_size=2)
    assert total == 3
    assert [o.id for o in await page.to_list()] == ["a", "c"]

    page2, total2 = await stream.paginate(page=2, page_size=2)
    assert total2 == 3
    assert [o.id for o in await page2.to_list()] == ["e"]


@pytest.mark.asyncio
async def test_paginate_without_filters_uses_key_count():
    backend = _backend(_records(**{k: {} for k in "abcd"}))
    stream = _stream(backend, list("abcd"), memory_capacity=0)
    page, total = await stream.paginate(page=1, page_size=2)
    assert total == 4
    assert [o.id for o in await page.to_list()] == ["a", "b"]


@pytest.mark.asyncio
async def test_union_preserves_order_dedupes_and_keeps_filters():
    backend = _backend(_records(**{k: {"n": i} for i, k in enumerate("abcd")}))
    evens = _stream(backend, list("abcd"), 0).where(lambda o: o.n % 2 == 0)  # a, c
    tail = _stream(backend, ["c", "d"], 0)  # unfiltered: c, d

    merged = evens.union(tail)
    assert merged._keys == ["a", "b", "c", "d"]  # first-seen order, no dupes
    # b came only from the filtered stream and fails its filter -> excluded;
    # d came from the unfiltered stream -> kept.
    assert [o.id for o in await merged.to_list()] == ["a", "c", "d"]


@pytest.mark.asyncio
async def test_union_without_filters_has_no_predicates():
    backend = _backend()
    merged = _stream(backend, ["a"], 0).union(_stream(backend, ["b", "a"], 0))
    assert merged._keys == ["a", "b"]
    assert merged._predicates == []


def test_filter_keeps_memory_capacity_and_cache_results_keeps_q():
    from volnux.result.stream import Q

    stream = _stream(_backend(), ["a"], memory_capacity=42)
    assert stream.filter(n=1).memory_capacity == 42


@pytest.mark.asyncio
async def test_cache_results_keeps_q_objects():
    from volnux.result.stream import Q

    backend = _backend(_records(a={"n": 1}))
    stream = _stream(backend, ["a"], 0).q_filter(Q(n=1))
    cached = await stream.cache_results()
    assert cached._q_objects == stream._q_objects


@pytest.mark.asyncio
async def test_optimize_result_is_cached_between_iterations():
    from volnux.result.stream import Q

    backend = _backend(_records(a={"n": 1}, b={"n": 2}))
    pushed = MagicMock()
    pushed._keys = ["a"]
    backend.filter.return_value = pushed
    stream = _stream(backend, ["a", "b"], 0).q_filter(Q(n=1))

    assert [o.id async for o in stream] == ["a"]
    assert [o.id async for o in stream] == ["a"]
    assert backend.filter.call_count == 1  # pushed down once, not per iteration


# ---------------------------------------------------------------------------
# _fetch_batch: one multi-get per batch instead of N+1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_batch_uses_a_single_multi_get_and_keeps_order():
    backend = _backend(_records(**{k: {} for k in "abcde"}))
    stream = _stream(backend, list("edcba"), memory_capacity=0)

    result = await stream.to_list()

    assert [o.id for o in result] == list("edcba")
    assert backend.get_many.call_count == 1  # one chunk, one round trip
    backend.get.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_batch_serves_second_pass_from_memory_tier():
    backend = _backend(_records(**{k: {} for k in "abc"}))
    stream = _stream(backend, list("abc"), memory_capacity=10)

    await stream.to_list()
    await stream.to_list()

    assert backend.get_many.call_count == 1  # second pass is all cache hits


@pytest.mark.asyncio
async def test_fetch_batch_only_requests_missing_ids_from_persisted():
    backend = _backend(_records(**{k: {} for k in "abc"}))
    stream = _stream(backend, list("abc"), memory_capacity=10)
    await stream.add(backend.store["a"], persist=False)  # a is cached

    await stream.to_list()

    requested = backend.get_many.call_args.args[1]
    assert sorted(requested) == ["b", "c"]


@pytest.mark.asyncio
async def test_fetch_batch_memory_tier_respects_capacity():
    backend = _backend(_records(**{k: {} for k in "abcdef"}))
    stream = _stream(backend, list("abcdef"), memory_capacity=3)

    assert len(await stream.to_list()) == 6
    assert len(stream._access_order) <= 3
    assert len(stream.memory_backend._storage.get("schema", {})) <= 3


@pytest.mark.asyncio
async def test_fetch_batch_drops_missing_ids():
    backend = _backend(_records(a={}, c={}))
    stream = _stream(backend, ["a", "b", "c"], memory_capacity=0)
    assert [o.id for o in await stream.to_list()] == ["a", "c"]


# ---------------------------------------------------------------------------
# Shared executor and shared cache across derived streams
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_uses_the_kv_executor_not_the_messaging_pool():
    import threading

    from volnux.backends.messaging.util import _BLOCKING_EXECUTOR
    from volnux.concurrency.kv_executor import KV_EXECUTOR

    name = await ResultStream._run_sync(lambda: threading.current_thread().name)
    assert name.startswith(KV_EXECUTOR._thread_name_prefix)
    assert not name.startswith(_BLOCKING_EXECUTOR._thread_name_prefix)


@pytest.mark.asyncio
async def test_stream_calls_follow_an_active_transaction_thread():
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from volnux.concurrency.kv_executor import TRANSACTION_EXECUTOR

    tx = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tx-test")
    token = TRANSACTION_EXECUTOR.set(tx)
    try:
        name = await ResultStream._run_sync(lambda: threading.current_thread().name)
    finally:
        TRANSACTION_EXECUTOR.reset(token)
        tx.shutdown()
    assert name.startswith("tx-test")


@pytest.mark.asyncio
async def test_derived_streams_share_one_cache():
    backend = _backend(_records(**{k: {"n": i} for i, k in enumerate("abc")}))
    parent = _stream(backend, list("abc"), memory_capacity=10)
    await parent.to_list()
    assert backend.get_many.call_count == 1

    child = parent.where(lambda o: o.n > 0)
    assert child.memory_backend is parent.memory_backend
    assert child._access_order is parent._access_order

    assert [o.id for o in await child.to_list()] == ["b", "c"]
    assert backend.get_many.call_count == 1  # served from the parent's cache


@pytest.mark.asyncio
async def test_cache_capacity_bounds_the_whole_chain():
    backend = _backend(_records(**{k: {} for k in "abcdef"}))
    parent = _stream(backend, list("abcdef"), memory_capacity=3)
    await parent.to_list()
    await parent.where(lambda o: True).to_list()
    assert len(parent._access_order) <= 3
    assert len(parent.memory_backend._storage.get("schema", {})) <= 3


def test_no_cache_and_shard_do_not_share_the_cache():
    parent = _stream(_backend(), list("abcd"), memory_capacity=10)
    assert parent.no_cache().memory_backend is None
    shards = list(parent.shard(2))
    assert all(sh.memory_backend is not parent.memory_backend for sh in shards)
    assert shards[0].memory_backend is not shards[1].memory_backend


@pytest.mark.asyncio
async def test_memory_tier_returns_the_added_object_by_identity():
    """Documented contract: the memory tier stores references, not copies."""
    backend = _backend()
    stream = _stream(backend, [], memory_capacity=10)
    obj = FakeModel("a", n=1)
    await stream.add(obj, persist=False)

    (got,) = await stream.to_list()
    assert got is obj


@pytest.mark.asyncio
async def test_evict_memory_forces_a_fresh_read_from_persisted():
    backend = _backend(_records(a={"n": 1}))
    stream = _stream(backend, ["a"], memory_capacity=10)
    await stream.to_list()
    backend.store["a"] = FakeModel("a", n=2)  # changed by another writer

    assert (await stream.to_list())[0].n == 1  # stale until evicted
    await stream.evict_memory()
    assert (await stream.to_list())[0].n == 2
