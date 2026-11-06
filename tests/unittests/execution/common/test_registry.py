from concurrent.futures import Future

import pytest

from volnux.execution.common._pool import _ContextFutureRegistry


class DummyFuture(Future):
    pass


@pytest.fixture
def registry():
    return _ContextFutureRegistry()


def test_add_creates_bucket_and_stores_future(registry):
    future = DummyFuture()

    registry.add("ctx-1", future)

    assert registry.active_count("ctx-1") == 1
    assert registry.snapshot("ctx-1") == {future}
    assert registry.all_context_ids() == ["ctx-1"]


def test_add_allows_multiple_futures_per_context(registry):
    future_1 = DummyFuture()
    future_2 = DummyFuture()

    registry.add("ctx-1", future_1)
    registry.add("ctx-1", future_2)

    assert registry.active_count("ctx-1") == 2
    assert registry.snapshot("ctx-1") == {future_1, future_2}


def test_discard_removes_future_and_deletes_empty_bucket(registry):
    future = DummyFuture()
    registry.add("ctx-1", future)

    registry.discard("ctx-1", future)

    assert registry.active_count("ctx-1") == 0
    assert registry.snapshot("ctx-1") == set()
    assert registry.all_context_ids() == []


def test_discard_is_noop_for_missing_context(registry):
    future = DummyFuture()

    registry.discard("missing", future)

    assert registry.active_count("missing") == 0
    assert registry.snapshot("missing") == set()


def test_snapshot_returns_shallow_copy(registry):
    future = DummyFuture()
    registry.add("ctx-1", future)

    snapshot = registry.snapshot("ctx-1")
    snapshot.clear()

    assert registry.active_count("ctx-1") == 1
    assert registry.snapshot("ctx-1") == {future}


def test_pop_all_removes_and_returns_all_futures(registry):
    future_1 = DummyFuture()
    future_2 = DummyFuture()

    registry.add("ctx-1", future_1)
    registry.add("ctx-1", future_2)

    popped = registry.pop_all("ctx-1")

    assert popped == {future_1, future_2}
    assert registry.active_count("ctx-1") == 0
    assert registry.snapshot("ctx-1") == set()
    assert registry.all_context_ids() == []


def test_pop_all_on_missing_context_returns_empty_set(registry):
    assert registry.pop_all("missing") == set()


def test_active_count_tracks_context_size(registry):
    future_1 = DummyFuture()
    future_2 = DummyFuture()

    registry.add("ctx-1", future_1)
    registry.add("ctx-1", future_2)
    registry.add("ctx-2", DummyFuture())

    assert registry.active_count("ctx-1") == 2
    assert registry.active_count("ctx-2") == 1
    assert registry.active_count("missing") == 0


def test_all_context_ids_returns_only_non_empty_contexts(registry):
    registry.add("ctx-1", DummyFuture())
    registry.add("ctx-2", DummyFuture())

    ids = registry.all_context_ids()

    assert set(ids) == {"ctx-1", "ctx-2"}
