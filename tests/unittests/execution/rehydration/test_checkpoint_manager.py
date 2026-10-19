import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from volnux.execution.context import ExecutionContext
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
from volnux.task import PipelineTask


class Item:
    """Minimal persistable object."""

    def __init__(self, id, save=None):
        self.id = id
        self.save = save or AsyncMock()


@pytest_asyncio.fixture
async def manager():
    cm = VolnuxCheckPointManager(
        checkpoint_interval=0.02, retry_attempts=3, retry_delay=0.001, snapshot_ttl=60
    )
    yield cm
    await asyncio.wait_for(cm.stop(), 5)


async def eventually(predicate, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


class TestWorker:
    @pytest.mark.asyncio
    async def test_idle_worker_does_not_block_event_loop(self, manager):
        await manager.start()
        # Would time out if the worker spun on an empty queue without yielding
        await asyncio.wait_for(asyncio.sleep(0.05), 1)

    @pytest.mark.asyncio
    async def test_enqueued_item_is_saved_with_ttl(self, manager):
        await manager.start()
        item = Item("a")
        manager.enqueue(item)
        await asyncio.wait_for(manager.flush(), 2)
        item.save.assert_awaited_once_with(ttl=60)

    @pytest.mark.asyncio
    async def test_flush_waits_for_save_to_finish(self, manager):
        finished = asyncio.Event()

        async def slow_save(**kwargs):
            await asyncio.sleep(0.05)
            finished.set()

        await manager.start()
        manager.enqueue(Item("a", save=slow_save))
        await asyncio.wait_for(manager.flush(), 2)
        assert finished.is_set()

    @pytest.mark.asyncio
    async def test_consecutive_duplicates_coalesced(self, manager):
        old, new, other = Item("a"), Item("a"), Item("b")
        manager.enqueue(old)
        manager.enqueue(new)
        manager.enqueue(other)
        await manager.start()
        await asyncio.wait_for(manager.flush(), 2)
        old.save.assert_not_awaited()
        new.save.assert_awaited_once()
        other.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_retries_failed_save(self, manager):
        save = AsyncMock(side_effect=[RuntimeError("down"), None])
        await manager.start()
        manager.enqueue(Item("a", save=save))
        await asyncio.wait_for(manager.flush(), 2)
        assert save.await_count == 2

    @pytest.mark.asyncio
    async def test_gives_up_after_retry_attempts(self, manager, caplog):
        save = AsyncMock(side_effect=RuntimeError("down"))
        await manager.start()
        manager.enqueue(Item("a", save=save))
        await asyncio.wait_for(manager.flush(), 2)
        assert save.await_count == 3
        assert "Persistence failed after 3 tries" in caplog.text

    @pytest.mark.asyncio
    async def test_failure_does_not_stop_worker(self, manager):
        failing = Item("a", save=AsyncMock(side_effect=RuntimeError("down")))
        healthy = Item("b")
        await manager.start()
        manager.enqueue(failing)
        manager.enqueue(healthy)
        await asyncio.wait_for(manager.flush(), 2)
        healthy.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_flushes_pending_items(self):
        cm = VolnuxCheckPointManager(checkpoint_interval=10, retry_delay=0.001)
        await cm.start()
        item = Item("a")
        cm.enqueue(item)
        await asyncio.wait_for(cm.stop(), 2)
        item.save.assert_awaited_once()


class TestGetLatestSnapshot:
    @pytest.mark.asyncio
    async def test_empty_queue_returns_none(self, manager):
        assert await asyncio.wait_for(manager.get_latest_snapshot(), 1) is None

    @pytest.mark.asyncio
    async def test_returns_latest_duplicate(self, manager):
        old, new = Item("a"), Item("a")
        manager.enqueue(old)
        manager.enqueue(new)
        assert await manager.get_latest_snapshot() is new
        assert manager._queue.empty()
        await asyncio.wait_for(manager.flush(), 1)  # entries marked done


class TestMonitoring:
    @pytest.mark.asyncio
    async def test_monitored_object_saved_periodically(self, manager):
        item = Item("ctx")
        manager.monitor(item)
        await manager.start()
        await eventually(lambda: item.save.await_count >= 2)

    @pytest.mark.asyncio
    async def test_unmonitored_object_not_saved(self, manager):
        item = Item("ctx")
        manager.monitor(item)
        manager.unmonitor(item)
        await manager.start()
        await asyncio.sleep(0.08)
        item.save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execution_context_is_monitorable(self, manager):
        task = MagicMock(spec=PipelineTask)
        task.options = None
        task.condition_node = MagicMock()
        context = ExecutionContext(task_profiles=task, pipeline=MagicMock())

        with patch.object(ExecutionContext, "save", new=AsyncMock()) as save:
            manager.monitor(context)
            await manager.start()
            await eventually(lambda: save.await_count >= 1)
        save.assert_awaited_with(ttl=60)
