"""Shared, bounded process/thread pools: nested flows must not multiply pools."""

import os
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from volnux.executors import DefaultExecutor, shared
from volnux.flows import SingleFlow
from volnux.parser.executor_config import ExecutorInitializerConfig


@pytest.fixture(autouse=True)
def clean_pools():
    shared.shutdown_shared_pools()
    yield
    shared.shutdown_shared_pools()


def config(monkeypatch, **values):
    monkeypatch.setattr(
        shared, "_config", lambda name, default: values.get(name, default)
    )


def make_flow():
    ctx = MagicMock()
    ctx.task_profiles = deque([MagicMock()])
    ctx.is_multitask.return_value = False
    ctx.is_stream_segment.return_value = False
    return SingleFlow(ctx)


class TestPools:
    def test_the_same_pool_is_returned_every_time(self):
        assert shared.get_shared_pool(ThreadPoolExecutor) is shared.get_shared_pool(
            ThreadPoolExecutor
        )

    def test_process_and_thread_pools_are_separate_and_of_the_right_kind(self):
        thread = shared.get_shared_pool(ThreadPoolExecutor)
        assert isinstance(thread, ThreadPoolExecutor)
        assert not isinstance(thread, ProcessPoolExecutor)

    def test_only_the_standard_pools_are_shared(self):
        class MyExecutor(ThreadPoolExecutor):
            pass

        assert shared.get_shared_pool(MyExecutor) is None
        assert shared.get_shared_pool(DefaultExecutor) is None
        assert shared.pool_size(MyExecutor) is None

    def test_sizes_come_from_configuration(self, monkeypatch):
        config(monkeypatch, SHARED_THREAD_POOL_SIZE=3, SHARED_PROCESS_POOL_SIZE=2)
        assert shared.pool_size(ThreadPoolExecutor) == 3
        assert shared.pool_size(ProcessPoolExecutor) == 2
        assert shared.get_shared_pool(ThreadPoolExecutor)._max_workers == 3

    def test_default_sizes_are_bounded_by_the_machine(self):
        cpus = os.cpu_count() or 1
        assert shared.pool_size(ProcessPoolExecutor) == cpus
        assert shared.pool_size(ThreadPoolExecutor) == min(32, cpus + 4)

    @pytest.mark.parametrize("bad", [0, -5])
    def test_a_nonsense_size_still_gives_one_worker(self, monkeypatch, bad):
        config(monkeypatch, SHARED_THREAD_POOL_SIZE=bad)
        assert shared.pool_size(ThreadPoolExecutor) == 1

    def test_sharing_can_be_switched_off(self, monkeypatch):
        config(monkeypatch, SHARED_EXECUTOR_POOLS=False)
        assert shared.get_shared_pool(ThreadPoolExecutor) is None

    def test_shutting_down_lets_a_fresh_pool_be_created(self):
        first = shared.get_shared_pool(ThreadPoolExecutor)
        shared.shutdown_shared_pools()
        second = shared.get_shared_pool(ThreadPoolExecutor)
        assert second is not first
        assert second.submit(lambda: 7).result() == 7

    def test_work_runs_on_the_pool(self):
        pool = shared.get_shared_pool(ThreadPoolExecutor)
        assert pool.submit(sum, [1, 2, 3]).result() == 6


@pytest.mark.asyncio
class TestFlowsShareThePools:
    async def test_two_flows_get_the_same_pool(self):
        one, two = make_flow(), make_flow()
        a = one._initialize_executor(ThreadPoolExecutor, ExecutorInitializerConfig())
        b = two._initialize_executor(ThreadPoolExecutor, ExecutorInitializerConfig())
        assert a is b

    async def test_a_shared_pool_is_not_owned_by_the_flow(self):
        flow = make_flow()
        pool = flow._initialize_executor(
            ThreadPoolExecutor, ExecutorInitializerConfig()
        )
        assert flow._flow_executors == []
        await flow.close()
        assert pool.submit(lambda: "still alive").result() == "still alive"

    async def test_many_nested_flows_create_one_pool(self):
        flows = [make_flow() for _ in range(25)]
        pools = {
            id(f._initialize_executor(ThreadPoolExecutor, ExecutorInitializerConfig()))
            for f in flows
        }
        assert len(pools) == 1 and len(shared._pools) == 1

    async def test_an_explicit_worker_count_does_not_make_a_private_pool(self):
        flow = make_flow()
        pool = flow._initialize_executor(
            ThreadPoolExecutor, ExecutorInitializerConfig(max_workers=500)
        )
        assert pool is shared.get_shared_pool(ThreadPoolExecutor)

    async def test_an_executor_instance_is_passed_through(self):
        flow = make_flow()
        mine = ThreadPoolExecutor(max_workers=1)
        try:
            assert flow._initialize_executor(mine, ExecutorInitializerConfig()) is mine
        finally:
            mine.shutdown()

    async def test_other_executor_classes_are_still_built_per_flow(self):
        flow = make_flow()
        created = flow._initialize_executor(
            DefaultExecutor, ExecutorInitializerConfig()
        )
        assert created in flow._flow_executors

    async def test_with_sharing_off_each_flow_has_its_own_pool(self, monkeypatch):
        config(monkeypatch, SHARED_EXECUTOR_POOLS=False)
        one, two = make_flow(), make_flow()
        a = one._initialize_executor(
            ThreadPoolExecutor, ExecutorInitializerConfig(max_workers=1)
        )
        b = two._initialize_executor(
            ThreadPoolExecutor, ExecutorInitializerConfig(max_workers=1)
        )
        assert a is not b
        a.shutdown()
        b.shutdown()
