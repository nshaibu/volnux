"""BaseFlow: signal-task lifetime, future bookkeeping, per-kind bridges,
dispatch-failure cleanup and thread-safe event caching."""

import asyncio
import gc
import threading
from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.executors import DefaultExecutor, ProcessPoolExecutor, ThreadPoolExecutor
from volnux.flows import SingleFlow
from volnux.flows import base as flow_base
from volnux.flows.base import _emit_signal_sync
from volnux.flows.bridge.communications.tasks import (
    LocalTaskCommunicationBridge,
    MessagingTaskCommunicationBridge,
)
from volnux.signal.signals import SoftSignal


def make_flow(enable_communication=False):
    ctx = MagicMock()
    ctx.task_profiles = deque([MagicMock()])
    return SingleFlow(ctx, enable_communication=enable_communication)


def fake_event(task_id="t1"):
    event = MagicMock()
    event._task_id = task_id
    return event


def fake_bridge():
    bridge = MagicMock()
    bridge.register_task = AsyncMock(return_value=MagicMock())
    bridge.start_monitoring = AsyncMock()
    bridge.send_command = AsyncMock(return_value=True)
    bridge.get_status = AsyncMock(return_value="status")
    bridge.shutdown = AsyncMock()
    return bridge


@pytest.fixture
def end_signals(monkeypatch):
    """Capture event_execution_end emissions instead of running listeners."""
    seen = []

    async def emit_async(**kwargs):
        seen.append(kwargs)
        return []

    monkeypatch.setattr(flow_base.event_execution_end, "emit_async", emit_async)
    return seen


# --- 1. _emit_signal_sync ----------------------------------------------------


class TestEmitSignalSync:
    @pytest.mark.asyncio
    async def test_returns_a_referenced_task_that_runs(self):
        got = []
        signal = SoftSignal("flow_sig_ok", provide_args=["x"])

        async def listener(sender, signal, x):
            got.append(x)

        signal.connect(None, listener)
        task = _emit_signal_sync(signal, sender=object, x=1)
        assert isinstance(task, asyncio.Future)
        await task
        assert got == [1]

    @pytest.mark.asyncio
    async def test_task_failure_is_logged_not_left_unretrieved(self, caplog):
        signal = MagicMock()
        signal.emit_async = AsyncMock(side_effect=RuntimeError("emit blew up"))
        task = _emit_signal_sync(signal, sender=object)
        await asyncio.wait([task])
        await asyncio.sleep(0)
        assert "Signal emission failed" in caplog.text

    def test_without_a_running_loop_it_emits_synchronously(self):
        got = []
        signal = SoftSignal("flow_sig_sync", provide_args=["x"])

        def listener(sender, signal, x):
            got.append(x)

        signal.connect(None, listener)
        assert _emit_signal_sync(signal, sender=object, x=2) is None
        assert got == [2]

    def test_sync_fallback_swallows_errors(self, caplog):
        signal = MagicMock()
        signal.emit.side_effect = RuntimeError("nope")
        assert _emit_signal_sync(signal, sender=object) is None
        assert "Signal emission failed" in caplog.text

    @pytest.mark.asyncio
    async def test_flow_keeps_end_signal_tasks_referenced_until_done(self, end_signals):
        flow = make_flow()
        flow._schedule_end_signal(fake_event())
        assert len(flow._signal_tasks) == 1
        gc.collect()
        await asyncio.gather(*list(flow._signal_tasks))
        await asyncio.sleep(0)
        assert len(end_signals) == 1 and not flow._signal_tasks

    @pytest.mark.asyncio
    async def test_close_waits_for_pending_end_signals(self, monkeypatch):
        finished = []

        async def slow_emit(**kwargs):
            await asyncio.sleep(0.05)
            finished.append(1)
            return []

        monkeypatch.setattr(flow_base.event_execution_end, "emit_async", slow_emit)
        flow = make_flow()
        flow._schedule_end_signal(fake_event())
        await flow.close()
        assert finished == [1]


# --- 2 + 3. future bookkeeping ----------------------------------------------


class TestFutureBookkeeping:
    @pytest.mark.asyncio
    async def test_close_cancels_pending_and_clears_futures(self):
        flow = make_flow()
        pending = asyncio.get_running_loop().create_future()
        done = asyncio.get_running_loop().create_future()
        done.set_result(1)
        flow._task_futures = {"a": pending, "b": done}

        await flow.close()

        assert pending.cancelled() and not done.cancelled()
        assert flow._task_futures == {}

    @pytest.mark.asyncio
    async def test_cancel_after_close_is_a_noop(self):
        flow = make_flow()
        flow._task_futures = {"a": asyncio.get_running_loop().create_future()}
        await flow.close()
        await flow.cancel()  # nothing left to touch

    @pytest.mark.asyncio
    async def test_resubmitting_a_task_id_keeps_the_pending_first_future(self):
        flow = make_flow()
        loop = asyncio.get_running_loop()
        first, second = loop.create_future(), loop.create_future()
        flow._track_future("t", first, None)
        flow._track_future("t", second, None)

        assert flow._task_futures["t"] is second
        assert flow._superseded_futures == [first]

        await flow.cancel()
        assert first.cancelled() and second.cancelled()

    @pytest.mark.asyncio
    async def test_a_finished_previous_future_is_not_kept(self):
        flow = make_flow()
        loop = asyncio.get_running_loop()
        first, second = loop.create_future(), loop.create_future()
        first.set_result(None)
        flow._track_future("t", first, None)
        flow._track_future("t", second, None)
        assert flow._superseded_futures == []

    @pytest.mark.asyncio
    async def test_close_cancels_superseded_futures(self):
        flow = make_flow()
        loop = asyncio.get_running_loop()
        first, second = loop.create_future(), loop.create_future()
        flow._track_future("t", first, None)
        flow._track_future("t", second, None)
        await flow.close()
        assert first.cancelled() and second.cancelled()
        assert flow._superseded_futures == []


# --- 4. one bridge per executor kind ----------------------------------------


class TestBridgePerKind:
    def test_same_kind_reuses_the_bridge(self):
        flow = make_flow(enable_communication=True)
        first = flow._ensure_comm_bridge(DefaultExecutor)
        assert flow._ensure_comm_bridge(ThreadPoolExecutor) is first
        assert flow._extra_bridges == {}

    def test_different_kind_gets_its_own_bridge(self):
        flow = make_flow(enable_communication=True)
        local = flow._ensure_comm_bridge(DefaultExecutor)
        process = flow._ensure_comm_bridge(ProcessPoolExecutor)

        assert type(local) is LocalTaskCommunicationBridge
        assert type(process) is MessagingTaskCommunicationBridge
        assert flow._comm_bridge is local  # primary is unchanged
        assert flow._extra_bridges == {"process": process}
        assert flow._ensure_comm_bridge(ProcessPoolExecutor) is process

    def test_executor_without_a_bridge_gets_none(self):
        flow = make_flow(enable_communication=True)
        flow._ensure_comm_bridge(DefaultExecutor)
        grpc = type("GRPCExecutor", (DefaultExecutor,), {})
        assert flow._ensure_comm_bridge(grpc) is None

    def test_disabled_communication_returns_injected_bridge_only(self):
        flow = make_flow(enable_communication=False)
        assert flow._ensure_comm_bridge(DefaultExecutor) is None
        bridge = fake_bridge()
        flow._comm_bridge = bridge
        assert flow._ensure_comm_bridge(ProcessPoolExecutor) is bridge

    def test_extra_bridge_creation_failure_is_not_fatal(self, monkeypatch):
        flow = make_flow(enable_communication=True)
        flow._ensure_comm_bridge(DefaultExecutor)

        def broken(*a, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr(flow_base, "create_communication_bridge", broken)
        assert flow._ensure_comm_bridge(ProcessPoolExecutor) is None

    @pytest.mark.asyncio
    async def test_commands_route_to_the_bridge_that_owns_the_task(self):
        flow = make_flow()
        primary, extra = fake_bridge(), fake_bridge()
        flow._comm_bridge = primary
        loop = asyncio.get_running_loop()
        flow._track_future("local-task", loop.create_future(), None)
        flow._track_future("proc-task", loop.create_future(), extra)

        assert await flow.pause_task("proc-task") is True
        extra.send_command.assert_awaited_once()
        primary.send_command.assert_not_called()

        assert await flow.resume_task("local-task") is True
        primary.send_command.assert_awaited_once()
        assert await flow.get_task_status("proc-task") == "status"
        extra.get_status.assert_awaited_once_with("proc-task")

    @pytest.mark.asyncio
    async def test_pause_all_reaches_every_bridge(self):
        flow = make_flow()
        primary, extra = fake_bridge(), fake_bridge()
        flow._comm_bridge = primary
        loop = asyncio.get_running_loop()
        flow._track_future("a", loop.create_future(), None)
        flow._track_future("b", loop.create_future(), extra)
        assert await flow.pause() == 2

    @pytest.mark.asyncio
    async def test_close_shuts_down_every_bridge(self):
        flow = make_flow()
        primary, extra = fake_bridge(), fake_bridge()
        flow._comm_bridge = primary
        flow._extra_bridges["process"] = extra
        await flow.close()
        primary.shutdown.assert_awaited_once()
        extra.shutdown.assert_awaited_once()
        assert flow._comm_bridge is None and flow._extra_bridges == {}

    @pytest.mark.asyncio
    async def test_one_bridge_failing_to_shutdown_does_not_block_the_other(self):
        flow = make_flow()
        primary, extra = fake_bridge(), fake_bridge()
        primary.shutdown.side_effect = RuntimeError("bad")
        flow._comm_bridge = primary
        flow._extra_bridges["process"] = extra
        await flow.close()
        extra.shutdown.assert_awaited_once()


# --- 5. dispatch failure cleanup --------------------------------------------


class TestDispatchFailure:
    @pytest.fixture(autouse=True)
    def start_signal(self, monkeypatch):
        monkeypatch.setattr(
            flow_base.event_execution_start, "emit_async", AsyncMock(return_value=[])
        )

    @pytest.mark.asyncio
    async def test_failed_dispatch_detaches_channel_and_emits_end(
        self, monkeypatch, end_signals
    ):
        flow = make_flow()
        bridge = fake_bridge()
        channel = object()
        bridge.register_task = AsyncMock(return_value=channel)
        flow._comm_bridge = bridge
        event = fake_event()

        def boom(executor, event, kwargs):
            raise RuntimeError("dispatch failed")

        monkeypatch.setattr(flow, "_dispatch_to_executor", boom)

        with pytest.raises(RuntimeError, match="dispatch failed"):
            await flow._submit_event_to_executor(MagicMock(), event, {})
        await asyncio.gather(*list(flow._signal_tasks))

        assert event._command_channel is None
        assert "t1" not in flow._task_futures and "t1" not in flow._task_bridges
        assert len(end_signals) == 1 and end_signals[0]["event"] is event

    @pytest.mark.asyncio
    async def test_successful_submit_emits_end_when_future_finishes(
        self, monkeypatch, end_signals
    ):
        flow = make_flow()
        future = asyncio.get_running_loop().create_future()
        monkeypatch.setattr(flow, "_dispatch_to_executor", lambda *a: future)
        event = fake_event()

        returned = await flow._submit_event_to_executor(MagicMock(), event, {"k": 1})
        assert returned is future and flow._task_futures["t1"] is future
        assert event._call_args == {"k": 1}
        assert end_signals == []

        future.set_result(None)
        await asyncio.sleep(0)
        await asyncio.gather(*list(flow._signal_tasks))
        assert len(end_signals) == 1

    @pytest.mark.asyncio
    async def test_channel_registered_on_the_bridge_for_the_executor_kind(
        self, monkeypatch
    ):
        flow = make_flow(enable_communication=True)
        local = fake_bridge()
        process = fake_bridge()
        flow._comm_bridge, flow._comm_bridge_kind = local, "local"
        flow._extra_bridges["process"] = process
        future = asyncio.get_running_loop().create_future()
        monkeypatch.setattr(flow, "_dispatch_to_executor", lambda *a: future)

        await flow._submit_event_to_executor(ProcessPoolExecutor, fake_event(), {})

        process.register_task.assert_awaited_once()
        process.start_monitoring.assert_awaited_once()
        local.register_task.assert_not_called()
        assert flow._task_bridges["t1"] is process


# --- 6. cancel semantics ----------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_falls_back_to_the_future_when_no_command_is_delivered():
    flow = make_flow()
    bridge = fake_bridge()
    bridge.send_command = AsyncMock(return_value=False)
    flow._comm_bridge = bridge
    future = asyncio.get_running_loop().create_future()
    flow._track_future("t", future, None)
    await flow.cancel()
    assert future.cancelled()


@pytest.mark.asyncio
async def test_cancel_leaves_the_future_alone_when_the_channel_delivered():
    flow = make_flow()
    bridge = fake_bridge()
    flow._comm_bridge = bridge
    future = asyncio.get_running_loop().create_future()
    flow._track_future("t", future, None)
    await flow.cancel()
    assert not future.cancelled()


# --- 7. executor naming + thread-safe event cache ----------------------------


def test_initialize_executor_accepts_class_or_shared_instance(monkeypatch):
    from volnux.executors import shared as shared_pools
    from volnux.parser.executor_config import ExecutorInitializerConfig

    # private pools: the flow-owned path (sharing is covered in test_shared_pools)
    monkeypatch.setattr(shared_pools, "enabled", lambda: False)
    flow = make_flow()
    shared = ThreadPoolExecutor(max_workers=1)
    try:
        assert flow._initialize_executor(shared, ExecutorInitializerConfig()) is shared
        assert flow._flow_executors == []
        created = flow._initialize_executor(
            ThreadPoolExecutor, ExecutorInitializerConfig(max_workers=1)
        )
        assert created._created_by_flow and created in flow._flow_executors
        created.shutdown()
    finally:
        shared.shutdown()


def test_concurrent_get_initialized_event_builds_one_event(monkeypatch):
    flow = make_flow()
    profile = MagicMock()
    profile.get_id.return_value = "task-1"
    built = []
    barrier = threading.Barrier(8)

    def slow_build(self, task_profile, task_id):
        built.append(1)
        threading.Event().wait(0.02)
        result = (object(), {})
        self._initialized_events[task_id] = result
        return result

    original = flow_base.BaseFlow._get_or_build_event

    def guarded(self, task_profile, task_id):
        if task_id in self._initialized_events:
            return original(self, task_profile, task_id)
        return slow_build(self, task_profile, task_id)

    monkeypatch.setattr(flow_base.BaseFlow, "_get_or_build_event", guarded)
    results = []

    def worker():
        barrier.wait()
        results.append(flow.get_initialized_event(profile))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(built) == 1
    assert all(r is results[0] for r in results)
