"""Pipeline.stop() / pause() / resume() reach running tasks through the channels."""

import asyncio
import threading
import time
import uuid
from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux import EventBase, Pipeline
from volnux.execution.context import ExecutionStatus
from volnux.flows import SingleFlow
from volnux.flows.bridge.communications.tasks import CommandType

EXECUTORS = ["default", "thread"]


def unique(name):
    return f"{name}{uuid.uuid4().hex[:8]}"


def make_event(name, process, executor="default", **attrs):
    return type(
        unique(name), (EventBase,), {"process": process, "executor": executor, **attrs}
    )


def make_pipeline(pointy):
    return type(
        unique("Pipe"), (Pipeline,), {"Meta": type("Meta", (), {"pointy": pointy})}
    )


class Probe:
    """Cross-thread markers (an event may run on a worker thread's own loop)."""

    def __init__(self):
        self.started = threading.Event()
        self.finished = threading.Event()
        self.steps = []

    async def wait_started(self, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not self.started.is_set():
            assert time.monotonic() < deadline, "the event never started"
            await asyncio.sleep(0.01)


def sleeper(probe, seconds=30):
    async def process(self, *a, **k):
        probe.started.set()
        await asyncio.sleep(seconds)
        probe.finished.set()
        return True, "slept"

    return process


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_stop_cancels_a_running_task_promptly(executor):
    probe = Probe()
    event = make_event("LongRunning", sleeper(probe), executor=executor)
    pipeline = make_pipeline(event.__name__)()
    run = asyncio.ensure_future(pipeline.start())
    await probe.wait_started()

    t0 = time.perf_counter()
    await pipeline.stop()
    await asyncio.wait_for(run, 8)

    assert time.perf_counter() - t0 < 4, "stop() should not wait out the 30 s task"
    assert not probe.finished.is_set()
    latest = pipeline.execution_context.get_latest_context()
    assert latest.status == ExecutionStatus.CANCELLED
    assert not [
        r for r in latest.get_results() if r.error
    ]  # cancelled is not a failure


@pytest.mark.asyncio
async def test_stop_prevents_the_next_event_from_starting():
    first, second = Probe(), Probe()

    async def never_runs(self, *a, **k):
        second.started.set()
        return True, "second"

    a = make_event("First", sleeper(first))
    b = make_event("Second", never_runs)
    pipeline = make_pipeline(f"{a.__name__}->{b.__name__}")()
    run = asyncio.ensure_future(pipeline.start())
    await first.wait_started()

    await pipeline.stop()
    await asyncio.wait_for(run, 8)

    assert not second.started.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_pause_holds_the_task_before_its_next_step_and_resume_releases_it(
    executor,
):
    probe = Probe()

    async def communicate(self, *a, **k):
        probe.started.set()
        await asyncio.sleep(0.4)  # steps remain after this: PROCESSING, POST_PROCESS...

    async def process(self, *a, **k):
        probe.steps.append("process")
        probe.finished.set()
        return True, "done"

    event = make_event("Pausable", process, executor=executor, communicate=communicate)
    pipeline = make_pipeline(event.__name__)()
    run = asyncio.ensure_future(pipeline.start())
    await probe.wait_started()

    assert await pipeline.pause() == 1
    await asyncio.sleep(1.0)  # well past the end of communicate()
    assert probe.steps == [], "process() ran although the task was paused"
    assert not run.done()

    await pipeline.resume()
    await asyncio.wait_for(run, 8)
    assert probe.steps == ["process"]


@pytest.mark.asyncio
async def test_stop_works_while_the_task_is_paused():
    probe = Probe()

    async def communicate(self, *a, **k):
        probe.started.set()
        await asyncio.sleep(0.3)

    event = make_event("PausedThenStopped", sleeper(Probe()), communicate=communicate)
    pipeline = make_pipeline(event.__name__)()
    run = asyncio.ensure_future(pipeline.start())
    await probe.wait_started()
    await pipeline.pause()
    await asyncio.sleep(0.6)

    await pipeline.stop()
    await asyncio.wait_for(run, 8)

    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.CANCELLED
    )


@pytest.mark.asyncio
async def test_pause_and_resume_with_nothing_running_are_harmless():
    event = make_event("Idle", sleeper(Probe(), 0))
    pipeline = make_pipeline(event.__name__)()
    assert await pipeline.pause() == 0
    assert await pipeline.resume() == 0
    await pipeline.stop()  # nothing to stop


@pytest.mark.asyncio
async def test_a_pipeline_can_run_again_after_being_stopped():
    probe = Probe()
    event = make_event("Rerunnable", sleeper(probe, 30))
    pipeline = make_pipeline(event.__name__)()
    run = asyncio.ensure_future(pipeline.start())
    await probe.wait_started()
    await pipeline.stop()
    await asyncio.wait_for(run, 8)

    second = Probe()

    async def quick(self, *a, **k):
        second.finished.set()
        return True, "ok"

    event2 = make_event("QuickAfterStop", quick)
    again = make_pipeline(event2.__name__)()
    await again.start()
    assert second.finished.is_set()


# --- flow-level fallbacks (no real executor) ---------------------------------


def make_flow(bridge, futures):
    context = MagicMock()
    context.task_profiles = deque([MagicMock()])
    flow = SingleFlow(context, enable_communication=False)
    flow._comm_bridge = bridge
    flow._task_futures = futures
    return flow


def fake_future(done=False):
    future = MagicMock()
    future.done.return_value = done
    return future


@pytest.mark.asyncio
async def test_cancel_uses_the_channel_when_the_command_is_delivered():
    bridge = MagicMock()
    bridge.send_command = AsyncMock(return_value=True)
    future = fake_future()
    flow = make_flow(bridge, {"t1": future})

    await flow.cancel()

    sent = bridge.send_command.await_args.args[1]
    assert sent.command_type == CommandType.CANCEL
    future.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_falls_back_to_the_future_when_no_channel_took_it():
    bridge = MagicMock()
    bridge.send_command = AsyncMock(return_value=False)
    future = fake_future()
    flow = make_flow(bridge, {"t1": future})

    await flow.cancel()

    future.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_without_any_bridge_cancels_the_futures():
    future = fake_future()
    await make_flow(None, {"t1": future}).cancel()
    future.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_leaves_finished_tasks_alone():
    bridge = MagicMock()
    bridge.send_command = AsyncMock(return_value=True)
    done = fake_future(done=True)
    await make_flow(bridge, {"t1": done}).cancel()
    bridge.send_command.assert_not_awaited()
    done.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_pause_and_resume_count_the_tasks_reached():
    bridge = MagicMock()
    bridge.send_command = AsyncMock(side_effect=[True, False, True])
    flow = make_flow(
        bridge, {"a": fake_future(), "b": fake_future(), "c": fake_future()}
    )

    assert await flow.pause() == 2

    bridge.send_command = AsyncMock(return_value=True)
    assert await flow.resume() == 3
    assert bridge.send_command.await_args.args[1].command_type == CommandType.RESUME


@pytest.mark.asyncio
async def test_pause_without_a_bridge_reaches_nothing():
    assert await make_flow(None, {"a": fake_future()}).pause() == 0


@pytest.mark.asyncio
async def test_one_failing_send_does_not_stop_the_rest():
    bridge = MagicMock()
    bridge.send_command = AsyncMock(side_effect=[RuntimeError("down"), True])
    flow = make_flow(bridge, {"a": fake_future(), "b": fake_future()})
    assert await flow.pause() == 1
