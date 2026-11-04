"""BaseFlow construction and the executor-specific communication bridge."""

from collections import deque
from unittest.mock import MagicMock

import pytest

from volnux.executors import DefaultExecutor, ProcessPoolExecutor, ThreadPoolExecutor
from volnux.flows import ParallelFlow, SingleFlow, setup_execution_flow
from volnux.flows.stream import StreamFlow
from volnux.flows.bridge.communications.tasks import (
    LocalTaskCommunicationBridge,
    MessagingTaskCommunicationBridge,
)


def make_context(multitask=False, stream=False):
    ctx = MagicMock()
    ctx.is_stream_segment.return_value = stream
    ctx.task_profiles = deque(
        [MagicMock(), MagicMock()] if multitask else [MagicMock()]
    )
    ctx.is_multitask.return_value = multitask
    return ctx


def test_flows_can_be_constructed():
    """BaseFlow used to ask for a bridge of type 'executor_type' and raise."""
    assert isinstance(SingleFlow(make_context()), SingleFlow)
    assert isinstance(ParallelFlow(make_context(multitask=True)), ParallelFlow)


def test_setup_execution_flow_picks_the_flow_by_task_count():
    assert isinstance(setup_execution_flow(make_context()), SingleFlow)
    assert isinstance(setup_execution_flow(make_context(multitask=True)), ParallelFlow)


def test_a_stream_segment_gets_a_stream_flow():
    flow = setup_execution_flow(make_context(multitask=True, stream=True))
    assert isinstance(flow, StreamFlow) and isinstance(flow, ParallelFlow)


def test_a_flow_gets_a_unique_identity():
    a, b = SingleFlow(make_context()), SingleFlow(make_context())
    assert a.id and b.id and a.id != b.id


@pytest.mark.parametrize(
    "executor, bridge",
    [
        (DefaultExecutor, LocalTaskCommunicationBridge),
        (ThreadPoolExecutor, LocalTaskCommunicationBridge),
        (ProcessPoolExecutor, MessagingTaskCommunicationBridge),
        (
            type("CeleryExecutor", (DefaultExecutor,), {}),
            MessagingTaskCommunicationBridge,
        ),
    ],
)
def test_the_bridge_matches_the_executor_kind(executor, bridge):
    flow = SingleFlow(make_context())
    assert flow._comm_bridge is None  # created once the executor is known

    flow._ensure_comm_bridge(executor)

    assert type(flow._comm_bridge) is bridge


def test_the_bridge_is_created_once():
    flow = SingleFlow(make_context())
    flow._ensure_comm_bridge(DefaultExecutor)
    first = flow._comm_bridge
    flow._ensure_comm_bridge(ProcessPoolExecutor)
    assert flow._comm_bridge is first


@pytest.mark.parametrize("name", ["GRPCExecutor", "RemoteExecutor", "XMLRPCExecutor"])
def test_executors_without_a_usable_bridge_get_none(name):
    flow = SingleFlow(make_context())
    flow._ensure_comm_bridge(type(name, (DefaultExecutor,), {}))
    assert flow._comm_bridge is None


def test_communication_can_be_disabled():
    flow = SingleFlow(make_context(), enable_communication=False)
    flow._ensure_comm_bridge(DefaultExecutor)
    assert flow._comm_bridge is None


def test_a_bridge_failure_never_stops_the_flow(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("bridge exploded")

    monkeypatch.setattr("volnux.flows.base.create_communication_bridge", broken)
    flow = SingleFlow(make_context())

    flow._ensure_comm_bridge(DefaultExecutor)  # must not raise

    assert flow._comm_bridge is None


@pytest.mark.asyncio
async def test_a_process_pool_created_by_the_flow_is_shut_down_on_close(monkeypatch):
    """Process pools used to be excluded from cleanup, leaking a pool of worker
    processes on every dispatch. (With sharing switched off, each flow owns its pool.)
    """
    from volnux.executors import shared as shared_pools
    from volnux.parser.executor_config import ExecutorInitializerConfig

    monkeypatch.setattr(shared_pools, "enabled", lambda: False)

    flow = SingleFlow(make_context())
    pool = flow._initialize_executor(
        ProcessPoolExecutor, ExecutorInitializerConfig(max_workers=1)
    )

    await flow.close()

    with pytest.raises(RuntimeError):
        pool.submit(print)  # "cannot schedule new futures after shutdown"


@pytest.mark.asyncio
async def test_a_shared_executor_instance_is_left_running_on_close():
    from volnux.parser.executor_config import ExecutorInitializerConfig

    shared = ThreadPoolExecutor(max_workers=1)
    try:
        flow = SingleFlow(make_context())
        returned = flow._initialize_executor(shared, ExecutorInitializerConfig())
        assert returned is shared

        await flow.close()

        assert shared.submit(lambda: 42).result(timeout=5) == 42  # still usable
    finally:
        shared.shutdown()


@pytest.mark.asyncio
async def test_a_parallel_group_honours_an_executor_chosen_in_pointy():
    flow = ParallelFlow(make_context(multitask=True))
    decision = MagicMock()
    decision.options.executor = "thread"
    flow.context.get_decision_task_profile.return_value = decision

    assert await flow.get_flow_executor() is ThreadPoolExecutor


@pytest.mark.asyncio
async def test_a_parallel_group_defaults_to_the_process_pool():
    flow = ParallelFlow(make_context(multitask=True))
    decision = MagicMock()
    decision.options = None
    flow.context.get_decision_task_profile.return_value = decision

    assert await flow.get_flow_executor() is ProcessPoolExecutor


# --- running an event on a worker -------------------------------------------


def test_cancelling_an_event_on_a_worker_raises_an_ordinary_exception():
    """CancelledError is a BaseException. Celery re-raises BaseException out of
    its task machinery (it only handles Exception), which can take the worker
    down; a thread/process pool would also report it oddly. The worker wrapper
    translates it into TaskCancelledError."""
    import asyncio

    from volnux.exceptions import TaskCancelledError
    from volnux.flows.base import _run_event_sync

    async def cancelled_event(**kwargs):
        raise asyncio.CancelledError()

    with pytest.raises(TaskCancelledError) as caught:
        _run_event_sync(cancelled_event, {})

    assert isinstance(caught.value, Exception)  # not a BaseException


def test_other_exceptions_pass_through_the_worker_wrapper_unchanged():
    from volnux.flows.base import _run_event_sync

    async def failing_event(**kwargs):
        raise KeyError("missing")

    with pytest.raises(KeyError):
        _run_event_sync(failing_event, {})
