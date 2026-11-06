"""ExecutionCoordinator control-flow handling (exception ladder)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.exceptions import StopProcessingError, SuspendTask
from volnux.execution.coordinator import ExecutionCoordinator, ExecutionError
from volnux.result import EventResult


def make_context(**requests):
    ctx = MagicMock()
    ctx.update_status_async = AsyncMock()
    ctx.bulk_update_async = AsyncMock()
    ctx.cancel_async = AsyncMock()
    ctx.failed_async = AsyncMock()
    ctx.paused_async = AsyncMock()
    ctx.get_stop_processing_request.return_value = requests.get("stop")
    ctx.get_suspension_request.return_value = requests.get("suspend")
    ctx.get_switch_request.return_value = None
    ctx.workflow_name = "wf"
    return ctx


def make_flow(result=None, error=None):
    flow = MagicMock()

    async def run():
        if error is not None:
            raise error
        future = asyncio.get_running_loop().create_future()
        future.set_result(
            result
            or EventResult(error=False, task_id="t", event_name="E", content="ok")
        )
        return future

    flow.run = run
    flow.close = AsyncMock()
    return flow


async def execute(ctx, flow):
    with patch("volnux.execution.coordinator.setup_execution_flow", return_value=flow):
        return await ExecutionCoordinator(ctx).execute_async()


@pytest.mark.asyncio
async def test_a_requested_stop_cancels_the_execution_instead_of_failing_it():
    """StopProcessingError subclasses RuntimeError: the generic RuntimeError
    handler used to catch it first, marking the run FAILED and raising."""
    stop = StopProcessingError("stop condition met")
    ctx = make_context(stop=stop)

    results, errors = await execute(ctx, make_flow())  # must not raise

    ctx.cancel_async.assert_awaited_once()
    ctx.failed_async.assert_not_awaited()
    assert len(results) == 1


@pytest.mark.asyncio
async def test_genuine_runtime_errors_still_fail_the_execution():
    ctx = make_context()
    with pytest.raises(ExecutionError):
        await execute(ctx, make_flow(error=RuntimeError("executor exploded")))
    ctx.failed_async.assert_awaited_once()
    ctx.cancel_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_value_errors_still_fail_the_execution():
    ctx = make_context()
    with pytest.raises(ExecutionError):
        await execute(ctx, make_flow(error=ValueError("bad config")))
    ctx.failed_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_suspension_pauses_the_execution():
    task = MagicMock()
    task._phase = "COMMUNICATING"
    ctx = make_context(
        suspend=SuspendTask(task, suspension_type=SuspendTask.SuspensionType.HITL)
    )

    await execute(ctx, make_flow())

    ctx.paused_async.assert_awaited_once()
    ctx.failed_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_flow_is_always_closed():
    flow = make_flow(error=RuntimeError("boom"))
    with pytest.raises(ExecutionError):
        await execute(make_context(), flow)
    flow.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Timeout, outer cancellation, setup failure, cleanup
# ---------------------------------------------------------------------------

from volnux.exceptions import SwitchTask, TaskCancelledError  # noqa: E402
from volnux.execution.coordinator import ExecutionTimeoutError  # noqa: E402


def make_pending_flow():
    """A flow whose run() hands back a future that never completes."""
    flow = MagicMock()
    holder = {}

    async def run():
        holder["future"] = asyncio.get_running_loop().create_future()
        return holder["future"]

    flow.run = run
    flow.close = AsyncMock()
    flow.cancel = AsyncMock()
    flow.holder = holder
    return flow


@pytest.mark.asyncio
async def test_the_timeout_covers_the_wait_for_the_task_result():
    """flow.run() returns once the task is *submitted*; the timeout used to
    wrap only that, so a task that never finished hung the coordinator."""
    ctx = make_context()
    flow = make_pending_flow()
    with patch("volnux.execution.coordinator.setup_execution_flow", return_value=flow):
        coordinator = ExecutionCoordinator(ctx, timeout=0.1)
        with pytest.raises(ExecutionTimeoutError):
            await asyncio.wait_for(coordinator.execute_async(), timeout=5)

    ctx.failed_async.assert_awaited_once()
    flow.cancel.assert_awaited()  # the running task is told to stop
    flow.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_task_finishing_inside_the_timeout_is_not_disturbed():
    ctx = make_context()
    flow = make_flow()
    flow.cancel = AsyncMock()
    with patch("volnux.execution.coordinator.setup_execution_flow", return_value=flow):
        results, errors = await ExecutionCoordinator(ctx, timeout=5).execute_async()
    assert len(results) == 1
    flow.cancel.assert_not_awaited()
    ctx.failed_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelling_the_coordinator_cancels_the_running_tasks():
    ctx = make_context()
    flow = make_pending_flow()
    with patch("volnux.execution.coordinator.setup_execution_flow", return_value=flow):
        task = asyncio.ensure_future(ExecutionCoordinator(ctx).execute_async())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    flow.cancel.assert_awaited()
    flow.close.assert_awaited_once()
    assert ctx._active_flow is None


@pytest.mark.asyncio
async def test_a_flow_that_cannot_be_set_up_fails_the_execution():
    ctx = make_context()
    with patch(
        "volnux.execution.coordinator.setup_execution_flow",
        side_effect=KeyError("no flow for this context"),
    ):
        with pytest.raises(ExecutionError):
            await ExecutionCoordinator(ctx).execute_async()
    ctx.failed_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_failing_close_does_not_mask_the_original_error():
    ctx = make_context()
    flow = make_flow(error=RuntimeError("executor exploded"))
    flow.close = AsyncMock(side_effect=OSError("close failed"))
    with pytest.raises(ExecutionError, match="executor exploded"):
        await execute(ctx, flow)


@pytest.mark.asyncio
async def test_a_failing_close_does_not_fail_a_successful_run():
    ctx = make_context()
    flow = make_flow()
    flow.close = AsyncMock(side_effect=OSError("close failed"))
    results, _ = await execute(ctx, flow)
    assert len(results) == 1


@pytest.mark.asyncio
async def test_unexpected_errors_fail_the_execution_and_propagate_unchanged():
    ctx = make_context()
    with pytest.raises(KeyError):
        await execute(ctx, make_flow(error=KeyError("odd")))
    ctx.failed_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_the_active_flow_is_published_during_the_run_and_cleared_after():
    ctx = make_context()
    seen = {}
    flow = make_flow()
    inner = flow.run

    async def run():
        seen["during"] = ctx._active_flow
        return await inner()

    flow.run = run
    await execute(ctx, flow)
    assert seen["during"] is flow
    assert ctx._active_flow is None

    ctx2 = make_context()
    with pytest.raises(ExecutionError):
        await execute(ctx2, make_flow(error=RuntimeError("x")))
    assert ctx2._active_flow is None


# ---------------------------------------------------------------------------
# Cancelled tasks, error results, switch handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_cancelled_task_cancels_the_context_without_raising():
    ctx = make_context()
    flow = make_flow(error=None, result=asyncio.CancelledError())

    # a future resolving to CancelledError is what gather() hands back
    async def run():
        future = asyncio.get_running_loop().create_future()
        future.cancel()
        return future

    flow.run = run
    results, errors = await execute(ctx, flow)

    from volnux.execution.context import ExecutionStatus

    assert ctx.bulk_update_async.await_args.args[0] == ExecutionStatus.CANCELLED
    ctx.cancel_async.assert_awaited_once_with(propagate=False)
    ctx.failed_async.assert_not_awaited()
    assert any(isinstance(e, TaskCancelledError) for e in errors)


@pytest.mark.asyncio
async def test_task_errors_become_error_results_and_are_recorded():
    ctx = make_context()

    async def run():
        future = asyncio.get_running_loop().create_future()
        future.set_exception(ValueError("task blew up"))
        return future

    flow = make_flow()
    flow.run = run
    results, errors = await execute(ctx, flow)

    assert len(errors) == 1
    assert any(r.error for r in results)
    status, recorded_errors, recorded_results = ctx.bulk_update_async.await_args.args
    assert list(recorded_errors) == list(errors)


def _switch_context(descriptor_known):
    ctx = make_context()
    switch = SwitchTask(
        "t", 2, EventResult(error=False, task_id="t", event_name="E", content="s")
    )
    ctx.get_switch_request.return_value = switch
    profile = MagicMock()
    profile.get_descriptor.return_value = object() if descriptor_known else None
    ctx.get_decision_task_profile.return_value = profile
    return ctx, switch


@pytest.mark.asyncio
async def test_a_switch_to_a_configured_descriptor_is_marked_configured():
    ctx, switch = _switch_context(True)
    results, _ = await execute(ctx, make_flow())
    assert switch.descriptor_configured is True
    assert switch.result in list(results)
    ctx.cancel_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_switch_to_an_unknown_descriptor_cancels_without_touching_tasks():
    """The tasks have finished by then: cancelling must not send them commands."""
    ctx, switch = _switch_context(False)
    await execute(ctx, make_flow())
    assert switch.descriptor_configured is False
    ctx.cancel_async.assert_awaited_once_with(propagate=False)


# ---------------------------------------------------------------------------
# cancel()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_stops_the_flow_and_emits_the_cancelled_signal():
    ctx = make_context()
    coordinator = ExecutionCoordinator(ctx)
    flow = MagicMock()
    flow.cancel = AsyncMock()
    coordinator._flow = flow

    await coordinator.cancel()

    flow.cancel.assert_awaited_once()
    ctx.cancel_async.assert_awaited_once_with(propagate=False)


@pytest.mark.asyncio
async def test_cancel_without_a_flow_is_a_no_op():
    ctx = make_context()
    await ExecutionCoordinator(ctx).cancel()
    ctx.cancel_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_after_the_run_does_not_touch_the_closed_flow():
    ctx = make_context()
    flow = make_flow()
    flow.cancel = AsyncMock()
    coordinator = ExecutionCoordinator(ctx)
    with patch("volnux.execution.coordinator.setup_execution_flow", return_value=flow):
        await coordinator.execute_async()
    await coordinator.cancel()
    flow.cancel.assert_not_awaited()
