"""AsyncTaskWorkflowExecutor: runs a workflow in this process, under a context."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.conf import override_settings
from volnux.context import (
    get_current_correlation_id,
    get_current_execution_id,
    get_current_workflow_id,
    volnux_context,
)
from volnux.engine.workflows.trigger.executors.asyncio_executor import (
    AsyncTaskWorkflowExecutor,
)
from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
from volnux.engine.workflows.workflow import WorkflowExecutionError


def make_registry(run_workflow=None, names=("wf",), ready=True):
    registry = MagicMock()
    registry.is_ready.return_value = ready
    config = MagicMock()
    config.run_workflow = run_workflow or AsyncMock(return_value="done")
    registry.get_workflow_config.side_effect = lambda n: config if n in names else None
    registry.config = config
    return registry


def test_it_is_a_workflow_executor():
    assert issubclass(AsyncTaskWorkflowExecutor, WorkflowExecutor)


class TestSettings:
    def test_defaults_come_from_the_settings(self):
        executor = AsyncTaskWorkflowExecutor(make_registry())
        assert executor._max_concurrent == 10 and executor._timeout is None

    def test_the_settings_are_used(self):
        with override_settings(
            ASYNC_EXECUTOR_MAX_CONCURRENT=3, ASYNC_EXECUTOR_TIMEOUT=12
        ):
            executor = AsyncTaskWorkflowExecutor(make_registry())
        assert executor._max_concurrent == 3 and executor._timeout == 12.0

    def test_arguments_win_over_the_settings(self):
        with override_settings(ASYNC_EXECUTOR_MAX_CONCURRENT=3):
            executor = AsyncTaskWorkflowExecutor(
                make_registry(), max_concurrent=7, timeout=1
            )
        assert executor._max_concurrent == 7 and executor._timeout == 1

    @pytest.mark.parametrize("bad", [0, -1])
    def test_max_concurrent_must_be_positive(self, bad):
        with pytest.raises(ValueError):
            AsyncTaskWorkflowExecutor(make_registry(), max_concurrent=bad)


class TestExecute:
    @pytest.mark.asyncio
    async def test_it_returns_the_result(self):
        registry = make_registry()
        assert (
            await AsyncTaskWorkflowExecutor(registry).execute("wf", {"a": 1}) == "done"
        )
        registry.config.run_workflow.assert_awaited_once_with(
            params={"a": 1}, run_type="single"
        )

    @pytest.mark.asyncio
    async def test_run_type_is_taken_out_of_the_params_and_not_mutated(self):
        registry = make_registry()
        params = {"run_type": "batch", "k": "v"}
        await AsyncTaskWorkflowExecutor(registry).execute("wf", params)
        registry.config.run_workflow.assert_awaited_once_with(
            params={"k": "v"}, run_type="batch"
        )
        assert params == {"run_type": "batch", "k": "v"}

    @pytest.mark.asyncio
    async def test_calling_it_executes(self):
        assert await AsyncTaskWorkflowExecutor(make_registry())("wf", {}) == "done"

    @pytest.mark.asyncio
    async def test_a_registry_that_is_not_ready(self):
        executor = AsyncTaskWorkflowExecutor(make_registry(ready=False))
        with pytest.raises(WorkflowExecutionError, match="not ready"):
            await executor.execute("wf", {})

    @pytest.mark.asyncio
    async def test_an_unknown_workflow(self):
        with pytest.raises(WorkflowExecutionError, match="not registered"):
            await AsyncTaskWorkflowExecutor(make_registry()).execute("nope", {})

    @pytest.mark.asyncio
    async def test_a_failure_is_a_workflow_execution_error_and_counted(self):
        registry = make_registry(AsyncMock(side_effect=RuntimeError("boom")))
        executor = AsyncTaskWorkflowExecutor(registry)
        with pytest.raises(WorkflowExecutionError, match="boom"):
            await executor.execute("wf", {})
        assert executor.get_stats()["failed"] == 1
        assert executor.get_stats()["active_count"] == 0

    @pytest.mark.asyncio
    async def test_a_workflow_execution_error_passes_through(self):
        registry = make_registry(AsyncMock(side_effect=WorkflowExecutionError("own")))
        with pytest.raises(WorkflowExecutionError, match="^own$"):
            await AsyncTaskWorkflowExecutor(registry).execute("wf", {})

    @pytest.mark.asyncio
    async def test_a_timeout_cancels_the_workflow(self):
        cancelled = []

        async def slow(params, run_type):
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        executor = AsyncTaskWorkflowExecutor(
            make_registry(AsyncMock(side_effect=slow)), timeout=0.05
        )
        with pytest.raises(WorkflowExecutionError, match="timed out"):
            await executor.execute("wf", {})
        assert cancelled == [True]
        assert executor.get_stats()["failed"] == 1

    @pytest.mark.asyncio
    async def test_the_concurrency_limit_holds(self):
        running = peak = 0

        async def work(params, run_type):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.02)
            running -= 1

        executor = AsyncTaskWorkflowExecutor(
            make_registry(AsyncMock(side_effect=work)), max_concurrent=2
        )
        await asyncio.gather(*(executor.execute("wf", {}) for _ in range(6)))
        assert peak == 2
        assert executor.get_stats()["completed"] == 6


class TestContext:
    @pytest.mark.asyncio
    async def test_the_workflow_runs_under_its_own_context(self):
        seen = {}

        async def work(params, run_type):
            seen.update(
                workflow=get_current_workflow_id(),
                execution=get_current_execution_id(),
                correlation=get_current_correlation_id(),
            )

        await AsyncTaskWorkflowExecutor(
            make_registry(AsyncMock(side_effect=work))
        ).execute("wf", {})
        assert seen["workflow"] == "wf"
        assert seen["execution"] and seen["correlation"] == seen["execution"]
        # and nothing is left behind in the caller
        assert get_current_workflow_id() is None and get_current_execution_id() is None

    @pytest.mark.asyncio
    async def test_the_callers_correlation_id_is_kept(self):
        seen = []

        async def work(params, run_type):
            seen.append(get_current_correlation_id())

        executor = AsyncTaskWorkflowExecutor(make_registry(AsyncMock(side_effect=work)))
        with volnux_context(correlation_id="request-1"):
            await executor.execute("wf", {})
        assert seen == ["request-1"]

    @pytest.mark.asyncio
    async def test_concurrent_workflows_do_not_see_each_others_context(self):
        seen = {}

        async def work(params, run_type):
            await asyncio.sleep(0.01)
            seen[params["n"]] = (get_current_workflow_id(), get_current_execution_id())

        registry = make_registry(AsyncMock(side_effect=work), names=("a", "b"))
        executor = AsyncTaskWorkflowExecutor(registry)
        await asyncio.gather(
            executor.execute("a", {"n": 1}), executor.execute("b", {"n": 2})
        )
        assert seen[1][0] == "a" and seen[2][0] == "b"
        assert seen[1][1] != seen[2][1]


class TestBackgroundAndCancel:
    @pytest.mark.asyncio
    async def test_a_background_workflow_runs_and_is_tracked(self):
        gate = asyncio.Event()

        async def work(params, run_type):
            await gate.wait()
            return get_current_workflow_id()

        executor = AsyncTaskWorkflowExecutor(make_registry(AsyncMock(side_effect=work)))
        execution_id = await executor.execute_background("wf", {})
        await asyncio.sleep(0.01)
        stats = executor.get_stats()
        assert stats["active_count"] == 1
        assert stats["active_tasks"][0]["execution_id"] == execution_id
        gate.set()
        await asyncio.sleep(0.05)
        assert executor.get_stats()["active_count"] == 0
        assert executor.get_stats()["completed"] == 1

    @pytest.mark.asyncio
    async def test_cancel_execution_stops_it(self):
        started = asyncio.Event()

        async def work(params, run_type):
            started.set()
            await asyncio.sleep(10)

        executor = AsyncTaskWorkflowExecutor(make_registry(AsyncMock(side_effect=work)))
        execution_id = await executor.execute_background("wf", {})
        await started.wait()
        assert await executor.cancel_execution(execution_id) is True
        await asyncio.sleep(0.01)
        assert executor.get_stats()["active_count"] == 0

    @pytest.mark.asyncio
    async def test_cancel_of_an_unknown_execution(self):
        assert (
            await AsyncTaskWorkflowExecutor(make_registry()).cancel_execution("x")
            is False
        )

    @pytest.mark.asyncio
    async def test_cancel_workflow_and_cancel_all(self):
        started = []

        async def work(params, run_type):
            started.append(1)
            await asyncio.sleep(10)

        executor = AsyncTaskWorkflowExecutor(make_registry(AsyncMock(side_effect=work)))
        for _ in range(3):
            await executor.execute_background("wf", {})
        await asyncio.sleep(0.02)
        assert await executor.cancel_workflow("wf") == 3
        assert await executor.cancel_workflow("wf") == 0
        await executor.execute_background("wf", {})
        await asyncio.sleep(0.02)
        assert await executor.cancel_all() == 1
