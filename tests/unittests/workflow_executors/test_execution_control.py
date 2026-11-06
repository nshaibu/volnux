"""Pausing, resuming and stopping a run by the caller's execution id, and being told
when it ends."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.engine.workflows.trigger.executors.asyncio_executor import (
    AsyncTaskWorkflowExecutor,
)
from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
from volnux.engine.workflows.workflow import WorkflowExecutionError


class FakePipeline:
    def __init__(self, release):
        self.release = release
        self.calls = []

    async def pause(self):
        self.calls.append("pause")

    async def resume(self):
        self.calls.append("resume")

    async def stop(self):
        self.calls.append("stop")
        self.release.set()  # a stopped run winds down


def make(run=None, fail=None):
    """An executor whose workflow builds a FakePipeline and waits to be released."""
    release = asyncio.Event()
    pipelines = []

    async def run_workflow(params, run_type, on_pipeline=None):
        pipeline = FakePipeline(release)
        pipelines.append(pipeline)
        if on_pipeline is not None:
            on_pipeline(pipeline)
        await release.wait()
        if fail is not None:
            raise fail
        return pipeline

    registry = MagicMock()
    registry.is_ready.return_value = True
    config = MagicMock()
    config.run_workflow = run or run_workflow
    registry.get_workflow_config.side_effect = lambda n: config if n == "wf" else None
    executor = AsyncTaskWorkflowExecutor(registry)
    return executor, release, pipelines


async def until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


class Recorder:
    def __init__(self):
        self.events = []

    def __call__(self, execution_id, state, error=None):
        self.events.append((execution_id, state, error))


class TestWhoCanControl:
    def test_the_base_class_cannot(self):
        assert WorkflowExecutor.supports_control is False

    @pytest.mark.asyncio
    async def test_the_base_class_refuses(self):
        class Plain(WorkflowExecutor):
            async def execute(self, workflow_name, params, *, execution_id=None):
                return None

        for method in ("pause_execution", "resume_execution", "stop_execution"):
            with pytest.raises(NotImplementedError):
                await getattr(Plain(MagicMock()), method)("x")

    def test_celery_and_kubernetes_cannot(self):
        from volnux.engine.workflows.trigger.executors.celery import (
            CeleryWorkflowExecutor,
        )
        from volnux.engine.workflows.trigger.executors.kubernetes import (
            KubernetesWorkflowExecutor,
        )

        assert CeleryWorkflowExecutor.supports_control is False
        assert KubernetesWorkflowExecutor.supports_control is False

    def test_the_asyncio_executor_can(self):
        assert AsyncTaskWorkflowExecutor.supports_control is True


class TestControl:
    @pytest.mark.asyncio
    async def test_the_given_id_is_the_runs_id_and_the_run_can_be_controlled(self):
        executor, release, pipelines = make()
        task = asyncio.create_task(executor.execute("wf", {}, execution_id="exec-1"))
        await until(lambda: pipelines)
        assert "exec-1" in executor._records_by_id

        assert await executor.pause_execution("exec-1") is True
        assert await executor.resume_execution("exec-1") is True
        assert pipelines[0].calls == ["pause", "resume"]

        release.set()
        await task

    @pytest.mark.asyncio
    async def test_unknown_runs_are_not_controlled(self):
        executor, _, _ = make()
        for method in ("pause_execution", "resume_execution", "stop_execution"):
            assert await getattr(executor, method)("nope") is False

    @pytest.mark.asyncio
    async def test_a_run_without_a_given_id_is_not_held(self):
        executor, release, pipelines = make()
        task = asyncio.create_task(executor.execute("wf", {}))
        await asyncio.sleep(0.05)
        assert executor._pipelines == {}  # (nothing was handed to the executor)
        release.set()
        await task

    @pytest.mark.asyncio
    async def test_the_same_id_cannot_run_twice(self):
        executor, release, pipelines = make()
        task = asyncio.create_task(executor.execute("wf", {}, execution_id="e"))
        await until(lambda: pipelines)
        with pytest.raises(ValueError, match="already running"):
            await executor.execute("wf", {}, execution_id="e")
        release.set()
        await task

    @pytest.mark.asyncio
    async def test_the_pipeline_is_forgotten_when_the_run_ends(self):
        executor, release, pipelines = make()
        task = asyncio.create_task(executor.execute("wf", {}, execution_id="e"))
        await until(lambda: pipelines)
        release.set()
        await task
        assert executor._pipelines == {}
        assert await executor.pause_execution("e") is False
        # ...and the id can be used again
        again, release2, p2 = make()
        release2.set()
        await again.execute("wf", {}, execution_id="e")


class TestListeners:
    @pytest.mark.asyncio
    async def test_completed(self):
        executor, release, pipelines = make()
        seen = Recorder()
        executor.add_listener(seen)
        release.set()
        await executor.execute("wf", {}, execution_id="e")
        assert seen.events == [("e", "completed", None)]

    @pytest.mark.asyncio
    async def test_failed(self):
        executor, release, _ = make(fail=RuntimeError("boom"))
        seen = Recorder()
        executor.add_listener(seen)
        release.set()
        with pytest.raises(WorkflowExecutionError):
            await executor.execute("wf", {}, execution_id="e")
        [(eid, state, error)] = seen.events
        assert (eid, state) == ("e", "failed") and "boom" in error

    @pytest.mark.asyncio
    async def test_stopped(self):
        executor, release, pipelines = make()
        seen = Recorder()
        executor.add_listener(seen)
        task = asyncio.create_task(executor.execute("wf", {}, execution_id="e"))
        await until(lambda: pipelines)
        assert await executor.stop_execution("e") is True
        await task
        assert pipelines[0].calls == ["stop"]
        assert seen.events == [("e", "stopped", None)]

    @pytest.mark.asyncio
    async def test_cancelled_counts_as_stopped(self):
        executor, release, pipelines = make()
        seen = Recorder()
        executor.add_listener(seen)
        task = asyncio.create_task(executor.execute("wf", {}, execution_id="e"))
        await until(lambda: pipelines)
        assert await executor.cancel_execution("e")
        with pytest.raises(WorkflowExecutionError):
            await task
        assert [e[:2] for e in seen.events] == [("e", "stopped")]

    @pytest.mark.asyncio
    async def test_runs_without_a_given_id_are_not_reported(self):
        executor, release, _ = make()
        seen = Recorder()
        executor.add_listener(seen)
        release.set()
        await executor.execute("wf", {})
        assert seen.events == []

    @pytest.mark.asyncio
    async def test_async_listeners_and_a_failing_one(self):
        executor, release, _ = make()
        got = []

        async def good(execution_id, state, error):
            got.append(state)

        def bad(execution_id, state, error):
            raise RuntimeError("listener bug")

        executor.add_listener(bad)
        executor.add_listener(good)
        executor.add_listener(good)  # (no duplicates)
        release.set()
        assert await executor.execute("wf", {}, execution_id="e")
        assert got == ["completed"]
        executor.remove_listener(good)
        assert executor._listeners == [bad]


class TestWorkflowConfigHook:
    @pytest.mark.asyncio
    async def test_on_pipeline_receives_the_pipeline_before_it_starts(self):
        from volnux.engine.workflows.workflow import WorkflowConfig

        order = []

        class Pipe:
            def __init__(self, **params):
                order.append("built")

            async def start(self, force_rerun=False):
                order.append("started")

        class Config(WorkflowConfig):
            def ready(self):
                return None

        config = Config.__new__(Config)
        config.get_pipeline_class = lambda: Pipe
        pipeline = await config._run_single(
            {}, on_pipeline=lambda p: order.append("hook")
        )
        assert order == ["built", "hook", "started"] and isinstance(pipeline, Pipe)
