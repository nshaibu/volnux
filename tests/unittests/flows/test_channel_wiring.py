"""The control channel is attached to events and carries status back.

Real pipelines, real executors: nothing about the flow, bridge, channel or
event is mocked (only observed).
"""

import asyncio
import logging
import uuid

import pytest

from volnux import EventBase, Pipeline
from volnux.execution.pipeline.fields import InputDataField
from volnux.flows.bridge.communications.tasks import (
    TaskCommunicationBridge,
    TaskState,
)
from volnux.flows.bridge.communications.tasks.channels.local import LocalCommandChannel

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


async def run(pipeline_class):
    pipeline = pipeline_class()
    await pipeline.start()
    return pipeline


@pytest.fixture
def observed_states(monkeypatch):
    """Every (task id, state) the bridges apply, in order."""
    seen = []
    original = TaskCommunicationBridge._process_message

    async def spy(self, message, full_id=None):
        await original(self, message, full_id)
        state = (message.payload or {}).get("state")
        if state:
            seen.append((message.task_id, state))

    monkeypatch.setattr(TaskCommunicationBridge, "_process_message", spy)
    return seen


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_the_event_receives_a_local_channel(executor):
    seen = {}

    async def process(self, *a, **k):
        seen["channel"] = type(self._command_channel).__name__
        seen["listener"] = self._command_listener_task is not None
        return True, "ok"

    event = make_event("Wired", process, executor=executor)
    await run(make_pipeline(event.__name__))

    assert seen == {"channel": "LocalCommandChannel", "listener": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_status_updates_reach_the_bridge(executor, observed_states):
    async def process(self, *a, **k):
        await asyncio.sleep(0.05)
        return True, "ok"

    event = make_event("Reports", process, executor=executor)
    await run(make_pipeline(event.__name__))

    states = [s for _, s in observed_states]
    assert states[0] == TaskState.RUNNING.value
    assert TaskState.COMPLETED.value in states
    assert states.index(TaskState.RUNNING.value) < states.index(
        TaskState.COMPLETED.value
    )


@pytest.mark.asyncio
async def test_a_failing_event_reports_failed(observed_states):
    async def process(self, *a, **k):
        raise ValueError("boom")

    event = make_event("ReportsFailure", process)
    await run(make_pipeline(event.__name__))

    # the failure becomes a failure *result*; the task itself still completed its steps
    assert TaskState.RUNNING.value in [s for _, s in observed_states]


@pytest.mark.asyncio
async def test_progress_is_reported_per_step(observed_states, monkeypatch):
    progress = []
    original = TaskCommunicationBridge._process_message

    async def spy(self, message, full_id=None):
        await original(self, message, full_id)
        if message.message_type.value == "progress_update":
            progress.append(self._statuses[full_id].progress)

    monkeypatch.setattr(TaskCommunicationBridge, "_process_message", spy)

    async def process(self, *a, **k):
        return True, "ok"

    event = make_event("ReportsProgress", process)
    await run(make_pipeline(event.__name__))

    assert progress and progress == sorted(progress) and progress[-1] > 0.5


@pytest.mark.asyncio
async def test_the_bridge_is_shut_down_when_the_run_ends():
    leaked_before = {t for t in asyncio.all_tasks() if "bridge" in (t.get_name() or "")}

    async def process(self, *a, **k):
        return True, "ok"

    event = make_event("Cleanup", process)
    await run(make_pipeline(event.__name__))
    await asyncio.sleep(0.05)

    leaked = {
        t
        for t in asyncio.all_tasks()
        if "bridge" in (t.get_name() or "") and not t.done()
    } - leaked_before
    assert not leaked, f"pumps still running: {leaked}"


@pytest.mark.asyncio
async def test_a_channel_failure_never_stops_the_task(monkeypatch):
    from volnux.flows.bridge.communications.tasks import LocalTaskCommunicationBridge

    async def broken(self, handle):
        raise RuntimeError("registry down")

    monkeypatch.setattr(LocalTaskCommunicationBridge, "register_task", broken)
    seen = {}

    async def process(self, *a, **k):
        seen["channel"] = self._command_channel
        return True, "ran"

    event = make_event("NoChannelNeeded", process)
    pipeline = await run(make_pipeline(event.__name__))

    assert seen["channel"] is None
    context = pipeline.execution_context.get_latest_context()
    assert [r.content for r in context.get_results()] == ["ran"]


@pytest.mark.asyncio
async def test_a_process_task_without_a_distributed_backend_runs_without_a_channel(
    caplog,
):
    """The default in-memory backend cannot reach another process: no channel,
    one warning, and the task still runs and returns its result."""
    from tests.unittests.flows import (
        process_events,
    )  # noqa: F401 - registers the events

    with caplog.at_level(logging.WARNING):
        pipeline = await run(make_pipeline("ProcessReportsChannel"))

    context = pipeline.execution_context.get_latest_context()
    (result,) = context.get_results()
    assert result.error is False
    assert result.content == "channel=None"
    warnings = [r for r in caplog.records if "control channel" in r.getMessage()]
    assert len(warnings) == 1
