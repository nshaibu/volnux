"""What steps_runner tells the coordinator about control-flow exceptions.

A goto, a requested stop, a suspension and a bypass are not failures, so none
of them may be reported as FAILED/ERROR (or logged as an error).
"""

import asyncio
import logging
import uuid
from unittest.mock import MagicMock

import pytest

from volnux import EventBase
from volnux.exceptions import (
    ExternalCommunicationSuspensionRequest,
    StopProcessingError,
    SuspendTask,
    SwitchTask,
)
from volnux.flows.bridge.communications.tasks import MessageType, TaskState


class RecordingChannel:
    def __init__(self):
        self.messages = []

    async def send_message(self, message):
        self.messages.append(message)

    async def receive_command(self, timeout=0.0):
        await asyncio.sleep(timeout)
        raise asyncio.TimeoutError

    def states(self):
        return [m.payload.get("state") for m in self.messages]

    def types(self):
        return [m.message_type for m in self.messages]


def make(process):
    cls = type("Reporting" + uuid.uuid4().hex[:8], (EventBase,), {"process": process})
    channel = RecordingChannel()
    return cls(MagicMock(), "task-1", command_channel=channel), channel


async def run_expecting(event, exc_type):
    with pytest.raises(exc_type):
        await event()


@pytest.mark.asyncio
async def test_a_normal_run_reports_completed():
    async def process(self, *a, **k):
        return True, "ok"

    event, channel = make(process)
    await event()
    assert channel.states()[-1] == TaskState.COMPLETED.value
    assert MessageType.ERROR not in channel.types()


@pytest.mark.asyncio
async def test_goto_is_reported_as_completed_not_failed(caplog):
    async def process(self, *a, **k):
        self.goto(1, True, "to-branch-1")

    event, channel = make(process)
    with caplog.at_level(logging.ERROR):
        await run_expecting(event, SwitchTask)

    assert TaskState.FAILED.value not in channel.states()
    assert MessageType.ERROR not in channel.types()
    assert channel.states()[-1] == TaskState.COMPLETED.value
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_a_suspension_is_reported_as_suspended_not_failed(caplog):
    async def communicate(self, *a, **k):
        await self.request_human_input(title="t", description="d", payload={})

    async def process(self, *a, **k):
        return True, "unreachable"

    cls = type(
        "Suspending", (EventBase,), {"process": process, "communicate": communicate}
    )
    channel = RecordingChannel()
    event = cls(MagicMock(), "task-1", command_channel=channel)

    with caplog.at_level(logging.ERROR):
        await run_expecting(event, ExternalCommunicationSuspensionRequest)

    assert TaskState.FAILED.value not in channel.states()
    assert channel.states()[-1] == TaskState.SUSPENDED.value
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_a_requested_stop_is_reported_as_cancelled_not_failed(caplog):
    from volnux.parser.options import StopCondition

    async def process(self, *a, **k):
        return True, "done"

    cls = type("Stopping" + uuid.uuid4().hex[:8], (EventBase,), {"process": process})
    channel = RecordingChannel()
    event = cls(
        MagicMock(),
        "task-1",
        command_channel=channel,
        stop_condition=StopCondition.ON_SUCCESS,
    )

    with caplog.at_level(logging.ERROR):
        await run_expecting(event, StopProcessingError)

    assert TaskState.FAILED.value not in channel.states()
    assert channel.states()[-1] == TaskState.CANCELLED.value


@pytest.mark.asyncio
async def test_a_bypass_is_completed_and_not_logged_as_an_error(caplog):
    async def bypass(self):
        return True, "skipped-data"

    async def process(self, *a, **k):
        return True, "unreachable"

    cls = type(
        "Bypassing" + uuid.uuid4().hex[:8],
        (EventBase,),
        {"process": process, "bypass": bypass},
    )
    channel = RecordingChannel()
    event = cls(MagicMock(), "task-1", command_channel=channel)

    with caplog.at_level(logging.ERROR):
        result = await event()

    assert result.content == "skipped-data"
    assert channel.states()[-1] == TaskState.COMPLETED.value
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_a_real_error_outside_process_is_still_reported_as_failed():
    async def communicate(self, *a, **k):
        raise RuntimeError("communication broke")

    async def process(self, *a, **k):
        return True, "unreachable"

    cls = type(
        "Broken" + uuid.uuid4().hex[:8],
        (EventBase,),
        {"process": process, "communicate": communicate},
    )
    channel = RecordingChannel()
    event = cls(MagicMock(), "task-1", command_channel=channel)

    await run_expecting(event, RuntimeError)

    assert TaskState.FAILED.value in channel.states()
    assert MessageType.ERROR in channel.types()
