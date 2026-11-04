"""Which event attributes end up in a checkpoint snapshot."""

import asyncio
import logging
import uuid
from unittest.mock import MagicMock

import pytest

from volnux import EventBase
from volnux.execution.rehydrator.event.serializer import StateSerializer
from volnux.execution.rehydrator.event.snapshot import EventPhase


def make_event(process=None, **attrs):
    async def default_process(self, *a, **k):
        return True, "ok"

    cls = type(
        "Snap" + uuid.uuid4().hex[:8],
        (EventBase,),
        {"process": process or default_process, **attrs},
    )
    return cls(MagicMock(), "task-1")


@pytest.mark.asyncio
async def test_transient_runtime_state_is_not_checkpointed(caplog):
    event = make_event()
    await event()

    with caplog.at_level(logging.WARNING):
        attribs = StateSerializer.serialize_attribs(event)

    for transient in ("_active_phase", "_event_result", "_execution_context"):
        assert transient not in attribs, f"{transient} leaked into the snapshot"
    assert not [r for r in caplog.records if "Cannot checkpoint" in r.getMessage()]


@pytest.mark.asyncio
async def test_the_bypass_decision_is_checkpointed():
    async def bypass(self):
        return True, "bypassed"

    event = make_event(bypass=bypass)
    await event()

    assert StateSerializer.serialize_attribs(event)["_bypassed"] is True


@pytest.mark.asyncio
async def test_a_bypassed_event_resumed_from_a_checkpoint_does_no_work():
    calls = []

    async def communicate(self, *a, **k):
        calls.append("communicate")

    async def process(self, *a, **k):
        calls.append("process")
        return True, "should-not-run"

    event = make_event(process=process, communicate=communicate)
    # State as restored from a checkpoint taken right after the bypass decision
    event._phase = EventPhase.PRE_PROCESS
    event._bypassed = True
    event.exec_status = True
    event.exec_result = "bypassed"

    result = await event()

    assert calls == []
    assert result.content == "bypassed" and result.error is False
