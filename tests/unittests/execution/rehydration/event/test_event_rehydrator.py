"""Tests for volnux.execution.rehydrator.event_rehydrator — EventRehydrator.

Covers:
  - rehydrate() reconstructs event from snapshot.
  - rehydrate() restores all snapshot fields (phase, retry_count, exec_status, exec_result, external_resources, call_args, retry_policy, attribs).
  - rehydrate() handles None exec_result (no deserialization called).
  - rehydrate() handles empty external_resources (no deserialization called).
  - rehydrate() handles empty attribs (no setattr calls).
  - rehydrate() handles event without retry_policy.
  - rehydrate() passes class_path to import_class.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from volnux.event.base import EventBase
from volnux.execution.rehydrator.event.rehydrator import EventRehydrator
from volnux.mixins.event import RetryPolicy
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    EventPhase,
)
from volnux.execution.rehydrator.event.deserializer import StateDeserializer


def _make_snapshot(**overrides: Any) -> EventCheckpointSnapshot:
    """Build an EventCheckpointSnapshot with sensible defaults."""
    defaults: Dict[str, Any] = {
        "task_id": "task-001",
        "class_path": "test.events.ProcessDataTask",
        "phase": EventPhase.PROCESSING,
        "init_args": {
            "execution_context_id": "ctx-001",
            "task_id": "task-001",
            "stop_condition": ["NEVER"],
            "run_bypass_event_checks": False,
            "options": None,
            "sequence_number": None,
            "kwargs": {},
            "previous_result": [],
        },
        "call_args": {"args": ["input_data"], "kwargs": {"mode": "fast"}},
        "external_resources": {
            "db_cursor": {
                "resource_name": "db_cursor",
                "data": {"offset": 42},
                "provider_path": "providers.CursorProvider",
            }
        },
        "attribs": {"_page": 3, "_buffer": ["item1", "item2"]},
        "timestamp": 1700000000.0,
        "exec_status": True,
        "exec_result": {"output": 99},
        "retry_count": 2,
        "max_retry_attempts": 5,
    }
    defaults.update(overrides)
    return EventCheckpointSnapshot(**defaults)


@pytest.fixture()
def rehydrator() -> EventRehydrator:
    return EventRehydrator(deserializer=StateDeserializer)


# ===========================================================================
# Real events, not mocks: the rehydrator is tested on what it does to an event.

_REHYDRATOR = "volnux.execution.rehydrator.event.rehydrator"


class RehydratorTargetEvent(EventBase):
    """An event whose class-level retry policy differs from the snapshot's."""

    retry_policy = RetryPolicy(max_attempts=9)

    def process(self, *args, **kwargs):
        return True, "done"


class NoPolicyRehydratorEvent(EventBase):
    retry_policy = None

    def process(self, *args, **kwargs):
        return True, "done"


async def rehydrate(rehydrator, snapshot, event_class=RehydratorTargetEvent):
    """Rehydrate ``snapshot`` as ``event_class``: class resolution and the
    (database-backed) init and exec-result deserialization are stood in for."""
    with patch(f"{_REHYDRATOR}.import_class", return_value=event_class), patch.object(
        StateDeserializer,
        "deserialize_init_args",
        return_value={"task_id": "task-001", "execution_context": None},
    ), patch.object(
        StateDeserializer, "deserialize_exec_result", side_effect=lambda value: value
    ):
        return await rehydrator.rehydrate(snapshot)


class TestEventRehydratorRehydrate:

    @pytest.mark.asyncio
    async def test_rehydrate_restores_all_fields(
        self, rehydrator: EventRehydrator
    ) -> None:
        snapshot = _make_snapshot()
        with patch(
            f"{_REHYDRATOR}.import_class", return_value=RehydratorTargetEvent
        ) as import_class, patch.object(
            StateDeserializer,
            "deserialize_init_args",
            return_value={"task_id": "task-001", "execution_context": None},
        ), patch.object(
            StateDeserializer, "deserialize_exec_result", side_effect=lambda v: v
        ):
            event = await rehydrator.rehydrate(snapshot)

        import_class.assert_called_once_with("test.events.ProcessDataTask")
        assert isinstance(event, RehydratorTargetEvent)

        # phase and retry state
        assert event._phase == EventPhase.PROCESSING
        assert event._retry_count == 2
        assert event.retry_policy.max_attempts == 5

        # execution state
        assert event.exec_status is True
        assert event.exec_result == {"output": 99}

        # call arguments: positional apart from keyword
        assert event._call_positional == ("input_data",)
        assert event._call_args == {"mode": "fast"}

        # external resources stay as saved states, restored on demand
        assert event._external_resources == snapshot.external_resources

        # user-bound attributes
        assert event._page == 3
        assert event._buffer == ["item1", "item2"]

    @pytest.mark.asyncio
    async def test_the_snapshots_retry_limit_does_not_leak_to_the_class(
        self, rehydrator: EventRehydrator
    ) -> None:
        # the class-level policy is shared by every instance of the event
        event = await rehydrate(rehydrator, _make_snapshot(max_retry_attempts=5))
        assert event.retry_policy.max_attempts == 5
        assert RehydratorTargetEvent.retry_policy.max_attempts == 9
        other = RehydratorTargetEvent(None, "other")
        assert other.retry_policy.max_attempts == 9

    @pytest.mark.asyncio
    async def test_none_exec_result_skips_deserialization(
        self, rehydrator: EventRehydrator
    ) -> None:
        snapshot = _make_snapshot(exec_result=None)
        with patch(
            f"{_REHYDRATOR}.import_class", return_value=RehydratorTargetEvent
        ), patch.object(
            StateDeserializer,
            "deserialize_init_args",
            return_value={"task_id": "task-001", "execution_context": None},
        ), patch.object(
            StateDeserializer, "deserialize_exec_result", side_effect=lambda v: v
        ) as deserialize:
            event = await rehydrator.rehydrate(snapshot)

        # (it is also what the call arguments go through, so look at what it got)
        seen = [c.args[0] for c in deserialize.call_args_list]
        assert None not in seen and seen == ["input_data", "fast"]
        assert event.exec_result is None

    @pytest.mark.asyncio
    async def test_empty_external_resources_are_not_set(
        self, rehydrator: EventRehydrator
    ) -> None:
        event = await rehydrate(rehydrator, _make_snapshot(external_resources={}))
        assert (
            event._external_resources
            == RehydratorTargetEvent(None, "x")._external_resources
        )

    @pytest.mark.asyncio
    async def test_empty_attribs_set_nothing(self, rehydrator: EventRehydrator) -> None:
        event = await rehydrate(rehydrator, _make_snapshot(attribs={}))
        fresh = RehydratorTargetEvent(None, "x")
        # the only difference from a new event is its own copy of the retry policy
        assert set(vars(event)) - set(vars(fresh)) == {"retry_policy"}
        assert event._phase == EventPhase.PROCESSING

    @pytest.mark.asyncio
    async def test_no_retry_policy_is_safe(self, rehydrator: EventRehydrator) -> None:
        event = await rehydrate(
            rehydrator, _make_snapshot(), event_class=NoPolicyRehydratorEvent
        )
        # (nothing to replace the limit on, and nothing raised)
        assert event.retry_policy is None
        assert event._retry_count == 2

    @pytest.mark.asyncio
    async def test_attribs_are_restored_on_the_instance(
        self, rehydrator: EventRehydrator
    ) -> None:
        """They are there when the runtime pushes execution forward."""
        snapshot = _make_snapshot(
            attribs={"_accumulated": [1, 2, 3], "_mode": "batch"},
            phase=EventPhase.PROCESSING,
        )
        event = await rehydrate(rehydrator, snapshot)

        assert event._accumulated == [1, 2, 3]
        assert event._mode == "batch"
        assert event._phase == EventPhase.PROCESSING

    @pytest.mark.asyncio
    async def test_a_later_attribute_does_not_survive_a_second_rehydration(
        self, rehydrator: EventRehydrator
    ) -> None:
        first = await rehydrate(rehydrator, _make_snapshot(attribs={"_only_here": 1}))
        second = await rehydrate(rehydrator, _make_snapshot(attribs={}))
        assert first._only_here == 1 and not hasattr(second, "_only_here")
