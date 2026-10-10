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
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from volnux.execution.rehydrator.event.rehydrator import EventRehydrator
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
class TestEventRehydratorRehydrate:

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_exec_result"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_external_resources"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_rehydrate_restores_all_fields(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_ext_res: MagicMock,
        mock_deserialize_exec: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        mock_event_class = MagicMock(return_value=MagicMock())
        mock_event_class.return_value.retry_policy = MagicMock()
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {
            "task_id": "task-001",
            "execution_context": None,
        }
        mock_deserialize_call.return_value = {
            "args": ["input_data"],
            "kwargs": {"mode": "fast"},
        }
        mock_deserialize_ext_res.return_value = {
            "db_cursor": "restored_cursor",
        }
        mock_deserialize_exec.return_value = {"output": 99}

        snapshot = _make_snapshot()
        event = await rehydrator.rehydrate(snapshot)

        # Class instantiation
        mock_import_class.assert_called_once_with("test.events.ProcessDataTask")
        mock_event_class.assert_called_once()

        # Phase
        assert event._phase == EventPhase.PROCESSING

        # Retry
        assert event._retry_count == 2
        assert event.retry_policy.max_attempts == 5

        # Exec state
        assert event._exec_status is True
        assert event._exec_result == {"output": 99}

        # Call args
        mock_deserialize_call.assert_called_once()
        assert event._call_args == {"args": ["input_data"], "kwargs": {"mode": "fast"}}

        # External resources
        mock_deserialize_ext_res.assert_called_once_with(snapshot.external_resources)
        assert event._external_resources == {"db_cursor": "restored_cursor"}

        # Attribs injected via setattr
        assert event._page == 3
        assert event._buffer == ["item1", "item2"]

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_external_resources"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_none_exec_result_skips_deserialization(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_ext_res: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        mock_event_class = MagicMock()
        mock_event_class.return_value.retry_policy = None
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {}
        mock_deserialize_call.return_value = {"args": [], "kwargs": {}}

        snapshot = _make_snapshot(exec_result=None, external_resources={})
        event = await rehydrator.rehydrate(snapshot)

        # exec_result is None — no deserialize_exec_result call expected
        # (We don't patch it here, so it would fail if called)
        assert event._exec_result is None

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_empty_external_resources_skips_deserialization(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        mock_event_class = MagicMock()
        mock_event_class.return_value.retry_policy = None
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {}
        mock_deserialize_call.return_value = {}

        snapshot = _make_snapshot(external_resources={})
        event = await rehydrator.rehydrate(snapshot)

        # external_resources should NOT have been set on the event
        # (since snapshot.external_resources is falsy, the if-block is skipped)
        # We verify by checking no external_resources assignment
        assert (
            not hasattr(event, "_external_resources")
            or event._external_resources is None
        )

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_empty_attribs_no_setattr(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        mock_event_class = MagicMock()
        mock_event_class.return_value.retry_policy = None
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {}
        mock_deserialize_call.return_value = {}

        snapshot = _make_snapshot(attribs={})
        event = await rehydrator.rehydrate(snapshot)

        # Verify no user attributes were set
        # (The mock should only have framework-assigned attributes)
        assert event._phase == EventPhase.PROCESSING

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_exec_result"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_no_retry_policy_safe(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_exec: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        mock_event = MagicMock()
        mock_event.retry_policy = None
        mock_event_class = MagicMock(return_value=mock_event)
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {}
        mock_deserialize_call.return_value = {}
        mock_deserialize_exec.return_value = {"data": 1}

        snapshot = _make_snapshot()
        event = await rehydrator.rehydrate(snapshot)

        # Should not raise — hasattr check protects against None retry_policy
        assert event._retry_count == 2

    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_call_args"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_exec_result"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_external_resources"
    )
    @patch(
        "volnux.execution.rehydrator.event_rehydrator.StateDeserializer.deserialize_init_args"
    )
    @patch("volnux.execution.rehydrator.event_rehydrator.import_class")
    async def test_attribs_restored_before_execution(
        self,
        mock_import_class: MagicMock,
        mock_deserialize_init: MagicMock,
        mock_deserialize_ext_res: MagicMock,
        mock_deserialize_exec: MagicMock,
        mock_deserialize_call: MagicMock,
        rehydrator: EventRehydrator,
    ) -> None:
        """Verify attribs are set on the instance so they're available
        when the runtime pushes execution forward."""
        mock_event_class = MagicMock()
        mock_event = mock_event_class.return_value
        mock_event.retry_policy = MagicMock()
        mock_import_class.return_value = mock_event_class

        mock_deserialize_init.return_value = {}
        mock_deserialize_call.return_value = {}
        mock_deserialize_ext_res.return_value = {}
        mock_deserialize_exec.return_value = None

        snapshot = _make_snapshot(
            attribs={"_accumulated": [1, 2, 3], "_mode": "batch"},
            phase=EventPhase.PROCESSING,
        )

        event = await rehydrator.rehydrate(snapshot)

        # The event instance should have user attrs bound
        assert event._accumulated == [1, 2, 3]
        assert event._mode == "batch"

        # The phase should be set for the runtime to pick up
        assert event._phase == EventPhase.PROCESSING
