"""Tests for volnux.execution.checkpoint.builder — SnapshotBuilder.

Covers:
  - build() creates EventCheckpointSnapshot with all expected fields.
  - build() includes attribs via StateSerializer.serialize_attribs.
  - build() passes max_retry_attempts (not max_attempts).
  - build() handles events without retry_policy.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.execution.rehydrator.event.builder import SnapshotBuilder
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    EventPhase,
)
from volnux.execution.rehydrator.event.serializer import StateSerializer


def _make_mock_event(**overrides: Any) -> MagicMock:
    """Create a mock EventBase with sensible defaults for SnapshotBuilder.build()."""
    event = MagicMock()

    event._task_id = "task-001"
    event.get_phase.return_value = EventPhase.PROCESSING

    # init_args
    event.get_init_args.return_value = {
        "execution_context": None,
        "task_id": "task-001",
        "stop_condition": None,
        "run_bypass_event_checks": False,
        "options": None,
        "sequence_number": None,
        "kwargs": {},
        "previous_result": [],
    }

    # call_args
    event.get_call_args.return_value = {"args": [1, 2], "kwargs": {"key": "val"}}

    # retry
    event._retry_count = 1
    event.retry_policy = MagicMock()
    event.retry_policy.max_attempts = 5

    # exec state
    event._exec_result = {"status": "done"}
    event.exec_status = True

    # external resources
    event._external_resources = {
        "db_cursor": {
            "resource_name": "db_cursor",
            "data": {"offset": 100},
            "provider_path": "myapp.providers.CursorProvider",
        }
    }

    # user attrs
    event.__dict__ = {
        "_page": 3,
        "_filter": "active",
        "init_args": {},
        "call_args": {},
        "external_resources": {},
    }

    event.class_path = "test.events.MockEvent"

    # Apply overrides
    for key, value in overrides.items():
        setattr(event, key, value)

    return event


@pytest.fixture()
def builder() -> SnapshotBuilder:
    return SnapshotBuilder(serializer=StateSerializer)


class TestSnapshotBuilderBuild:

    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_init_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_call_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_exec_result")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_attribs")
    @patch("volnux.execution.checkpoint.builder.get_obj_klass_import_str")
    async def test_build_returns_snapshot(
        self,
        mock_import_str: MagicMock,
        mock_serialize_attribs: MagicMock,
        mock_serialize_exec_result: MagicMock,
        mock_serialize_call_args: MagicMock,
        mock_serialize_init_args: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "test.events.MockEvent"
        mock_serialize_init_args.return_value = {"task_id": "task-001"}
        mock_serialize_call_args.return_value = {"args": [1], "kwargs": {}}
        mock_serialize_exec_result.return_value = {"status": "done"}
        mock_serialize_attribs.return_value = {"_page": 3, "_filter": "active"}

        event = _make_mock_event()
        snapshot = await builder.build(event)

        assert isinstance(snapshot, EventCheckpointSnapshot)
        assert snapshot.task_id == "task-001"
        assert snapshot.class_path == "test.events.MockEvent"
        assert snapshot.phase == EventPhase.PROCESSING
        assert snapshot.retry_count == 1
        assert snapshot.max_retry_attempts == 5
        assert snapshot.exec_status is True
        assert snapshot.exec_result == {"status": "done"}
        assert snapshot.attribs == {"_page": 3, "_filter": "active"}
        assert snapshot.external_resources == event._external_resources

    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_init_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_call_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_exec_result")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_attribs")
    @patch("volnux.execution.checkpoint.builder.get_obj_klass_import_str")
    async def test_no_retry_policy_uses_max_retries(
        self,
        mock_import_str: MagicMock,
        mock_serialize_attribs: MagicMock,
        mock_serialize_exec_result: MagicMock,
        mock_serialize_call_args: MagicMock,
        mock_serialize_init_args: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "test.events.NoRetryEvent"
        mock_serialize_init_args.return_value = {}
        mock_serialize_call_args.return_value = {}
        mock_serialize_exec_result.return_value = None
        mock_serialize_attribs.return_value = {}

        event = _make_mock_event()
        event.retry_policy = None

        snapshot = await builder.build(event)

        # Should use MAX_RETRIES default
        assert snapshot.max_retry_attempts == 3  # MAX_RETRIES constant

    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_init_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_call_args")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_exec_result")
    @patch("volnux.execution.checkpoint.builder.StateSerializer.serialize_attribs")
    @patch("volnux.execution.checkpoint.builder.get_obj_klass_import_str")
    async def test_attribs_called_on_event_instance(
        self,
        mock_import_str: MagicMock,
        mock_serialize_attribs: MagicMock,
        mock_serialize_exec_result: MagicMock,
        mock_serialize_call_args: MagicMock,
        mock_serialize_init_args: MagicMock,
        builder: SnapshotBuilder,
    ) -> None:
        mock_import_str.return_value = "test.events.AttrEvent"
        mock_serialize_init_args.return_value = {}
        mock_serialize_call_args.return_value = {}
        mock_serialize_exec_result.return_value = None
        mock_serialize_attribs.return_value = {"_counter": 7}

        event = _make_mock_event()
        await builder.build(event)

        mock_serialize_attribs.assert_called_once_with(event)
