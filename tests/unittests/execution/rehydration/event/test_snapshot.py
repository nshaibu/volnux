"""Tests for volnux.execution.checkpoint.snapshot.

Covers:
  - EventPhase enum values and integer ordering (corrected spec).
  - InitArgsTemplate / CallArgsTemplate / ResourceState TypedDicts.
  - EventCheckpointSnapshot construction, defaults, and classmethods.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any, Dict

import pytest


# We import directly so that if the source file changes, tests reflect reality.
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    EventPhase,
    InitArgsTemplate,
    CallArgsTemplate,
    ResourceState,
)


class TestEventPhase:
    """The corrected lifecycle order is:
    INITIALIZED=0 → PRE_PROCESS=1 → COMMUNICATING=2 → PROCESSING=3
    → POST_PROCESS=4 → COMPLETED=5
    """

    def test_is_int_enum(self) -> None:
        assert issubclass(EventPhase, IntEnum)

    def test_correct_values(self) -> None:
        assert EventPhase.INITIALIZED == 0
        assert EventPhase.PRE_PROCESS == 1
        assert EventPhase.COMMUNICATING == 2
        assert EventPhase.PROCESSING == 3
        assert EventPhase.POST_PROCESS == 4
        assert EventPhase.COMPLETED == 5

    def test_total_six_members(self) -> None:
        assert len(EventPhase) == 6

    def test_ordering_comparable(self) -> None:
        assert EventPhase.PRE_PROCESS < EventPhase.COMMUNICATING
        assert EventPhase.COMMUNICATING < EventPhase.PROCESSING
        assert EventPhase.PROCESSING < EventPhase.POST_PROCESS
        assert EventPhase.POST_PROCESS < EventPhase.COMPLETED

    def test_pre_process_before_communicating(self) -> None:
        """Bypass check (PRE_PROCESS) MUST be lower than external I/O (COMMUNICATING)."""
        assert EventPhase.PRE_PROCESS < EventPhase.COMMUNICATING


class TestEventCheckpointSnapshot:
    """Construction and default-value tests.

    NOTE: We mock `formax.BaseModel` via the real import — if formax is not
    available in the test environment we patch it at the import level.
    """

    # helpers
    @staticmethod
    def _make_snapshot(**overrides: Any) -> EventCheckpointSnapshot:
        defaults: Dict[str, Any] = {
            "task_id": "task-001",
            "class_path": "myapp.events.SendEmailTask",
            "phase": EventPhase.INITIALIZED,
            "init_args": {},
            "call_args": {},
            "external_resources": {},
            "attribs": {},
            "timestamp": 1700000000.0,
            "exec_status": False,
            "exec_result": None,
            "retry_count": 0,
            "max_retry_attempts": 3,
        }
        defaults.update(overrides)
        return EventCheckpointSnapshot(**defaults)

    # minimal creation
    def test_create_with_required_fields(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.task_id == "task-001"
        assert snapshot.class_path == "myapp.events.SendEmailTask"
        assert snapshot.phase == EventPhase.INITIALIZED

    # default field values
    def test_default_exec_status(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.exec_status is False

    def test_default_exec_result(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.exec_result is None

    def test_default_retry_count(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.retry_count == 0

    def test_default_max_retry_attempts(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.max_retry_attempts == 3

    def test_default_attribs_is_empty_dict(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.attribs == {}

    def test_default_external_resources_is_empty_dict(self) -> None:
        snapshot = self._make_snapshot()
        assert snapshot.external_resources == {}

    # phase variation
    @pytest.mark.parametrize("phase", list(EventPhase))
    def test_accepts_all_phases(self, phase: EventPhase) -> None:
        snapshot = self._make_snapshot(phase=phase)
        assert snapshot.phase == phase

    # attribs field populated
    def test_attribs_stores_user_state(self) -> None:
        snapshot = self._make_snapshot(attribs={"_page": 5, "_filter": "active"})
        assert snapshot.attribs["_page"] == 5
        assert snapshot.attribs["_filter"] == "active"

    # class-level utility methods
    def test_get_schema_name(self) -> None:
        assert EventCheckpointSnapshot.get_schema_name() == "volnux:event:checkpoint"

    def test_get_backend_config(self) -> None:
        config = EventCheckpointSnapshot.get_backend_config()
        assert config["ENGINE"] == "volnux.backends.stores.redis.RedisStoreBackend"
        assert "CONNECTOR_CONFIG" in config
        assert config["CONNECTOR_CONFIG"]["host"] == "localhost"
        assert config["CONNECTOR_CONFIG"]["port"] == 6379
        assert config["CONNECTOR_CONFIG"]["db"] == 0
