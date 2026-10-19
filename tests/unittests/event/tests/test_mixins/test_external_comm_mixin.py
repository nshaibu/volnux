"""
Tests for `ExternalCommunicationMixin`.

Scope:
  * communicate — default no-op returns None.
  * wait_for_condition — phase guard (must be COMMUNICATING), enqueue_checkpoint,
    raises ExternalCommunicationSuspensionRequest with request_type=CONDITION.
  * wait_for_event — phase guard, enqueue_checkpoint, raises
    ExternalCommunicationSuspensionRequest with request_type=EVENT, propagates
    event_type and event_filter.
  * request_human_input — phase guard, enqueue_checkpoint, raises
    ExternalCommunicationSuspensionRequest with request_type=HITL, propagates
    payload and options.
  * All three suspension methods reject calls outside the COMMUNICATING phase
    with a RuntimeError.
  * All three methods generate a unique request_id per call.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

def _import_mixin_and_types():
    """Try common module paths for the external-comm mixin."""
    import importlib
    candidates = [
        "volnux.mixins._external_communication",
        "volnux.mixins.external_communication",
        "volnux.mixins.event_external_communication",
    ]
    last_err = None
    for path in candidates:
        try:
            mod = importlib.import_module(path)
            if hasattr(mod, "ExternalCommunicationMixin"):
                return mod.ExternalCommunicationMixin, mod.ExternalCommunicationType
        except ImportError as e:
            last_err = e
    # Fallback: maybe it lives inside volnux.mixins.event
    try:
        from volnux.mixins.event import ExternalCommunicationMixin, ExternalCommunicationType  # type: ignore
        return ExternalCommunicationMixin, ExternalCommunicationType
    except ImportError:
        pass
    raise ImportError(
        f"Could not import ExternalCommunicationMixin from any of {candidates} "
        f"or volnux.mixins.event. Last error: {last_err!r}."
    )


# ---------------------------------------------------------------------------
# communicate — default no-op
# ---------------------------------------------------------------------------

class TestCommunicate:

    @pytest.mark.asyncio
    async def test_default_returns_none(self, event):
        result = await event.communicate()
        assert result is None

    @pytest.mark.asyncio
    async def test_default_with_args_returns_none(self, event):
        result = await event.communicate("arg1", "arg2", key="v")
        assert result is None


# ---------------------------------------------------------------------------
# Shared helpers for the three suspension methods
# ---------------------------------------------------------------------------

def _set_phase(event, phase_name: str):
    """Helper: set event._phase to a named EventPhase."""
    from volnux.execution.rehydrator.event.snapshot import EventPhase
    event._phase = EventPhase[phase_name]


# ---------------------------------------------------------------------------
# wait_for_condition
# ---------------------------------------------------------------------------

class TestWaitForCondition:

    @pytest.mark.asyncio
    async def test_raises_runtime_error_when_not_communicating(self, event):
        _set_phase(event, "PROCESSING")
        with pytest.raises(RuntimeError, match="communicate"):
            await event.wait_for_condition(
                condition_fn=lambda: True,
                title="t",
            )

    @pytest.mark.asyncio
    async def test_raises_runtime_error_when_phase_is_initialized(self, event):
        _set_phase(event, "INITIALIZED")
        with pytest.raises(RuntimeError, match="communicate"):
            await event.wait_for_condition(
                condition_fn=lambda: True,
                title="t",
            )

    @pytest.mark.asyncio
    async def test_enqueues_checkpoint_before_suspending(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")
        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()) as mock_enqueue:
            with pytest.raises(ExternalCommunicationSuspensionRequest):
                await event.wait_for_condition(
                    condition_fn=lambda: True,
                    title="wait for file",
                )
            mock_enqueue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_external_communication_suspension_request(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _, ExternalCommunicationType = _import_mixin_and_types()
        _set_phase(event, "COMMUNICATING")
        cond = lambda: True  # noqa: E731

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.wait_for_condition(
                    condition_fn=cond,
                    title="wait for file",
                    description="path s3://...",
                    timeout_hours=2,
                    poll_interval_seconds=15.0,
                )

        req = exc_info.value
        assert req.request_type == ExternalCommunicationType.CONDITION
        assert req.title == "wait for file"
        assert req.description == "path s3://..."
        assert req.payload == {}
        assert req.options == []
        assert req.timeout_hours == 2
        assert req.condition_fn is cond
        assert req.poll_interval_seconds == 15.0

    @pytest.mark.asyncio
    async def test_request_id_is_unique_uuid(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        ids = []
        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            for _ in range(3):
                with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                    await event.wait_for_condition(
                        condition_fn=lambda: True,
                        title="t",
                    )
                ids.append(exc_info.value.request_id)

        # All request_ids should be unique
        assert len(set(ids)) == 3
        # And each should parse as a UUID
        for rid in ids:
            uuid.UUID(rid)  # raises ValueError if not a valid UUID

    @pytest.mark.asyncio
    async def test_defaults_for_optional_args(self, event):
        """title is required, but description defaults to "",
        timeout_hours to None, poll_interval_seconds to 30.0."""
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.wait_for_condition(
                    condition_fn=lambda: True,
                    title="t",
                )

        req = exc_info.value
        assert req.description == ""
        assert req.timeout_hours is None
        assert req.poll_interval_seconds == 30.0


# ---------------------------------------------------------------------------
# wait_for_event
# ---------------------------------------------------------------------------

class TestWaitForEvent:

    @pytest.mark.asyncio
    async def test_raises_runtime_error_when_not_communicating(self, event):
        _set_phase(event, "PROCESSING")
        with pytest.raises(RuntimeError, match="communicate"):
            await event.wait_for_event(
                event_type="payment.confirmed",
                title="t",
                description="d",
            )

    @pytest.mark.asyncio
    async def test_enqueues_checkpoint_before_suspending(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")
        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()) as mock_enqueue:
            with pytest.raises(ExternalCommunicationSuspensionRequest):
                await event.wait_for_event(
                    event_type="x.y",
                    title="t",
                    description="d",
                )
            mock_enqueue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_external_communication_suspension_request(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _, ExternalCommunicationType = _import_mixin_and_types()
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.wait_for_event(
                    event_type="ml.training.completed",
                    title="wait for ML",
                    description="training job done",
                    event_filter={"job_id": "j-123"},
                    timeout_hours=12,
                )

        req = exc_info.value
        assert req.request_type == ExternalCommunicationType.EVENT
        assert req.title == "wait for ML"
        assert req.description == "training job done"
        assert req.event_type == "ml.training.completed"
        assert req.event_filter == {"job_id": "j-123"}
        assert req.payload == {}
        assert req.options == []
        assert req.timeout_hours == 12

    @pytest.mark.asyncio
    async def test_event_filter_defaults_to_empty_dict(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.wait_for_event(
                    event_type="x.y",
                    title="t",
                    description="d",
                )

        assert exc_info.value.event_filter == {}

    @pytest.mark.asyncio
    async def test_timeout_hours_defaults_to_none(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.wait_for_event(
                    event_type="x.y",
                    title="t",
                    description="d",
                )

        assert exc_info.value.timeout_hours is None


# ---------------------------------------------------------------------------
# request_human_input
# ---------------------------------------------------------------------------

class TestRequestHumanInput:

    @pytest.mark.asyncio
    async def test_raises_runtime_error_when_not_communicating(self, event):
        _set_phase(event, "PROCESSING")
        with pytest.raises(RuntimeError, match="communicate"):
            await event.request_human_input(
                title="t",
                description="d",
                payload={},
            )

    @pytest.mark.asyncio
    async def test_enqueues_checkpoint_before_suspending(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")
        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()) as mock_enqueue:
            with pytest.raises(ExternalCommunicationSuspensionRequest):
                await event.request_human_input(
                    title="t",
                    description="d",
                    payload={"order_id": "123"},
                )
            mock_enqueue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_external_communication_suspension_request(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _, ExternalCommunicationType = _import_mixin_and_types()
        _set_phase(event, "COMMUNICATING")

        payload = {"order": {"id": 1, "amount": 200_000}}
        options = ["approve", "reject"]

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.request_human_input(
                    title="approve high-value order",
                    description="order 1 exceeds threshold",
                    payload=payload,
                    options=options,
                    timeout_hours=4,
                )

        req = exc_info.value
        assert req.request_type == ExternalCommunicationType.HITL
        assert req.title == "approve high-value order"
        assert req.description == "order 1 exceeds threshold"
        assert req.payload == payload
        assert req.options == options
        assert req.timeout_hours == 4

    @pytest.mark.asyncio
    async def test_options_default_to_none(self, event):
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.request_human_input(
                    title="t",
                    description="d",
                    payload={},
                )

        assert exc_info.value.options is None

    @pytest.mark.asyncio
    async def test_timeout_hours_defaults_to_24(self, event):
        """Default timeout_hours is 24 per source signature."""
        from volnux.exceptions import ExternalCommunicationSuspensionRequest
        _set_phase(event, "COMMUNICATING")

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(ExternalCommunicationSuspensionRequest) as exc_info:
                await event.request_human_input(
                    title="t",
                    description="d",
                    payload={},
                )

        assert exc_info.value.timeout_hours == 24
