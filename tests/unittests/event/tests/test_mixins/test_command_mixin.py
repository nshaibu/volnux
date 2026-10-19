"""
Tests for `EventCommandMixin`.

Scope:
  * _send_update — no-op when no channel; otherwise builds TaskMessage payload
    with state.value + extra; send errors swallowed (logged).
  * _command_listener — returns immediately when no channel; otherwise polls
    receive_command(timeout=0.01) and delegates to _handle_command; passes
    CancelledError through; swallows other exceptions.
  * _handle_command — dispatch on command_type:
    - PAUSE clears gate, sends PAUSED status
    - RESUME sets gate, sends RUNNING status
    - CANCEL cancels _main_worker_task (no status update)
    - UPDATE_PRIORITY sets _preempted, sends RUNNING with priority+preempted
      extra, then cancels worker
    - CHECKPOINT enqueues checkpoint, sends RUNNING with checkpoint_saved
    - QUERY_STATUS sends PAUSED/RUNNING based on gate state
    - unknown command → warning log
  * _run_step — async step awaited; sync step offloaded via to_thread.
  * steps_runner — happy path (all steps succeed, COMPLETED status);
    pause/resume; phase-skip; per-step error reporting; final checkpoint;
    preemption → SuspendTask; hard cancellation → CANCELLED status;
    unknown exception → FAILED status (no double-report).
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

def _import_mixin_and_types():
    from volnux.mixins.event import EventCommandMixin
    from volnux.flows.bridge.communications.tasks import (
        CommandType,
        MessageType,
        TaskState,
        TaskCommand,
    )
    return EventCommandMixin, CommandType, MessageType, TaskState, TaskCommand


def _make_command(command_type, payload=None):
    """Build a TaskCommand-like object."""
    _, CommandType, _, _, TaskCommand = _import_mixin_and_types()
    return TaskCommand(
        task_id="test-task-id",
        command_type=command_type,
        payload=payload,
    )


# ---------------------------------------------------------------------------
# _send_update
# ---------------------------------------------------------------------------

class TestSendUpdate:

    @pytest.mark.asyncio
    async def test_no_channel_is_noop(self, event):
        event._command_channel = None
        # Should not raise
        await event._send_update(
            MessageType := _import_mixin_and_types()[2],
            _import_mixin_and_types()[3].RUNNING,
        )

    @pytest.mark.asyncio
    async def test_sends_message_with_state_value(self, event, command_channel):
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        await event._send_update(MessageType.STATUS_UPDATE, TaskState.RUNNING)
        # The mock's send_message side_effect was a coroutine; check it was awaited.
        command_channel.send_message.assert_called_once()
        sent_msg = command_channel.send_message.call_args.args[0]
        assert sent_msg.task_id == event._task_id
        assert sent_msg.message_type == MessageType.STATUS_UPDATE
        assert sent_msg.payload == {"state": TaskState.RUNNING.value}

    @pytest.mark.asyncio
    async def test_extra_payload_merged_into_state(self, event, command_channel):
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        await event._send_update(
            MessageType.STATUS_UPDATE, TaskState.RUNNING,
            extra={"step": 2, "total": 5},
        )
        sent_msg = command_channel.send_message.call_args.args[0]
        assert sent_msg.payload == {
            "state": TaskState.RUNNING.value,
            "step": 2,
            "total": 5,
        }

    @pytest.mark.asyncio
    async def test_send_exception_swallowed(self, event, command_channel, caplog):
        """If channel.send_message raises, _send_update logs but doesn't
        propagate."""
        import logging
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        command_channel.send_message = AsyncMock(side_effect=RuntimeError("send boom"))
        event._command_channel = command_channel
        with caplog.at_level(logging.ERROR):
            # Should not raise
            await event._send_update(MessageType.STATUS_UPDATE, TaskState.RUNNING)
        assert any("send boom" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# _command_listener
# ---------------------------------------------------------------------------

class TestCommandListener:

    @pytest.mark.asyncio
    async def test_no_channel_returns_immediately(self, event):
        event._command_channel = None
        # Should return without raising
        await event._command_listener()

    @pytest.mark.asyncio
    async def test_with_channel_loops_and_handles_commands(self, event, command_channel):
        """The listener polls receive_command and calls _handle_command."""
        # Make receive_command yield one command then raise CancelledError
        sentinel_cmd = MagicMock(name="command")
        command_channel.receive_command = AsyncMock(
            side_effect=[sentinel_cmd, asyncio.CancelledError()],
        )
        event._command_channel = command_channel

        with patch.object(event, "_handle_command", new=AsyncMock()) as mock_handle:
            with pytest.raises(asyncio.CancelledError):
                await event._command_listener()
            mock_handle.assert_awaited_once_with(sentinel_cmd)

    @pytest.mark.asyncio
    async def test_timeout_is_swallowed_and_loop_continues(self, event, command_channel):
        """asyncio.TimeoutError from receive_command is swallowed; loop
        continues until cancelled."""
        sentinel_cmd = MagicMock(name="command")
        command_channel.receive_command = AsyncMock(
            side_effect=[
                asyncio.TimeoutError(),
                asyncio.TimeoutError(),
                sentinel_cmd,
                asyncio.CancelledError(),
            ]
        )
        event._command_channel = command_channel

        with patch.object(event, "_handle_command", new=AsyncMock()) as mock_handle:
            with pytest.raises(asyncio.CancelledError):
                await event._command_listener()
            # _handle_command called once with the one command that came through
            mock_handle.assert_awaited_once_with(sentinel_cmd)

    @pytest.mark.asyncio
    async def test_unexpected_exception_logged_loop_continues(self, event, command_channel, caplog):
        """Non-timeout, non-cancel exceptions are logged but loop continues."""
        import logging
        sentinel_cmd = MagicMock(name="command")
        command_channel.receive_command = AsyncMock(
            side_effect=[
                RuntimeError("transient"),
                sentinel_cmd,
                asyncio.CancelledError(),
            ]
        )
        event._command_channel = command_channel

        with patch.object(event, "_handle_command", new=AsyncMock()):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(asyncio.CancelledError):
                    await event._command_listener()
            assert any("transient" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self, event, command_channel):
        command_channel.receive_command = AsyncMock(side_effect=asyncio.CancelledError())
        event._command_channel = command_channel
        with pytest.raises(asyncio.CancelledError):
            await event._command_listener()


# ---------------------------------------------------------------------------
# _handle_command
# ---------------------------------------------------------------------------

class TestHandleCommand:

    @pytest.mark.asyncio
    async def test_pause_clears_gate_and_sends_paused(self, event, command_channel):
        _, CommandType, MessageType, TaskState, TaskCommand = _import_mixin_and_types()
        event._command_channel = command_channel
        event._pause_gate.set()  # start in running state
        await event._handle_command(_make_command(CommandType.PAUSE))
        assert event._pause_gate.is_set() is False
        # Verify a STATUS_UPDATE / PAUSED message was sent
        sent = command_channel.send_message.call_args.args[0]
        assert sent.message_type == MessageType.STATUS_UPDATE
        assert sent.payload == {"state": TaskState.PAUSED.value}

    @pytest.mark.asyncio
    async def test_resume_sets_gate_and_sends_running(self, event, command_channel):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._pause_gate.clear()
        await event._handle_command(_make_command(CommandType.RESUME))
        assert event._pause_gate.is_set() is True
        sent = command_channel.send_message.call_args.args[0]
        assert sent.payload == {"state": TaskState.RUNNING.value}

    @pytest.mark.asyncio
    async def test_cancel_cancels_main_worker_task(self, event, command_channel):
        _, CommandType, _, _, _ = _import_mixin_and_types()
        event._command_channel = command_channel

        # _main_worker_task is an asyncio.Task — we need a real one to cancel.
        async def _long_running():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise

        event._main_worker_task = asyncio.create_task(_long_running())
        try:
            await event._handle_command(_make_command(CommandType.CANCEL))
            # Give the cancellation a tick to propagate
            await asyncio.sleep(0)
            assert event._main_worker_task.cancelled() or event._main_worker_task.done()
        finally:
            if not event._main_worker_task.done():
                event._main_worker_task.cancel()
                try:
                    await event._main_worker_task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_update_priority_sets_preempted_and_cancels_worker(self, event, command_channel):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._preempted = False

        async def _long_running():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise

        event._main_worker_task = asyncio.create_task(_long_running())
        try:
            await event._handle_command(
                _make_command(CommandType.UPDATE_PRIORITY, payload={"priority": 5})
            )
            assert event._preempted is True
            # Verify the status update carried priority + preempted extras
            sent = command_channel.send_message.call_args.args[0]
            assert sent.payload == {
                "state": TaskState.RUNNING.value,
                "priority": 5,
                "preempted": True,
            }
            # Worker cancelled
            await asyncio.sleep(0)
            assert event._main_worker_task.cancelled() or event._main_worker_task.done()
        finally:
            if not event._main_worker_task.done():
                event._main_worker_task.cancel()
                try:
                    await event._main_worker_task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_update_priority_with_no_payload(self, event, command_channel):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._preempted = False

        async def _long_running():
            await asyncio.sleep(3600)

        event._main_worker_task = asyncio.create_task(_long_running())
        try:
            await event._handle_command(_make_command(CommandType.UPDATE_PRIORITY))
            assert event._preempted is True
            sent = command_channel.send_message.call_args.args[0]
            assert sent.payload["priority"] is None
            assert sent.payload["preempted"] is True
        finally:
            event._main_worker_task.cancel()
            try:
                await event._main_worker_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_checkpoint_enqueues_and_sends_running_with_checkpoint_saved(
        self, event, command_channel
    ):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()) as mock_enqueue:
            await event._handle_command(_make_command(CommandType.CHECKPOINT))
            mock_enqueue.assert_awaited_once()
            sent = command_channel.send_message.call_args.args[0]
            assert sent.payload == {
                "state": TaskState.RUNNING.value,
                "checkpoint_saved": True,
            }

    @pytest.mark.asyncio
    async def test_query_status_when_gate_set_sends_running(self, event, command_channel):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._pause_gate.set()
        await event._handle_command(_make_command(CommandType.QUERY_STATUS))
        sent = command_channel.send_message.call_args.args[0]
        assert sent.payload == {"state": TaskState.RUNNING.value}

    @pytest.mark.asyncio
    async def test_query_status_when_gate_clear_sends_paused(self, event, command_channel):
        _, CommandType, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._pause_gate.clear()
        await event._handle_command(_make_command(CommandType.QUERY_STATUS))
        sent = command_channel.send_message.call_args.args[0]
        assert sent.payload == {"state": TaskState.PAUSED.value}

    @pytest.mark.asyncio
    async def test_unknown_command_logs_warning(self, event, command_channel, caplog):
        import logging
        _, CommandType, _, _, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        # Build a command with an unknown command_type
        unknown_cmd = MagicMock()
        unknown_cmd.command_type = "UNKNOWN"
        with caplog.at_level(logging.WARNING):
            await event._handle_command(unknown_cmd)
        assert any("unknown" in r.getMessage().lower() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_handler_exception_does_not_propagate(self, event, command_channel, caplog):
        """If _send_update raises inside _handle_command, the exception is
        caught and logged (not propagated)."""
        import logging
        _, CommandType, _, _, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        # Make _send_update raise — but actually _send_update itself swallows.
        # So simulate an exception by patching the gate access.
        with patch.object(
            type(event._pause_gate), "clear", side_effect=RuntimeError("gate boom")
        ):
            with caplog.at_level(logging.ERROR):
                # Should not raise
                await event._handle_command(_make_command(CommandType.PAUSE))
            assert any("gate boom" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# _run_step
# ---------------------------------------------------------------------------

class TestRunStep:

    @pytest.mark.asyncio
    async def test_async_step_is_awaited(self, event):
        called = False

        async def _step(*args, **kwargs):
            nonlocal called
            called = True
            return "ok"

        result = await event._run_step(_step, "arg", k="v")
        assert called is True
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_sync_step_offloaded_to_thread(self, event):
        """Sync step functions are offloaded via volnux.concurrency.async_utils.to_thread."""
        called_from_thread = []

        def _step(*args, **kwargs):
            import threading
            called_from_thread.append(threading.current_thread().name)
            return "sync-ok"

        # Don't patch to_thread — let it actually run in the thread pool.
        result = await event._run_step(_step, "arg")
        assert result == "sync-ok"
        assert len(called_from_thread) == 1
        # The thread name should NOT be the main thread
        assert called_from_thread[0] != threading.main_thread().name

    @pytest.mark.asyncio
    async def test_passes_args_kwargs_to_step(self, event):
        received = {}

        async def _step(*args, **kwargs):
            received["args"] = args
            received["kwargs"] = kwargs
            return None

        await event._run_step(_step, "p1", "p2", key="v")
        assert received["args"] == ("p1", "p2")
        assert received["kwargs"] == {"key": "v"}

    @pytest.mark.asyncio
    async def test_step_exception_propagates(self, event):
        async def _step(*args, **kwargs):
            raise RuntimeError("step boom")

        with pytest.raises(RuntimeError, match="step boom"):
            await event._run_step(_step)


# ---------------------------------------------------------------------------
# steps_runner
# ---------------------------------------------------------------------------

class TestStepsRunner:
    def _patch_steps(self, event, steps):
        """Replace _get_steps to return our list."""
        return patch.object(event, "_get_steps", return_value=steps)

    @pytest.mark.asyncio
    async def test_happy_path_completes_all_steps(self, event, command_channel):
        """All steps succeed → final checkpoint enqueued, COMPLETED status sent."""
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event.exec_status = True
        event.exec_result = {"ok": True}

        calls = []

        async def _step_a(*args, **kwargs):
            calls.append("a")
            return None

        async def _step_b(*args, **kwargs):
            calls.append("b")
            return None

        steps = [_step_a, _step_b]
        with self._patch_steps(event, steps):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()) as mock_enqueue:
                with patch.object(event, "on_success", return_value=MagicMock(name="result")) as mock_os:
                    result = await event.steps_runner()

        assert calls == ["a", "b"]
        # Final checkpoint + per-step checkpoints (2 steps + 1 final = 3)
        assert mock_enqueue.await_count == 3
        mock_os.assert_called_once_with({"ok": True})

        # Verify the final COMPLETED status was sent
        # _send_update was called multiple times; check the last call
        last_call = command_channel.send_message.call_args_list[-1]
        sent = last_call.args[0]
        assert sent.message_type == MessageType.STATUS_UPDATE
        assert sent.payload == {"state": TaskState.COMPLETED.value}

    @pytest.mark.asyncio
    async def test_step_failure_sends_error_and_reraises(self, event, command_channel, caplog):
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel

        async def _good_step(*args, **kwargs):
            return None

        async def _bad_step(*args, **kwargs):
            raise RuntimeError("step failure")

        steps = [_good_step, _bad_step]
        with self._patch_steps(event, steps):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                with pytest.raises(RuntimeError, match="step failure"):
                    await event.steps_runner()

        # An ERROR / FAILED message should have been sent
        sent_msgs = [c.args[0] for c in command_channel.send_message.call_args_list]
        error_msgs = [
            m for m in sent_msgs
            if m.message_type == MessageType.ERROR and m.payload.get("state") == TaskState.FAILED.value
        ]
        assert len(error_msgs) == 1
        assert error_msgs[0].payload["error"] == "step failure"
        assert error_msgs[0].payload["step_name"] == "_bad_step"

    @pytest.mark.asyncio
    async def test_pause_gate_blocks_until_set(self, event, command_channel):
        """If _pause_gate is cleared, steps_runner blocks at that step until
        the gate is set (e.g. via RESUME command)."""
        event._command_channel = command_channel
        event._pause_gate.clear()

        async def _step(*args, **kwargs):
            return None

        # Schedule a RESUME after a short delay
        async def _resume_after_delay():
            await asyncio.sleep(0.05)
            event._pause_gate.set()

        steps = [_step, _step]
        with self._patch_steps(event, steps):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                asyncio.create_task(_resume_after_delay())
                # Should complete (after the gate is set)
                await asyncio.wait_for(event.steps_runner(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_phase_skip_skips_completed_steps(self, event, command_channel):
        """Steps whose _phase <= self._phase are skipped."""
        from volnux.execution.rehydrator.event.snapshot import EventPhase
        event._command_channel = command_channel

        # Set _phase to COMMUNICATING → steps with phase <= COMMUNICATING are skipped
        event._phase = EventPhase.COMMUNICATING

        executed = []

        async def _skipped_step(*args, **kwargs):
            executed.append("skipped")
            return None

        async def _executed_step(*args, **kwargs):
            executed.append("executed")
            return None

        _skipped_step._phase = EventPhase.INITIALIZED  # type: ignore[attr-defined]
        _executed_step._phase = EventPhase.PROCESSING  # type: ignore[attr-defined]

        steps = [_skipped_step, _executed_step]
        with self._patch_steps(event, steps):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                await event.steps_runner()

        assert executed == ["executed"]

    @pytest.mark.asyncio
    async def test_step_without_phase_attribute_is_executed(self, event, command_channel):
        """Steps with no _phase attribute are always executed."""
        event._command_channel = command_channel
        from volnux.execution.rehydrator.event.snapshot import EventPhase
        event._phase = EventPhase.COMPLETED  # everything is "complete"

        executed = []

        async def _step(*args, **kwargs):
            executed.append("step")

        # No _phase attribute set on _step
        steps = [_step]
        with self._patch_steps(event, steps):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                await event.steps_runner()

        assert executed == ["step"]

    @pytest.mark.asyncio
    async def test_preemption_raises_suspend_task(self, event, command_channel):
        """When _preempted is True and a CancelledError is raised mid-step,
        steps_runner converts it to SuspendTask."""
        from volnux.exceptions import SuspendTask
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._preempted = True

        async def _step(*args, **kwargs):
            raise asyncio.CancelledError()

        with self._patch_steps(event, [_step]):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                with pytest.raises(SuspendTask) as exc_info:
                    await event.steps_runner()

        # The _preempted flag should be reset
        assert event._preempted is False
        # A QUEUED status should have been sent before the SuspendTask raise
        sent_msgs = [c.args[0] for c in command_channel.send_message.call_args_list]
        queued_msgs = [
            m for m in sent_msgs
            if m.payload.get("state") == TaskState.QUEUED.value
        ]
        assert len(queued_msgs) == 1

    @pytest.mark.asyncio
    async def test_hard_cancel_raises_cancelled_error(self, event, command_channel):
        """When _preempted is False and CancelledError is raised, steps_runner
        sends CANCELLED status and re-raises."""
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel
        event._preempted = False

        async def _step(*args, **kwargs):
            raise asyncio.CancelledError()

        with self._patch_steps(event, [_step]):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                with pytest.raises(asyncio.CancelledError):
                    await event.steps_runner()

        sent_msgs = [c.args[0] for c in command_channel.send_message.call_args_list]
        cancelled_msgs = [
            m for m in sent_msgs
            if m.payload.get("state") == TaskState.CANCELLED.value
        ]
        assert len(cancelled_msgs) == 1

    @pytest.mark.asyncio
    async def test_outer_exception_no_double_report(self, event, command_channel, caplog):
        """If a step's exception is already reported by the per-step handler,
        the outer except-Exception branch must NOT report again."""
        import logging
        _, _, MessageType, TaskState, _ = _import_mixin_and_types()
        event._command_channel = command_channel

        async def _bad_step(*args, **kwargs):
            raise RuntimeError("step failure")

        with self._patch_steps(event, [_bad_step]):
            with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(RuntimeError, match="step failure"):
                        await event.steps_runner()

        # Count FAILED status messages — should be exactly 1 (from per-step handler)
        sent_msgs = [c.args[0] for c in command_channel.send_message.call_args_list]
        failed_msgs = [
            m for m in sent_msgs
            if m.payload.get("state") == TaskState.FAILED.value
        ]
        assert len(failed_msgs) == 1
