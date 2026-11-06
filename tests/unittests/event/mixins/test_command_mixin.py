import asyncio
from unittest.mock import AsyncMock

import pytest

from volnux.exceptions import (
    ExternalCommunicationSuspensionRequest,
    StopProcessingError,
    SuspendTask,
    SwitchTask,
)
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.flows.bridge.communications.tasks import (
    CommandType,
    MessageType,
    TaskCommand,
    TaskState,
)

from ..conftest import FakeChannel


def command(kind, **payload):
    return TaskCommand(task_id="task-1", command_type=kind, payload=payload or None)


def kinds(channel):
    return [m.message_type for m in channel.sent]


class TestSendUpdate:
    @pytest.mark.asyncio
    async def test_noop_without_a_channel(self, build):
        await build()._send_update(MessageType.STATUS_UPDATE, TaskState.RUNNING)

    @pytest.mark.asyncio
    async def test_message_carries_task_state_and_extras(self, build, channel):
        event = build(command_channel=channel)
        await event._send_update(
            MessageType.PROGRESS_UPDATE, TaskState.RUNNING, {"step": 2}
        )
        [message] = channel.sent
        assert message.task_id == "task-1"
        assert message.message_type == MessageType.PROGRESS_UPDATE
        assert message.payload == {"state": "running", "step": 2}

    @pytest.mark.asyncio
    async def test_transport_errors_are_swallowed(self, build, channel):
        channel.send_message.side_effect = ConnectionError("down")
        await build(command_channel=channel)._send_update(
            MessageType.STATUS_UPDATE, TaskState.RUNNING
        )


class TestHandleCommand:
    @pytest.mark.asyncio
    async def test_pause_and_resume_toggle_the_gate(self, build, channel):
        event = build(command_channel=channel)
        await event._handle_command(command(CommandType.PAUSE))
        assert not event._pause_gate.is_set()
        await event._handle_command(command(CommandType.RESUME))
        assert event._pause_gate.is_set()
        assert channel.states == ["paused", "running"]

    @pytest.mark.asyncio
    async def test_cancel_cancels_the_worker_task(self, build):
        event = build()
        worker = asyncio.ensure_future(asyncio.sleep(10))
        event._main_worker_task = worker
        await event._handle_command(command(CommandType.CANCEL))
        with pytest.raises(asyncio.CancelledError):
            await worker

    @pytest.mark.asyncio
    async def test_cancel_before_the_worker_exists_is_harmless(self, build, caplog):
        event = build()
        assert event._main_worker_task is None
        await event._handle_command(command(CommandType.CANCEL))
        assert "no running worker" in caplog.text

    @pytest.mark.asyncio
    async def test_update_priority_marks_preempted_and_cancels(self, build, channel):
        event = build(command_channel=channel)
        worker = asyncio.ensure_future(asyncio.sleep(10))
        event._main_worker_task = worker
        await event._handle_command(command(CommandType.UPDATE_PRIORITY, priority=9))
        assert event._preempted is True
        [message] = channel.sent
        assert message.payload == {"state": "running", "priority": 9, "preempted": True}
        with pytest.raises(asyncio.CancelledError):
            await worker

    @pytest.mark.asyncio
    async def test_update_priority_without_a_worker_still_marks_preempted(self, build):
        event = build()
        await event._handle_command(command(CommandType.UPDATE_PRIORITY))
        assert event._preempted is True

    @pytest.mark.asyncio
    async def test_checkpoint_command_enqueues_and_reports(
        self, build, channel, checkpoint_manager
    ):
        event = build(command_channel=channel, checkpoint_manager=checkpoint_manager)
        await event._handle_command(command(CommandType.CHECKPOINT))
        checkpoint_manager.enqueue.assert_called_once()
        assert channel.sent[0].payload["checkpoint_saved"] is True

    @pytest.mark.asyncio
    async def test_query_status_reports_running_or_paused(self, build, channel):
        event = build(command_channel=channel)
        await event._handle_command(command(CommandType.QUERY_STATUS))
        event._pause_gate.clear()
        await event._handle_command(command(CommandType.QUERY_STATUS))
        assert channel.states == ["running", "paused"]

    @pytest.mark.asyncio
    async def test_unhandled_command_type_is_logged(self, build, channel, caplog):
        event = build(command_channel=channel)
        await event._handle_command(command(CommandType.REQUEST_PROGRESS))
        assert "unknown command type" in caplog.text and channel.sent == []

    @pytest.mark.asyncio
    async def test_handler_errors_are_contained(self, build, channel):
        channel.send_message.side_effect = RuntimeError("x")
        await build(command_channel=channel)._handle_command(command(CommandType.PAUSE))


class TestCommandListener:
    @pytest.mark.asyncio
    async def test_exits_immediately_without_a_channel(self, build):
        await build()._command_listener()

    @pytest.mark.asyncio
    async def test_processes_commands_until_cancelled(self, build, channel):
        event = build(command_channel=channel)
        channel.commands.extend(
            [command(CommandType.PAUSE), command(CommandType.RESUME)]
        )
        listener = asyncio.ensure_future(event._command_listener())
        for _ in range(200):
            if channel.states == ["paused", "running"]:
                break
            await asyncio.sleep(0.005)
        listener.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener
        assert channel.states == ["paused", "running"]

    @pytest.mark.asyncio
    async def test_idle_timeouts_do_not_stop_the_loop(self, build, channel):
        event = build(command_channel=channel)
        listener = asyncio.ensure_future(event._command_listener())
        await asyncio.sleep(0.03)
        assert not listener.done()
        assert channel.receive_command.await_count > 1
        listener.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener

    @pytest.mark.asyncio
    async def test_transport_error_backs_off_and_keeps_listening(
        self, build, channel, monkeypatch
    ):
        sleeps = []
        real_sleep = asyncio.sleep

        async def fake_sleep(delay, *a, **k):
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr("volnux.mixins.event.command.asyncio.sleep", fake_sleep)
        calls = []

        async def flaky(timeout=None):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("transport down")
            await real_sleep(0.001)
            raise asyncio.TimeoutError()

        channel.receive_command = AsyncMock(side_effect=flaky)
        event = build(command_channel=channel)
        listener = asyncio.ensure_future(event._command_listener())
        for _ in range(100):
            if len(calls) > 2:
                break
            await real_sleep(0.005)
        listener.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener
        assert sleeps and sleeps[0] == 1.0 and len(calls) > 2


class TestHelpers:
    @pytest.mark.parametrize(
        "exc, state",
        [
            (
                SwitchTask(current_task_id="t", next_task_descriptor=1, result=None),
                TaskState.COMPLETED,
            ),
            (StopProcessingError(message="s", exception=None), TaskState.CANCELLED),
            (ValueError("x"), None),
        ],
    )
    def test_control_flow_state(self, build, exc, state):
        assert build()._control_flow_state(exc) == state

    def test_suspensions_map_to_suspended(self, build):
        event = build()
        assert (
            event._control_flow_state(
                SuspendTask(event, suspension_type=SuspendTask.SuspensionType.HITL)
            )
            == TaskState.SUSPENDED
        )

    def test_annotate_error_adds_identity(self, build):
        event = build(sequence_number=4)
        exc = ValueError("x")
        event._annotate_error(exc)
        assert exc.params["task_id"] == "task-1" and exc.params["sequence_number"] == 4
        assert exc.params["event_name"] == type(event).__name__

    def test_annotate_error_keeps_existing_params(self, build):
        exc = ValueError("x")
        exc.params = {"mine": 1}
        build()._annotate_error(exc)
        assert exc.params == {"mine": 1}

    def test_annotate_error_tolerates_read_only_exceptions(self, build):
        class Frozen(Exception):
            __slots__ = ()

        build()._annotate_error(Frozen())  # must not raise

    @pytest.mark.asyncio
    async def test_run_step_handles_sync_and_async_steps(self, build):
        event = build()

        async def a(x):
            return ("async", x)

        def b(x):
            return ("sync", x)

        assert await event._run_step(a, 1) == ("async", 1)
        assert await event._run_step(b, 2) == ("sync", 2)


class TestStepsRunner:
    @pytest.mark.asyncio
    async def test_runs_every_step_in_order_and_reports(
        self, build, channel, checkpoint_manager
    ):
        event = build(command_channel=channel, checkpoint_manager=checkpoint_manager)
        result = await event.steps_runner()
        assert result.success and result.content == "ok"
        assert event.get_phase() == EventPhase.COMPLETED
        names = [
            m.payload.get("step_name")
            for m in channel.sent
            if m.payload.get("step_name")
        ]
        assert names == [
            "_pre_process",
            "_communicate",
            "_process",
            "_post_process",
            "_completed",
        ]
        assert channel.states[0] == "running" and channel.states[-1] == "completed"
        assert kinds(channel).count(MessageType.PROGRESS_UPDATE) == 5
        assert checkpoint_manager.enqueue.call_count == 6  # per step + final

    @pytest.mark.asyncio
    async def test_phases_are_visited_once_in_order(self, build, recorded):
        event = build()
        await event.steps_runner()
        phases = [
            e["phase"] for e in recorded["event_phase_changed"] if e["event"] is event
        ]
        assert phases == sorted(set(phases)) and phases[-1] == EventPhase.COMPLETED
        assert len(phases) == 6

    @pytest.mark.asyncio
    async def test_resume_skips_completed_phases(self, build):
        calls = []

        async def bypass(self):
            calls.append("bypass")
            raise NotImplementedError

        async def communicate(self, *a, **k):
            calls.append("communicate")

        async def process(self):
            calls.append("process")
            return True, "resumed"

        event = build(
            {"bypass": bypass, "communicate": communicate, "process": process}
        )
        event._phase = EventPhase.COMMUNICATING  # as if restored from a checkpoint
        result = await event.steps_runner()
        assert calls == ["process"]
        assert result.content == "resumed"

    @pytest.mark.asyncio
    async def test_resume_after_post_process_rebuilds_the_result(self, build):
        event = build()
        event._phase = EventPhase.POST_PROCESS
        event.exec_status, event.exec_result = True, {"saved": 1}
        result = await event.steps_runner()
        assert result.success and result.content == {"saved": 1}

    @pytest.mark.asyncio
    async def test_bypass_skips_communication_and_processing(self, build, channel):
        ran = []

        async def bypass(self):
            return True, {"cached": True}

        async def communicate(self, *a, **k):
            ran.append("communicate")

        async def process(self):
            ran.append("process")
            return True, "x"

        event = build(
            {"bypass": bypass, "communicate": communicate, "process": process},
            command_channel=channel,
        )
        result = await event.steps_runner()
        assert ran == [] and event._bypassed is True
        assert result.content == {"cached": True}
        assert event.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_failed_process_returns_a_failed_result_not_an_exception(self, build):
        async def process(self):
            raise ValueError("bad")

        result = await build({"process": process}).steps_runner()
        assert not result.success and isinstance(result.content, ValueError)

    @pytest.mark.asyncio
    async def test_step_exception_is_reported_once_and_annotated(self, build, channel):
        async def bypass(self):
            raise KeyError("boom")

        event = build({"bypass": bypass}, command_channel=channel)
        with pytest.raises(KeyError) as info:
            await event.steps_runner()
        errors = [m for m in channel.sent if m.message_type == MessageType.ERROR]
        assert len(errors) == 1
        assert errors[0].payload["state"] == "failed"
        assert errors[0].payload["step_name"] == "_pre_process"
        assert info.value.params["task_id"] == "task-1"

    @pytest.mark.asyncio
    async def test_goto_reports_completed_and_is_not_a_failure(self, build, channel):
        async def process(self):
            self.goto(1, True, "to-branch")

        event = build({"process": process}, command_channel=channel)
        with pytest.raises(SwitchTask):
            await event.steps_runner()
        assert not [m for m in channel.sent if m.message_type == MessageType.ERROR]
        last = channel.sent[-1]
        assert last.payload["state"] == "completed"
        assert last.payload["reason"] == "SwitchTask"

    @pytest.mark.asyncio
    async def test_stop_processing_reports_cancelled(self, build, channel):
        from volnux.parser.options import StopCondition

        event = build(
            {"process": _succeeds},
            command_channel=channel,
            stop_condition=StopCondition.ON_SUCCESS,
        )
        with pytest.raises(StopProcessingError):
            await event.steps_runner()
        assert channel.sent[-1].payload["state"] == "cancelled"

    @pytest.mark.asyncio
    async def test_external_communication_reports_suspended(self, build, channel):
        async def communicate(self, *a, **k):
            await self.request_human_input("t", "d", {"a": 1})

        event = build({"communicate": communicate}, command_channel=channel)
        with pytest.raises(ExternalCommunicationSuspensionRequest):
            await event.steps_runner()
        assert channel.sent[-1].payload["state"] == "suspended"
        assert event.get_phase() == EventPhase.PRE_PROCESS  # communication not complete

    @pytest.mark.asyncio
    async def test_pause_gate_blocks_until_resumed(self, build):
        event = build()
        event._pause_gate.clear()
        runner = asyncio.ensure_future(event.steps_runner())
        await asyncio.sleep(0.05)
        assert not runner.done() and event.get_phase() == EventPhase.INITIALIZED
        event._pause_gate.set()
        result = await asyncio.wait_for(runner, 2)
        assert result.success

    @pytest.mark.asyncio
    async def test_preemption_checkpoints_queues_and_suspends(
        self, build, channel, checkpoint_manager
    ):
        started = asyncio.Event()

        async def process(self):
            started.set()
            await asyncio.Event().wait()

        event = build(
            {"process": process},
            command_channel=channel,
            checkpoint_manager=checkpoint_manager,
        )
        runner = asyncio.ensure_future(event())
        await started.wait()
        await event._handle_command(command(CommandType.UPDATE_PRIORITY, priority=1))
        with pytest.raises(SuspendTask) as info:
            await runner
        assert info.value.suspension_type == SuspendTask.SuspensionType.PREEMPTION
        assert "queued" in channel.states
        assert event._preempted is False  # flag consumed
        assert checkpoint_manager.enqueue.called

    @pytest.mark.asyncio
    async def test_hard_cancel_reports_cancelled_and_propagates(self, build, channel):
        started = asyncio.Event()

        async def process(self):
            started.set()
            await asyncio.Event().wait()

        event = build({"process": process}, command_channel=channel)
        runner = asyncio.ensure_future(event())
        await started.wait()
        await event._handle_command(command(CommandType.CANCEL))
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert channel.sent[-1].payload["state"] == "cancelled"


async def _succeeds(self):
    return True, "x"
