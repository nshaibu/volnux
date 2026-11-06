"""RehydrationManager: wakes suspended workflows through the models, not raw connections."""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from volnux.engine.base import EngineExecutionResult
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.triggers import RehydrationManager
from volnux.engine.workflows.trigger.triggers.base import TriggerType
from volnux.engine.workflows.trigger.triggers.manager import WakeOutcome
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
    RequestStatus,
    ResumeClaim,
)
from volnux.signal import signals

from .helpers import answer, suspend


class Recorder:
    """A resume handler that records what it was asked to resume."""

    def __init__(self, delay=0.0, error=None):
        self.calls = []
        self.delay = delay
        self.error = error
        self.active = 0
        self.max_active = 0

    async def __call__(self, entry, response):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            self.calls.append((entry.request_id, response.decision, entry.status))
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.error:
                raise self.error
            return MagicMock(status=EngineExecutionResult.COMPLETED)
        finally:
            self.active -= 1


def manager_with(handler, **kw):
    kw.setdefault("poll_timeout", 0.02)
    kw.setdefault("timeout_sweep_interval", 0.05)
    return RehydrationManager(resume_handler=handler, **kw)


async def waiting_entry(request_id="r1", timeout_at=None, **kw):
    entry = ExternalCommunicationQueueEntry.create(
        request_id=request_id,
        workflow_name="Orders",
        workflow_id="wf-1",
        task_id="t1",
        request_type="hitl",
        checkpoint_key="ck",
        context_id="ctx",
        request_title="Approve",
        timeout_at=timeout_at,
        **kw,
    )
    await entry.save()
    return entry


def past():
    return (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()


async def until(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


async def status_reached(entry, status, timeout=3.0):
    """The last workflow step running is not the end: the entry is marked and the
    claim released afterwards. Wait for that instead of racing it."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (await reload(entry)).status == status.value:
            return
        await asyncio.sleep(0.01)


async def reload(entry):
    return await ExternalCommunicationQueueEntry.get(entry.request_id)


class TestIsATrigger:
    def test_it_is_a_manager_trigger_for_the_whole_node(self):
        manager = RehydrationManager()
        assert manager.trigger_type is TriggerType.MANAGER
        assert manager.workflow_name == RehydrationManager.WORKFLOW_SCOPE

    def test_it_takes_no_infrastructure_connections(self):
        import inspect

        parameters = inspect.signature(RehydrationManager.__init__).parameters
        assert not {"redis_client", "event_bus", "checkpoint_store"} & set(parameters)
        assert "coordinator_registry" not in parameters

    def test_it_opens_no_connection_of_its_own(self):
        from volnux.engine.workflows.trigger.triggers.manager import rehydrate

        source = open(rehydrate.__file__).read()
        for forbidden in ("redis", "scan_iter", "subscribe(", ".publish("):
            assert forbidden not in source

    def test_the_trigger_engine_accepts_it_without_a_workflow(self):
        engine = TriggerEngine(workflow_executor=MagicMock())
        manager = RehydrationManager()
        engine.register_manager(manager)
        assert engine.triggers[manager.trigger_id] is manager

    def test_the_trigger_engine_rejects_other_triggers_and_duplicates(self):
        from volnux.engine.workflows.trigger.triggers import ManualTrigger

        engine = TriggerEngine(workflow_executor=MagicMock())
        with pytest.raises(ValueError, match="MANAGER"):
            engine.register_manager(ManualTrigger("flow"))
        manager = RehydrationManager()
        engine.register_manager(manager)
        with pytest.raises(ValueError, match="already registered"):
            engine.register_manager(manager)


class TestWake:
    @pytest.mark.asyncio
    async def test_resumes_the_workflow_and_records_it(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)

        outcome = await manager.wake(answer(entry, "approve"))

        assert outcome == WakeOutcome.RESUMED
        assert handler.calls == [("r1", "approve", RequestStatus.READY.value)]
        stored = await reload(entry)
        assert stored.status == RequestStatus.RESUMED.value
        assert stored.attempts == 1 and stored.last_error is None
        assert stored.response["decision"] == "approve"

    @pytest.mark.asyncio
    async def test_the_claim_is_released_afterwards(self, serialized_store):
        entry = await waiting_entry()
        await manager_with(Recorder()).wake(answer(entry))
        assert await ResumeClaim.get_or_none("r1") is None

    @pytest.mark.asyncio
    async def test_announces_the_rehydrated_task(self, serialized_store):
        seen = []

        def listener(sender, signal, **kwargs):
            seen.append(kwargs)

        signals.task_rehydrated.connect(None, listener)
        try:
            entry = await waiting_entry()
            await manager_with(Recorder()).wake(answer(entry, "reject"))
        finally:
            signals.task_rehydrated.disconnect(None, listener)
        assert seen == [
            {
                "workflow_name": "Orders",
                "task_id": "t1",
                "request_id": "r1",
                "decision": "reject",
                "reviewer": "ann",
            }
        ]

    @pytest.mark.asyncio
    async def test_a_failing_listener_does_not_undo_the_resume(self, serialized_store):
        def listener(sender, signal, **kwargs):
            raise RuntimeError("listener broke")

        signals.task_rehydrated.connect(None, listener)
        try:
            entry = await waiting_entry()
            outcome = await manager_with(Recorder()).wake(answer(entry))
        finally:
            signals.task_rehydrated.disconnect(None, listener)
        assert outcome == WakeOutcome.RESUMED

    @pytest.mark.asyncio
    async def test_an_answer_for_an_unknown_request_is_dropped(self, serialized_store):
        handler = Recorder()
        outcome = await manager_with(handler).wake(
            ExternalCommunicationResponse(request_id="nope", decision="x", data={})
        )
        assert outcome == WakeOutcome.IGNORED and handler.calls == []

    @pytest.mark.asyncio
    async def test_a_duplicate_answer_is_harmless(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)
        assert await manager.wake(answer(entry)) == WakeOutcome.RESUMED
        assert await manager.wake(answer(entry)) == WakeOutcome.IGNORED
        assert len(handler.calls) == 1

    @pytest.mark.asyncio
    async def test_a_timeout_marks_the_entry_expired(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        await manager_with(handler).wake(
            ExternalCommunicationResponse.timeout(entry.request_id)
        )
        assert handler.calls[0][1] == "timeout"
        assert (await reload(entry)).status == RequestStatus.EXPIRED.value

    @pytest.mark.asyncio
    async def test_a_request_claimed_elsewhere_is_left_alone(self, serialized_store):
        entry = await waiting_entry()
        await ResumeClaim.acquire("r1", owner="other-worker", lease_seconds=60)
        handler = Recorder()
        outcome = await manager_with(handler).wake(answer(entry))
        assert outcome == WakeOutcome.CLAIMED_ELSEWHERE and handler.calls == []
        assert (await reload(entry)).status == RequestStatus.WAITING.value
        assert (await ResumeClaim.get("r1")).owner == "other-worker"  # not stolen

    @pytest.mark.asyncio
    async def test_a_human_reply_racing_a_timeout_resumes_once(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder(delay=0.05)
        manager = manager_with(handler)
        outcomes = await asyncio.gather(
            manager.wake(answer(entry, "approve")),
            manager.wake(ExternalCommunicationResponse.timeout(entry.request_id)),
        )
        assert sorted(outcomes) == sorted(
            [WakeOutcome.RESUMED, WakeOutcome.CLAIMED_ELSEWHERE]
        ) or sorted(outcomes) == sorted([WakeOutcome.RESUMED, WakeOutcome.IGNORED])
        assert len(handler.calls) == 1

    @pytest.mark.asyncio
    async def test_two_managers_never_resume_the_same_request_twice(
        self, serialized_store
    ):
        entry = await waiting_entry()
        handler = Recorder(delay=0.05)
        first, second = manager_with(handler), manager_with(handler)
        await asyncio.gather(first.wake(answer(entry)), second.wake(answer(entry)))
        assert len(handler.calls) == 1


class TestFailure:
    @pytest.mark.asyncio
    async def test_a_failed_resume_keeps_the_entry_for_a_retry(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder(error=RuntimeError("store down"))
        outcome = await manager_with(handler).wake(answer(entry))

        assert outcome == WakeOutcome.FAILED
        stored = await reload(entry)
        assert stored.status == RequestStatus.FAILED.value
        assert "store down" in stored.last_error
        assert stored.response["decision"] == "approve"  # kept for the retry
        assert await ResumeClaim.get_or_none("r1") is None  # released

    @pytest.mark.asyncio
    async def test_a_failed_entry_can_be_woken_again(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder(error=RuntimeError("down"))
        manager = manager_with(handler)
        await manager.wake(answer(entry))

        handler.error = None
        assert await manager.wake(answer(entry)) == WakeOutcome.RESUMED
        assert (await reload(entry)).attempts == 2

    @pytest.mark.asyncio
    async def test_retry_failed_requeues_the_kept_answer(self, serialized_store):
        entry = await waiting_entry()
        manager = manager_with(Recorder(error=RuntimeError("down")))
        await manager.wake(answer(entry, "reject"))

        assert await manager.retry_failed() == 1
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert queued.request_id == "r1" and queued.decision == "reject"

    @pytest.mark.asyncio
    async def test_cancellation_mid_resume_leaves_the_entry_recoverable(
        self, serialized_store
    ):
        entry = await waiting_entry()
        manager = manager_with(Recorder(delay=5))
        task = asyncio.ensure_future(manager.wake(answer(entry, "approve")))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        stored = await reload(entry)
        assert stored.status == RequestStatus.READY.value  # not lost, not "resumed"
        assert stored.response["decision"] == "approve"
        assert await ResumeClaim.get_or_none("r1") is None  # the claim was released


class TestRecovery:
    @pytest.mark.asyncio
    async def test_an_interrupted_resume_is_redelivered(self, serialized_store):
        entry = await waiting_entry()
        entry.status = RequestStatus.READY.value
        entry.response = answer(entry, "approve").dump("dict")
        await entry.save()

        assert await manager_with(Recorder()).recover() == 1

        assert (await reload(entry)).status == RequestStatus.WAITING.value
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert queued.request_id == "r1" and queued.decision == "approve"

    @pytest.mark.asyncio
    async def test_a_resume_in_progress_elsewhere_is_not_redelivered(
        self, serialized_store
    ):
        entry = await waiting_entry()
        entry.status = RequestStatus.READY.value
        entry.response = answer(entry).dump("dict")
        await entry.save()
        await ResumeClaim.acquire("r1", owner="live-worker", lease_seconds=60)

        assert await manager_with(Recorder()).recover() == 0
        assert await ExternalCommunicationResponse.dequeue(timeout=0.05) is None

    @pytest.mark.asyncio
    async def test_a_dead_workers_expired_claim_does_not_block_recovery(
        self, serialized_store
    ):
        entry = await waiting_entry()
        entry.status = RequestStatus.READY.value
        entry.response = answer(entry).dump("dict")
        await entry.save()
        await ResumeClaim.acquire("r1", "dead", 5, now=time.time() - 100)

        assert await manager_with(Recorder()).recover() == 1

    @pytest.mark.asyncio
    async def test_waiting_and_finished_entries_are_left_alone(self, serialized_store):
        await waiting_entry("w")
        done = await waiting_entry("d")
        done.status = RequestStatus.RESUMED.value
        await done.save()
        assert await manager_with(Recorder()).recover() == 0

    @pytest.mark.asyncio
    async def test_start_recovers_before_consuming(self, serialized_store):
        entry = await waiting_entry()
        entry.status = RequestStatus.READY.value
        entry.response = answer(entry, "approve").dump("dict")
        await entry.save()

        handler = Recorder()
        manager = manager_with(handler)
        await manager.start()
        try:
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()
        assert (await reload(entry)).status == RequestStatus.RESUMED.value


class TestTimeouts:
    @pytest.mark.asyncio
    async def test_sweep_queues_a_timeout_for_expired_requests_only(
        self, serialized_store
    ):
        await waiting_entry("late", timeout_at=past())
        await waiting_entry(
            "soon",
            timeout_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )
        await waiting_entry("never", timeout_at=None)

        assert await manager_with(Recorder()).sweep_timeouts() == 1
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert (queued.request_id, queued.source, queued.decision) == (
            "late",
            "timeout",
            "timeout",
        )
        assert await ExternalCommunicationResponse.dequeue(timeout=0.05) is None

    @pytest.mark.asyncio
    async def test_sweeping_twice_does_not_queue_two_timeouts(self, serialized_store):
        await waiting_entry("late", timeout_at=past())
        manager = manager_with(Recorder())
        assert await manager.sweep_timeouts() == 1
        assert await manager.sweep_timeouts() == 0

    @pytest.mark.asyncio
    async def test_answered_requests_do_not_time_out(self, serialized_store):
        entry = await waiting_entry("late", timeout_at=past())
        entry.status = RequestStatus.RESUMED.value
        await entry.save()
        assert await manager_with(Recorder()).sweep_timeouts() == 0

    @pytest.mark.asyncio
    async def test_the_running_manager_resumes_an_expired_request(
        self, serialized_store
    ):
        await waiting_entry("late", timeout_at=past())
        handler = Recorder()
        manager = manager_with(handler)
        await manager.start()
        try:
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()
        assert handler.calls[0][:2] == ("late", "timeout")


class TestConsuming:
    @pytest.mark.asyncio
    async def test_resumes_what_arrives_on_the_queue(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)
        await manager.start()
        try:
            assert manager.running
            await ExternalCommunicationResponse.enqueue(answer(entry, "approve"))
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()
        assert not manager.running
        assert (await reload(entry)).status == RequestStatus.RESUMED.value

    @pytest.mark.asyncio
    async def test_a_paused_manager_leaves_answers_on_the_queue(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)
        manager.state.enabled = False
        await manager.start()
        try:
            await ExternalCommunicationResponse.enqueue(answer(entry))
            await asyncio.sleep(0.15)
            assert handler.calls == []
            assert await ExternalCommunicationResponse.queue_length() == 1

            manager.state.enabled = True
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_a_failing_queue_does_not_stop_the_manager(
        self, serialized_store, monkeypatch
    ):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)
        real_dequeue = ExternalCommunicationResponse.dequeue.__func__
        attempts = []

        async def flaky(cls, timeout=None, **kw):
            attempts.append(1)
            if len(attempts) == 1:
                raise ConnectionError("backend down")
            return await real_dequeue(cls, timeout=timeout, **kw)

        async def fast_sleep(delay, *a, **k):
            await real_sleep(0.001)

        real_sleep = asyncio.sleep
        monkeypatch.setattr(
            ExternalCommunicationResponse, "dequeue", classmethod(flaky)
        )
        monkeypatch.setattr(
            "volnux.engine.workflows.trigger.triggers.manager.rehydrate.asyncio.sleep",
            fast_sleep,
        )
        await ExternalCommunicationResponse.enqueue(answer(entry))
        await manager.start()
        try:
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()
        assert len(attempts) >= 2

    @pytest.mark.asyncio
    async def test_resumes_run_concurrently_but_only_up_to_the_limit(
        self, serialized_store
    ):
        entries = [await waiting_entry("r%d" % i) for i in range(4)]
        handler = Recorder(delay=0.05)
        manager = manager_with(handler, max_concurrent_resumes=2)
        for entry in entries:
            await ExternalCommunicationResponse.enqueue(answer(entry))
        await manager.start()
        try:
            assert await until(lambda: len(handler.calls) == 4)
            await until(lambda: handler.active == 0)
        finally:
            await manager.stop()
        assert handler.max_active == 2

    @pytest.mark.asyncio
    async def test_stop_cancels_resumes_in_flight_and_is_idempotent(
        self, serialized_store
    ):
        entry = await waiting_entry()
        handler = Recorder(delay=5)
        manager = manager_with(handler)
        await ExternalCommunicationResponse.enqueue(answer(entry))
        await manager.start()
        assert await until(lambda: handler.calls)
        await manager.stop()
        await manager.stop()
        assert not manager._wake_tasks
        assert (await reload(entry)).status == RequestStatus.READY.value

    @pytest.mark.asyncio
    async def test_start_twice_runs_one_consumer(self, serialized_store):
        manager = manager_with(Recorder())
        await manager.start()
        first = manager._consumer_task
        await manager.start()
        try:
            assert manager._consumer_task is first
        finally:
            await manager.stop()


class TestEndToEnd:
    """Suspend a real workflow, answer it over the queue, and let the manager finish it."""

    @pytest.mark.asyncio
    async def test_a_human_answer_completes_the_workflow(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        assert seen["ran"] == ["A"]

        manager = RehydrationManager(poll_timeout=0.02)
        await manager.start()
        try:
            await ExternalCommunicationResponse.enqueue(answer(entry, "approve"))
            assert await until(lambda: "C" in seen["ran"])
            await status_reached(entry, RequestStatus.RESUMED)
        finally:
            await manager.stop()

        assert seen["ran"] == ["A", "B", "C"]
        assert seen["decision"] == "approve"
        stored = await reload(entry)
        assert stored.status == RequestStatus.RESUMED.value
        assert await ResumeClaim.get_or_none(entry.request_id) is None

    @pytest.mark.asyncio
    async def test_an_expired_request_resumes_with_a_timeout(
        self, serialized_store, workflow_factory, seen
    ):
        _, entry = await suspend(workflow_factory(timeout_hours=-0.001))
        assert entry.is_expired()

        manager = RehydrationManager(poll_timeout=0.02, timeout_sweep_interval=0.05)
        await manager.start()
        try:
            assert await until(lambda: "C" in seen["ran"])
            await status_reached(entry, RequestStatus.EXPIRED)
        finally:
            await manager.stop()

        assert seen["decision"] == "timeout"
        assert (await reload(entry)).status == RequestStatus.EXPIRED.value

    @pytest.mark.asyncio
    async def test_a_second_answer_does_not_run_the_workflow_again(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        manager = RehydrationManager(poll_timeout=0.02)
        await manager.start()
        try:
            await ExternalCommunicationResponse.enqueue(answer(entry, "approve"))
            await ExternalCommunicationResponse.enqueue(answer(entry, "reject"))
            assert await until(lambda: "C" in seen["ran"])
            await asyncio.sleep(0.2)
        finally:
            await manager.stop()
        assert seen["ran"] == ["A", "B", "C"]
        assert seen["decision"] == "approve"
