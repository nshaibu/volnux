"""RehydrationManager: configuration, interrupted-resume recovery limits, timeout
bookkeeping and its lifecycle as a MANAGER trigger."""

import asyncio

import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import (
    RehydrationManager,
    TriggerLifecycle,
)
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
    RequestStatus,
)

from .helpers import answer
from .test_rehydration_manager import (
    Recorder,
    manager_with,
    past,
    reload,
    until,
    waiting_entry,
)


class TestConfiguration:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_concurrent_resumes": 0},
            {"max_concurrent_resumes": -1},
            {"max_attempts": 0},
            {"poll_timeout": 0},
            {"poll_timeout": -1},
            {"timeout_sweep_interval": 0},
            {"claim_lease_seconds": 0},
        ],
    )
    def test_nonsense_values_are_refused(self, kwargs):
        with pytest.raises(ValueError):
            RehydrationManager(**kwargs)

    def test_defaults_are_valid(self):
        manager = RehydrationManager()
        assert manager.workflow_name == RehydrationManager.WORKFLOW_SCOPE
        assert manager.running is False


async def interrupted(request_id="r1", attempts=1):
    entry = await waiting_entry(request_id)
    entry.status = RequestStatus.READY.value
    entry.response = answer(entry, "approve").dump("dict")
    entry.attempts = attempts
    await entry.save()
    return entry


class TestRecoveryLimits:
    @pytest.mark.asyncio
    async def test_a_poison_answer_is_given_up_on(self, serialized_store):
        entry = await interrupted(attempts=5)

        assert await manager_with(Recorder(), max_attempts=5).recover() == 0

        stored = await reload(entry)
        assert stored.status == RequestStatus.FAILED.value
        assert "gave up after 5" in stored.last_error
        assert await ExternalCommunicationResponse.dequeue(timeout=0.05) is None

    @pytest.mark.asyncio
    async def test_below_the_limit_it_is_still_redelivered(self, serialized_store):
        await interrupted(attempts=4)
        assert await manager_with(Recorder(), max_attempts=5).recover() == 1

    @pytest.mark.asyncio
    async def test_the_limit_is_configurable(self, serialized_store):
        await interrupted(attempts=1)
        assert await manager_with(Recorder(), max_attempts=1).recover() == 0

    @pytest.mark.asyncio
    async def test_a_given_up_entry_can_still_be_retried_by_hand(
        self, serialized_store
    ):
        entry = await interrupted(attempts=9)
        manager = manager_with(Recorder(), max_attempts=5)
        await manager.recover()
        assert await manager.retry_failed() == 1
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert queued.request_id == entry.request_id

    @pytest.mark.asyncio
    async def test_one_poison_entry_does_not_block_the_others(self, serialized_store):
        await interrupted("poison", attempts=9)
        await interrupted("fine", attempts=1)
        assert await manager_with(Recorder(), max_attempts=5).recover() == 1
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert queued.request_id == "fine"

    @pytest.mark.asyncio
    async def test_a_lost_enqueue_keeps_the_entry_recoverable(
        self, serialized_store, monkeypatch
    ):
        entry = await interrupted()
        manager = manager_with(Recorder())

        real = ExternalCommunicationResponse.enqueue.__func__
        calls = []

        async def flaky(cls, *args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("queue down")
            return await real(cls, *args, **kwargs)

        monkeypatch.setattr(
            ExternalCommunicationResponse, "enqueue", classmethod(flaky)
        )

        with pytest.raises(ConnectionError):
            await manager.recover()
        # not left WAITING with the answer nowhere: the next recovery retries it
        assert (await reload(entry)).status == RequestStatus.READY.value

        assert await manager.recover() == 1
        assert (await reload(entry)).status == RequestStatus.WAITING.value
        queued = await ExternalCommunicationResponse.dequeue(timeout=0.2)
        assert queued.request_id == entry.request_id

    @pytest.mark.asyncio
    async def test_a_resume_that_keeps_failing_is_counted(self, serialized_store):
        entry = await waiting_entry()
        manager = manager_with(Recorder(error=RuntimeError("boom")))
        for _ in range(3):
            await manager.wake(answer(entry))
        assert (await reload(entry)).attempts == 3


class TestTimeoutBookkeeping:
    @pytest.mark.asyncio
    async def test_the_marker_is_dropped_once_the_answer_is_handled(
        self, serialized_store
    ):
        await waiting_entry("late", timeout_at=past())
        handler = Recorder()
        manager = manager_with(handler)
        await manager.start()
        try:
            assert await until(lambda: handler.calls)
            assert await until(lambda: not manager._timeouts_enqueued)
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_a_lost_timeout_answer_is_queued_again_after_a_while(
        self, serialized_store
    ):
        await waiting_entry("late", timeout_at=past())
        manager = manager_with(Recorder())
        now = [1000.0]
        manager._clock = lambda: now[0]

        assert await manager.sweep_timeouts() == 1
        now[0] += manager._timeout_retry_after - 1
        assert await manager.sweep_timeouts() == 0  # still waiting for it
        now[0] += 2
        assert await manager.sweep_timeouts() == 1  # presumed lost: sent again

    @pytest.mark.asyncio
    async def test_a_restarted_manager_may_queue_it_again_harmlessly(
        self, serialized_store
    ):
        entry = await waiting_entry("late", timeout_at=past())
        await manager_with(Recorder()).sweep_timeouts()
        handler = Recorder()
        second = manager_with(handler)
        await second.start()
        try:
            assert await until(lambda: handler.calls)
            await asyncio.sleep(0.2)  # the duplicate arrives and is dropped
        finally:
            await second.stop()
        assert len(handler.calls) == 1
        assert (await reload(entry)).status == RequestStatus.EXPIRED.value


class TestAsATrigger:
    @pytest.fixture
    def state_store(self, serialized_store, monkeypatch):
        monkeypatch.setattr(
            TriggerStateRecord, "_backend_store", serialized_store, raising=False
        )
        monkeypatch.setattr(
            TriggerStateRecord, "_backend_initialized", True, raising=False
        )

    @pytest.mark.asyncio
    async def test_run_and_end_drive_the_record_and_the_loops(self, state_store):
        manager = manager_with(Recorder())
        await manager.run()
        assert manager.running
        assert (await TriggerStateRecord.get(manager.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await manager.end()
        assert not manager.running
        assert (await TriggerStateRecord.get(manager.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )

    @pytest.mark.asyncio
    async def test_a_failing_recovery_marks_the_manager_errored(
        self, state_store, monkeypatch
    ):
        manager = manager_with(Recorder())

        async def broken():
            raise ConnectionError("store down")

        monkeypatch.setattr(manager, "recover", broken)
        with pytest.raises(ConnectionError):
            await manager.run()
        assert manager.lifecycle is TriggerLifecycle.ERROR
        assert not manager.running

    @pytest.mark.asyncio
    async def test_it_can_be_restarted(self, serialized_store):
        entry = await waiting_entry()
        handler = Recorder()
        manager = manager_with(handler)
        await manager.start()
        await manager.stop()
        await manager.start()
        try:
            await ExternalCommunicationResponse.enqueue(answer(entry, "approve"))
            assert await until(lambda: handler.calls)
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_the_sweeper_survives_a_failing_sweep(
        self, serialized_store, monkeypatch
    ):
        manager = manager_with(Recorder(), timeout_sweep_interval=0.01)
        calls = []
        real = manager.sweep_timeouts

        async def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("store down")
            return await real()

        monkeypatch.setattr(manager, "sweep_timeouts", flaky)
        await manager.start()
        try:
            assert await until(lambda: len(calls) >= 3)
        finally:
            await manager.stop()
