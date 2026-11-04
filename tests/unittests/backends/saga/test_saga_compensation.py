import pytest

from volnux.backends.saga import (
    RetryPolicy,
    Saga,
    SagaCompensationFailureError,
    SagaStatus,
    StepStatus,
)
from volnux.backends.saga.recovery import SagaRecoveryManager


def failing_saga(undo_fails=(), **kwargs):
    log = []
    saga = Saga(**kwargs)
    for n in range(3):

        def undo(n=n):
            if n in undo_fails:
                raise RuntimeError("undo %d failed" % n)
            log.append("undo%d" % n)

        def do(n=n):
            if n == 2:
                raise RuntimeError("step 2 failed")

        saga.add_step("s%d" % n, do, undo)
    return saga, log


class TestCompensationFailures:
    @pytest.mark.asyncio
    async def test_all_compensations_attempted_despite_failure(self, dlq, checkpoints):
        saga, log = failing_saga(undo_fails={1})
        result = await saga.execute()

        assert result.status == SagaStatus.PARTIALLY_COMPENSATED
        assert result.needs_intervention and result.step_failed
        assert log == ["undo0"]  # undo1 failed, undo0 still ran
        assert result.steps[1].status == StepStatus.COMPENSATION_FAILED
        assert result.steps[0].status == StepStatus.COMPENSATED
        assert isinstance(result.error, SagaCompensationFailureError)
        assert [i for i, _, _ in result.error.compensation_failures] == [1]
        assert "step 2 failed" in str(result.error.original_error)

    @pytest.mark.asyncio
    async def test_dead_letter_entry_and_checkpoint_retained(self, dlq, checkpoints):
        saga, _ = failing_saga(undo_fails={1})
        await saga.execute()

        assert len(dlq) == 1
        entry = dlq[0]
        assert entry.saga_id == saga.saga_id
        assert entry.failed_step_index == 2 and entry.failed_step_name == "s2"
        assert entry.compensation_failures[0]["step_name"] == "s1"
        assert saga.saga_id in checkpoints

    @pytest.mark.asyncio
    async def test_dlq_failure_keeps_partially_compensated(self, monkeypatch):
        async def boom(entry):
            raise ConnectionError("redis down")

        monkeypatch.setattr("volnux.backends.saga.saga.DeadLetterEntry.enqueue", boom)
        saga, _ = failing_saga(undo_fails={0})
        result = await saga.execute()
        assert result.status == SagaStatus.PARTIALLY_COMPENSATED
        assert result.needs_intervention

    @pytest.mark.asyncio
    async def test_compensation_retry(self, monkeypatch):
        async def nosleep(*a, **k):
            return None

        monkeypatch.setattr("volnux.backends.saga.saga.asyncio.sleep", nosleep)
        attempts = []

        def flaky_undo():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("again")

        saga = Saga(enable_recovery=False)
        saga.add_step(
            "a", lambda: None, flaky_undo, retry_policy=RetryPolicy(max_attempts=3)
        )
        saga.add_step("b", lambda: 1 / 0, lambda: None)
        result = await saga.execute()
        assert result.status == SagaStatus.COMPENSATED
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_checkpoint_failure_triggers_rollback(self, monkeypatch):
        log = []
        saga = Saga()
        saga.add_step("a", lambda: log.append("a"), lambda: log.append("-a"))
        saga.add_step("b", lambda: log.append("b"), lambda: log.append("-b"))

        real = SagaRecoveryManager.save_state
        calls = []

        async def flaky_save(**kwargs):
            calls.append(kwargs["current_step_index"])
            if len(calls) == 2:
                raise ConnectionError("store down")
            return await real(**kwargs)

        monkeypatch.setattr(
            "volnux.backends.saga.saga.SagaRecoveryManager.save_state", flaky_save
        )
        result = await saga.execute()

        assert log == ["a", "-a"]  # b never ran; a rolled back
        assert result.status == SagaStatus.COMPENSATED
        assert result.failed_step_index == 1
        assert result.steps[1].status == StepStatus.PENDING

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_change_outcome(self, monkeypatch):
        async def boom(saga_id):
            raise RuntimeError("cannot delete")

        monkeypatch.setattr(
            "volnux.backends.saga.saga.SagaRecoveryManager.delete_state", boom
        )
        saga = Saga().add_step("a", lambda: None, lambda: None)
        assert (await saga.execute()).succeeded
