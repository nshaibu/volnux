import pytest

from volnux.backends.saga import (
    Saga,
    SagaRecoveryManager,
    SagaStatus,
    SagaStep,
    StepStatus,
)
from volnux.backends.saga.recovery import SagaRecoveryState


def steps(log, fail_at=None, undo_fails=()):
    out = []
    for n in range(3):

        def do(n=n):
            if n == fail_at:
                raise RuntimeError("fail %d" % n)
            log.append("do%d" % n)

        def undo(n=n):
            if n in undo_fails:
                raise RuntimeError("undo %d" % n)
            log.append("undo%d" % n)

        out.append(SagaStep("s%d" % n, do, undo))
    return out


class TestRecoveryManager:
    @pytest.mark.asyncio
    async def test_roundtrip_uses_saga_id_as_key(self, checkpoints):
        step = SagaStep("a", lambda: None, lambda: None)
        await SagaRecoveryManager.save_state(
            "saga-1", "n", 0, "running", [step], created_at=1.0
        )
        assert list(checkpoints) == ["saga-1"]
        state = await SagaRecoveryManager.load_state("saga-1")
        assert state.saga_id == "saga-1" and state.steps_state[0]["name"] == "a"

    @pytest.mark.asyncio
    async def test_repeated_saves_overwrite_one_record(self, checkpoints):
        for i in range(3):
            await SagaRecoveryManager.save_state("s", "n", i, "running", [], 1.0)
        assert len(checkpoints) == 1
        assert checkpoints["s"].current_step_index == 2

    @pytest.mark.asyncio
    async def test_delete_and_missing(self, checkpoints):
        await SagaRecoveryManager.save_state("s", "n", 0, "running", [], 1.0)
        await SagaRecoveryManager.delete_state("s")
        assert checkpoints == {}
        assert await SagaRecoveryManager.load_state("s") is None
        await SagaRecoveryManager.delete_state("s")  # no error

    def test_state_importable_without_project_config(self):
        state = SagaRecoveryState(
            saga_id="x",
            saga_name="n",
            current_step_index=0,
            status="running",
            steps_state=[],
            created_at=1.0,
            updated_at=1.0,
        )
        assert state.compensate_failed_step is False


class TestCheckpointLifecycle:
    @pytest.mark.asyncio
    async def test_checkpoint_written_before_each_step(self, monkeypatch, checkpoints):
        seen = []
        real = SagaRecoveryManager.save_state

        async def spy(**kw):
            seen.append(kw["current_step_index"])
            return await real(**kw)

        monkeypatch.setattr(
            "volnux.backends.saga.saga.SagaRecoveryManager.save_state", spy
        )
        saga = Saga()
        for s in steps([]):
            saga.add_step(s.name, s.action, s.compensation)
        await saga.execute()
        assert seen == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_recovery_disabled_writes_nothing(self, checkpoints):
        saga = Saga(enable_recovery=False)
        for s in steps([]):
            saga.add_step(s.name, s.action, s.compensation)
        await saga.execute()
        assert checkpoints == {}


class TestResume:
    @pytest.mark.asyncio
    async def test_missing_checkpoint(self):
        with pytest.raises(ValueError, match="No active recovery"):
            await Saga.resume("nope", steps([]))

    @pytest.mark.asyncio
    async def test_resume_forward_from_checkpoint(self, checkpoints):
        log = []
        # Simulate a crash while step 1 was in flight.
        defs = steps(log)
        defs[0].status = StepStatus.SUCCEEDED
        await SagaRecoveryManager.save_state("sid", "order", 1, "running", defs, 1.0)

        result = await Saga.resume("sid", steps(log))
        assert result.succeeded and result.saga_id == "sid"
        assert result.saga_name == "order"
        assert log == ["do1", "do2"]  # step 0 not re-run
        assert checkpoints == {}

    @pytest.mark.asyncio
    async def test_resume_forward_failure_compensates_earlier_steps(self):
        log = []
        defs = steps(log)
        defs[0].status = StepStatus.SUCCEEDED
        await SagaRecoveryManager.save_state("sid", "n", 1, "running", defs, 1.0)

        result = await Saga.resume("sid", steps(log, fail_at=1))
        assert result.status == SagaStatus.COMPENSATED
        assert log == ["undo0"]

    @pytest.mark.asyncio
    async def test_resume_mid_compensation_skips_compensated_steps(self, checkpoints):
        log = []
        defs = steps(log)
        defs[0].status = StepStatus.SUCCEEDED
        defs[1].status = StepStatus.COMPENSATED
        defs[2].status = StepStatus.FAILED
        await SagaRecoveryManager.save_state(
            "sid",
            "n",
            2,
            "compensating",
            defs,
            1.0,
            compensate_failed_step=False,
            failed_step_index=2,
        )

        result = await Saga.resume("sid", steps(log))
        assert result.status == SagaStatus.COMPENSATED
        assert result.failed_step_index == 2
        assert log == ["undo0"]  # undo1 already done
        assert checkpoints == {}

    @pytest.mark.asyncio
    async def test_compensate_failed_step_flag_persists(self):
        log = []
        saga = Saga(compensate_failed_step=True)
        for s in steps(log, fail_at=1, undo_fails={0}):
            saga.add_step(s.name, s.action, s.compensation)
        first = await saga.execute()
        assert first.status == SagaStatus.PARTIALLY_COMPENSATED
        assert log == ["do0", "undo1"]

        # Operator resumes with a healthy undo0: failed step is compensated
        # again? No - it is already COMPENSATED and is skipped.
        log.clear()
        result = await Saga.resume(saga.saga_id, steps(log))
        assert result.status == SagaStatus.COMPENSATED
        assert log == ["undo0"]

    @pytest.mark.asyncio
    async def test_resume_validation(self):
        defs = steps([])
        await SagaRecoveryManager.save_state("sid", "n", 0, "running", defs, 1.0)

        with pytest.raises(ValueError, match="count mismatch"):
            await Saga.resume("sid", defs[:2])

        renamed = steps([])
        renamed[1].name = "other"
        with pytest.raises(ValueError, match="name mismatch"):
            await Saga.resume("sid", renamed)

    @pytest.mark.asyncio
    async def test_resume_corrupt_status(self, checkpoints):
        await SagaRecoveryManager.save_state("sid", "n", 0, "running", steps([]), 1.0)
        checkpoints["sid"].steps_state[0]["status"] = "garbage"
        with pytest.raises(ValueError, match="corrupt status"):
            await Saga.resume("sid", steps([]))
