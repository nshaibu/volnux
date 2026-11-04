import asyncio

import pytest

from volnux.backends.saga import (
    RetryPolicy,
    Saga,
    SagaStatus,
    SagaStep,
    SagaTimeoutError,
    StepStatus,
    execute_saga,
)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Record backoff delays instead of sleeping.

    This patches ``asyncio.sleep`` globally, so tests that need a step to hang
    must block on an ``asyncio.Event`` rather than ``asyncio.sleep``.
    """
    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *a, **k):
        delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr("volnux.backends.saga.saga.asyncio.sleep", fake_sleep)
    return delays


def make_saga(log, fail_at=None, **kwargs):
    saga = Saga(**kwargs)
    for n in range(3):

        def action(n=n):
            if n == fail_at:
                raise RuntimeError("boom %d" % n)
            log.append("do%d" % n)

        saga.add_step("s%d" % n, action, lambda n=n: log.append("undo%d" % n))
    return saga


class TestConstruction:
    def test_unnamed_saga_defaults_to_id(self):
        saga = Saga()
        assert saga.name == saga.saga_id and saga.saga_id

    def test_ids_are_unique(self):
        assert Saga().saga_id != Saga().saga_id

    def test_add_step_validation(self):
        saga = Saga().add_step("a", lambda: None, lambda: None)
        with pytest.raises(ValueError):
            saga.add_step("a", lambda: None, lambda: None)  # duplicate
        with pytest.raises(ValueError):
            saga.add_step("", lambda: None, lambda: None)
        with pytest.raises(ValueError):
            saga.add_step("b", "nope", lambda: None)
        with pytest.raises(ValueError):
            saga.add_step("b", lambda: None, None)
        with pytest.raises(ValueError):
            saga.add_step("b", lambda: None, lambda: None, timeout=0)

    @pytest.mark.asyncio
    async def test_cannot_add_step_after_execute(self):
        saga = Saga().add_step("a", lambda: None, lambda: None)
        await saga.execute()
        with pytest.raises(RuntimeError):
            saga.add_step("b", lambda: None, lambda: None)


class TestExecution:
    @pytest.mark.asyncio
    async def test_success(self, checkpoints):
        log = []
        result = await make_saga(log).execute()
        assert result.succeeded and result.error is None
        assert log == ["do0", "do1", "do2"]
        assert [s.status for s in result.steps] == [StepStatus.SUCCEEDED] * 3
        assert checkpoints == {}  # cleaned up

    @pytest.mark.asyncio
    async def test_async_and_sync_actions(self):
        log = []

        async def a():
            log.append("async")

        saga = Saga(enable_recovery=False)
        saga.add_step("a", a, lambda: None)
        saga.add_step("b", lambda: log.append("sync"), lambda: None)
        assert (await saga.execute()).succeeded
        assert log == ["async", "sync"]

    @pytest.mark.asyncio
    async def test_failure_compensates_previous_steps_in_reverse(self, checkpoints):
        log = []
        result = await make_saga(log, fail_at=2).execute()
        assert result.status == SagaStatus.COMPENSATED
        assert result.step_failed and not result.needs_intervention
        assert result.failed_step_index == 2
        assert log == ["do0", "do1", "undo1", "undo0"]
        assert result.steps[2].status == StepStatus.FAILED
        assert checkpoints == {}

    @pytest.mark.asyncio
    async def test_compensate_failed_step(self):
        log = []
        result = await make_saga(log, fail_at=1, compensate_failed_step=True).execute()
        assert log == ["do0", "undo1", "undo0"]
        assert result.status == SagaStatus.COMPENSATED

    @pytest.mark.asyncio
    async def test_first_step_failure_has_nothing_to_compensate(self):
        log = []
        result = await make_saga(log, fail_at=0).execute()
        assert result.status == SagaStatus.COMPENSATED
        assert log == []

    @pytest.mark.asyncio
    async def test_execute_guards(self):
        with pytest.raises(RuntimeError):
            await Saga().execute()  # no steps
        saga = make_saga([])
        await saga.execute()
        with pytest.raises(RuntimeError):
            await saga.execute()  # already ran
        for bad in (-1, 99):
            with pytest.raises(ValueError):
                await make_saga([]).execute(start_at_index=bad)

    @pytest.mark.asyncio
    async def test_execute_saga_helper(self):
        log = []
        steps = [
            SagaStep("a", lambda: log.append("a"), lambda: log.append("-a")),
            SagaStep("b", lambda: 1 / 0, lambda: log.append("-b")),
        ]
        result = await execute_saga(steps, name="helper")
        assert result.saga_name == "helper"
        assert result.status == SagaStatus.COMPENSATED
        assert log == ["a", "-a"]


class TestRetriesAndTimeouts:
    @pytest.mark.asyncio
    async def test_retry_then_succeed(self, no_sleep):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("x")

        saga = Saga(enable_recovery=False).add_step(
            "a",
            flaky,
            lambda: None,
            retry_policy=RetryPolicy(max_attempts=3, backoff_base=1, backoff_max=10),
        )
        result = await saga.execute()
        assert result.succeeded and result.steps[0].attempts == 3
        assert no_sleep == [1, 2]

    @pytest.mark.asyncio
    async def test_retries_exhausted(self):
        calls = []

        def bad():
            calls.append(1)
            raise ValueError("no")

        saga = Saga(enable_recovery=False).add_step(
            "a", bad, lambda: None, retry_policy=RetryPolicy(max_attempts=3)
        )
        result = await saga.execute()
        assert len(calls) == 3
        assert result.status == SagaStatus.COMPENSATED

    @pytest.mark.asyncio
    async def test_non_retryable_stops_immediately(self):
        calls = []

        def bad():
            calls.append(1)
            raise ValueError("no")

        policy = RetryPolicy(max_attempts=5, retryable_exceptions=(ConnectionError,))
        saga = Saga(enable_recovery=False).add_step(
            "a", bad, lambda: None, retry_policy=policy
        )
        await saga.execute()
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_timeout_keeps_timed_out_status(self):
        async def slow():
            await asyncio.Event().wait()

        saga = Saga(enable_recovery=False).add_step(
            "slow", slow, lambda: None, timeout=0.01
        )
        result = await saga.execute()
        assert isinstance(result.error, SagaTimeoutError)
        assert result.steps[0].status == StepStatus.TIMED_OUT

    @pytest.mark.asyncio
    async def test_user_timeouterror_without_step_timeout_is_plain_failure(self):
        def raises():
            raise TimeoutError("upstream")

        result = (
            await Saga(enable_recovery=False)
            .add_step("a", raises, lambda: None)
            .execute()
        )
        assert not isinstance(result.error, SagaTimeoutError)
        assert result.steps[0].status == StepStatus.FAILED

    @pytest.mark.asyncio
    async def test_timeout_retried_when_asyncio_timeout_is_retryable(self):
        attempts = []

        async def slow():
            attempts.append(1)
            await asyncio.Event().wait()

        policy = RetryPolicy(
            max_attempts=2, retryable_exceptions=(asyncio.TimeoutError,)
        )
        saga = Saga(enable_recovery=False).add_step(
            "slow", slow, lambda: None, retry_policy=policy, timeout=0.01
        )
        await saga.execute()
        assert len(attempts) == 2


class TestCancellation:
    @pytest.mark.asyncio
    async def test_cancel_compensates_then_reraises(self):
        log = []
        started = asyncio.Event()

        async def hang():
            started.set()
            await asyncio.Event().wait()

        saga = Saga(enable_recovery=False, compensate_failed_step=True)
        saga.add_step("a", lambda: log.append("a"), lambda: log.append("-a"))
        saga.add_step("b", hang, lambda: log.append("-b"))

        task = asyncio.ensure_future(saga.execute())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert log == ["a", "-b", "-a"]
        assert saga.status == SagaStatus.COMPENSATED
