import asyncio
from unittest.mock import patch

import pytest

from volnux.exceptions import MaxRetryError, StopProcessingError, SwitchTask
from volnux.mixins.event.retry import RetryPolicy

from ..conftest import make_event_class


@pytest.fixture
def sleeps(monkeypatch):
    """Record backoff sleeps instead of waiting."""
    calls = []

    async def fake_sleep(delay, *a, **k):
        calls.append(delay)

    monkeypatch.setattr("volnux.mixins.event.retry.asyncio.sleep", fake_sleep)
    return calls


def retrying(build, **policy):
    policy.setdefault("max_attempts", 3)
    policy.setdefault("backoff_factor", 0.1)
    policy.setdefault("max_backoff", 1.0)
    return build({"retry_policy": RetryPolicy(**policy)})


class TestPolicyConfiguration:
    def test_no_policy_by_default(self, build):
        event = build()
        assert event.retry_policy is None and event.init_retry() is None

    def test_dict_policy_is_converted_on_construction(self, build):
        event = build({"retry_policy": {"max_attempts": 4, "backoff_factor": 2}})
        assert isinstance(event.retry_policy, RetryPolicy)
        assert event.retry_policy.max_attempts == 4

    def test_config_retry_policy_accepts_single_type_list_and_tuple(self, build):
        event = build()
        event.config_retry_policy(2, retry_on_exceptions=ValueError)
        assert event.retry_policy.retry_on_exceptions == [ValueError]
        event.config_retry_policy(2, retry_on_exceptions=(KeyError, OSError))
        assert event.retry_policy.retry_on_exceptions == [KeyError, OSError]
        event.config_retry_policy(2)
        assert event.retry_policy.retry_on_exceptions == []

    def test_unknown_policy_key_is_rejected(self, build):
        with pytest.raises(TypeError):
            build({"retry_policy": {"max_attempt": 1}})


class TestBackoff:
    @pytest.mark.parametrize(
        "count, expected", [(0, 0), (1, 0), (2, 0.2), (3, 0.4), (4, 0.8), (5, 1.0)]
    )
    def test_exponential_with_cap(self, build, count, expected):
        event = retrying(build, backoff_factor=0.1, max_backoff=1.0)
        event._retry_count = count
        assert event.get_backoff_time() == pytest.approx(expected)

    def test_no_policy_means_no_backoff(self, build):
        event = build()
        event._retry_count = 5
        assert event.get_backoff_time() == 0

    @pytest.mark.asyncio
    async def test_zero_backoff_does_not_sleep(self, build, sleeps):
        event = retrying(build)
        event._retry_count = 1
        assert await event._sleep_for_backoff() == 0
        assert sleeps == []


class TestRetryableAndExhausted:
    def test_everything_retryable_when_no_exceptions_configured(self, build):
        event = retrying(build)
        assert event.is_retryable(ValueError()) and event.is_retryable(RuntimeError())

    def test_only_configured_exceptions_and_subclasses(self, build):
        event = retrying(build, retry_on_exceptions=[OSError])
        assert event.is_retryable(FileNotFoundError())
        assert not event.is_retryable(ValueError())

    def test_no_policy_is_never_retryable(self, build):
        assert not build().is_retryable(ValueError())

    def test_control_flow_exceptions_are_not_retryable(self, build):
        event = retrying(build)
        for exc in (
            SwitchTask(current_task_id="t", next_task_descriptor=1, result=None),
            StopProcessingError(message="stop", exception=None),
        ):
            assert not event.is_retryable(exc)

    def test_exhausted(self, build):
        event = retrying(build, max_attempts=2)
        assert not event.is_exhausted()
        event._retry_count = 2
        assert event.is_exhausted()
        assert build().is_exhausted()  # no policy: nothing left to retry


class TestRetry:
    @pytest.mark.asyncio
    async def test_without_policy_runs_once_and_propagates(self, build):
        event = build()

        async def boom():
            raise ValueError("x")

        with pytest.raises(ValueError):
            await event._retry(boom)
        assert event._retry_count == 0

    @pytest.mark.asyncio
    async def test_success_first_try(self, build, sleeps):
        event = retrying(build)

        async def ok(a, b=0):
            return True, a + b

        assert await event._retry(ok, 1, b=2) == (True, 3)
        assert event._retry_count == 1 and sleeps == []

    @pytest.mark.asyncio
    async def test_succeeds_after_retries_with_backoff_and_signals(
        self, build, sleeps, recorded
    ):
        event = retrying(build, max_attempts=4)
        calls = []

        async def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("again")
            return True, "done"

        assert await event._retry(flaky) == (True, "done")
        assert len(calls) == 3
        assert sleeps == [pytest.approx(0.2)]  # no backoff after the 1st failure
        assert [r["retry_count"] for r in recorded["event_execution_retry"]] == [1, 2]
        assert recorded["event_execution_retry_done"] == []

    @pytest.mark.asyncio
    async def test_exhaustion_raises_max_retry_error_with_last_exception(
        self, build, sleeps, recorded
    ):
        event = retrying(build, max_attempts=3)
        errors = iter([ValueError("first"), KeyError("second"), ValueError("last")])

        async def always_fails():
            raise next(errors)

        with pytest.raises(MaxRetryError) as info:
            await event._retry(always_fails)

        assert str(info.value.exception) == "last"
        assert info.value.attempt == 3
        assert len(recorded["event_execution_retry_done"]) == 1

    @pytest.mark.asyncio
    async def test_no_sleep_or_retry_signal_after_the_final_attempt(
        self, build, sleeps, recorded
    ):
        """Regression: the last failure used to sleep and announce a retry that never came."""
        event = retrying(build, max_attempts=3, backoff_factor=0.1)

        async def always_fails():
            raise ValueError("x")

        with pytest.raises(MaxRetryError):
            await event._retry(always_fails)

        # attempts 1..3 fail; only the failure of attempt 2 is followed by a retry
        assert sleeps == [pytest.approx(0.2)]
        assert [r["retry_count"] for r in recorded["event_execution_retry"]] == [1, 2]

    @pytest.mark.asyncio
    async def test_single_attempt_policy_never_sleeps(self, build, sleeps, recorded):
        event = retrying(build, max_attempts=1)

        async def always_fails():
            raise ValueError("x")

        with pytest.raises(MaxRetryError):
            await event._retry(always_fails)
        assert sleeps == [] and recorded["event_execution_retry"] == []

    @pytest.mark.asyncio
    async def test_zero_attempts_raises_without_calling(self, build):
        event = retrying(build, max_attempts=0)
        called = []

        async def fn():
            called.append(1)

        with pytest.raises(MaxRetryError) as info:
            await event._retry(fn)
        assert called == [] and info.value.exception is None

    @pytest.mark.asyncio
    async def test_non_retryable_exception_propagates_immediately(self, build, sleeps):
        event = retrying(build, retry_on_exceptions=[ValueError])
        calls = []

        async def fn():
            calls.append(1)
            raise KeyError("nope")

        with pytest.raises(KeyError):
            await event._retry(fn)
        assert len(calls) == 1 and sleeps == []

    @pytest.mark.asyncio
    async def test_control_flow_is_not_retried(self, build, sleeps):
        event = retrying(build)
        calls = []

        async def fn():
            calls.append(1)
            raise SwitchTask(current_task_id="t", next_task_descriptor=1, result=None)

        with pytest.raises(SwitchTask):
            await event._retry(fn)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_inner_max_retry_error_returns_plain_failure(self, build):
        event = retrying(build)

        async def inner_gave_up():
            raise MaxRetryError(attempt=3, exception=ValueError("x"))

        assert await event._retry(inner_gave_up) == (False, None)

    @pytest.mark.asyncio
    async def test_cancellation_is_not_swallowed(self, build, sleeps):
        event = retrying(build)

        async def cancelled():
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await event._retry(cancelled)

    @pytest.mark.asyncio
    async def test_retry_count_carries_over_between_calls(self, build, sleeps):
        """A resumed event keeps its count: the second run only gets what is left."""
        event = retrying(build, max_attempts=3)
        event._retry_count = 2
        calls = []

        async def fn():
            calls.append(1)
            raise ValueError("x")

        with pytest.raises(MaxRetryError):
            await event._retry(fn)
        assert len(calls) == 1
