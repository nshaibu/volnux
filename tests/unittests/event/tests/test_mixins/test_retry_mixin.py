"""
Tests for `RetryMixin` and `RetryPolicy`.

Scope:
  * RetryPolicy dataclass defaults & explicit construction.
  * init_retry() — converts dict → RetryPolicy, leaves RetryPolicy alone,
    leaves None alone.
  * config_retry_policy() — builds RetryPolicy from kwargs.
  * get_backoff_time() — formula: min(backoff_factor * 2^(retry_count-1),
    max_backoff); 0 when policy is None or retry_count <= 1.
  * _sleep_for_backoff() — awaits asyncio.sleep(backoff) when backoff > 0,
    returns the slept seconds; returns 0 when backoff is 0.
  * is_retryable() — None policy → False; empty retry_on_exceptions → True
    for any Exception; specific list → True only for matching classes; name
    comparison quirk.
  * is_exhausted() — None policy → True; retry_count >= max_attempts → True.
  * _retry() — happy path (first attempt success), retryable-then-success,
    retryable-then-exhaustion → MaxRetryError, non-retryable → re-raise,
    MaxRetryError caught and broken out, signal emissions.
"""

from __future__ import annotations

import asyncio
from typing import Tuple, Any
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

def _import_retry():
    from volnux.mixins.event import (
        RetryMixin,
        RetryPolicy,
        RetryConfigDict,
    )
    return RetryMixin, RetryPolicy, RetryConfigDict


# ---------------------------------------------------------------------------
# RetryPolicy dataclass
# ---------------------------------------------------------------------------

class TestRetryPolicyDefaults:
    def test_default_max_attempts(self):
        _, RetryPolicy, _ = _import_retry()
        from volnux.constants import MAX_RETRIES
        # Defaults come from conf.get(...) which the stub returns default for.
        # So max_attempts default should be MAX_RETRIES.
        p = RetryPolicy()
        assert p.max_attempts == MAX_RETRIES

    def test_default_backoff_factor(self):
        _, RetryPolicy, _ = _import_retry()
        from volnux.constants import MAX_BACKOFF_FACTOR
        p = RetryPolicy()
        assert p.backoff_factor == MAX_BACKOFF_FACTOR

    def test_default_max_backoff(self):
        _, RetryPolicy, _ = _import_retry()
        from volnux.constants import MAX_BACKOFF
        p = RetryPolicy()
        assert p.max_backoff == MAX_BACKOFF

    def test_default_retry_on_exceptions_empty_list(self):
        _, RetryPolicy, _ = _import_retry()
        p = RetryPolicy()
        assert p.retry_on_exceptions == []

    def test_explicit_construction(self):
        _, RetryPolicy, _ = _import_retry()
        p = RetryPolicy(
            max_attempts=5,
            backoff_factor=2.0,
            max_backoff=60.0,
            retry_on_exceptions=[ValueError, TypeError],
        )
        assert p.max_attempts == 5
        assert p.backoff_factor == 2.0
        assert p.max_backoff == 60.0
        assert p.retry_on_exceptions == [ValueError, TypeError]


# ---------------------------------------------------------------------------
# init_retry
# ---------------------------------------------------------------------------

class TestInitRetry:
    def test_dict_is_converted_to_retry_policy(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            retry_policy = {"max_attempts": 3, "backoff_factor": 1.0}

        h = _Holder()
        result = h.init_retry()
        assert isinstance(result, RetryPolicy)
        assert result.max_attempts == 3
        assert result.backoff_factor == 1.0
        # And the attribute is updated in place
        assert isinstance(h.retry_policy, RetryPolicy)

    def test_existing_retry_policy_is_returned_unchanged(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        existing = RetryPolicy(max_attempts=7)

        class _Holder(RetryMixin):
            retry_policy = existing

        h = _Holder()
        result = h.init_retry()
        assert result is existing

    def test_none_returns_none(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            retry_policy = None

        h = _Holder()
        assert h.init_retry() is None
        # And the attribute stays None
        assert h.retry_policy is None


# ---------------------------------------------------------------------------
# config_retry_policy
# ---------------------------------------------------------------------------

class TestConfigRetryPolicy:
    def test_builds_retry_policy_with_provided_args(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(
            max_attempts=4,
            backoff_factor=1.5,
            max_backoff=30.0,
        )
        assert isinstance(h.retry_policy, RetryPolicy)
        assert h.retry_policy.max_attempts == 4
        assert h.retry_policy.backoff_factor == 1.5
        assert h.retry_policy.max_backoff == 30.0
        assert h.retry_policy.retry_on_exceptions == []

    def test_single_exception_class_wrapped_in_list(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(
            max_attempts=3,
            retry_on_exceptions=ValueError,
        )
        assert h.retry_policy.retry_on_exceptions == [ValueError]

    def test_list_of_exception_classes(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(
            max_attempts=3,
            retry_on_exceptions=[ValueError, TypeError],
        )
        assert h.retry_policy.retry_on_exceptions == [ValueError, TypeError]

    def test_tuple_of_exception_classes(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(
            max_attempts=3,
            retry_on_exceptions=(ValueError, TypeError),
        )
        assert h.retry_policy.retry_on_exceptions == [ValueError, TypeError]

    def test_none_retry_on_exceptions_yields_empty_list(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(max_attempts=3, retry_on_exceptions=None)
        assert h.retry_policy.retry_on_exceptions == []

    def test_uses_defaults_for_backoff_when_not_provided(self):
        RetryMixin, _, _ = _import_retry()
        from volnux.constants import MAX_BACKOFF_FACTOR, MAX_BACKOFF

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.config_retry_policy(max_attempts=2)
        assert h.retry_policy.backoff_factor == MAX_BACKOFF_FACTOR
        assert h.retry_policy.max_backoff == MAX_BACKOFF


# ---------------------------------------------------------------------------
# get_backoff_time
# ---------------------------------------------------------------------------

class TestGetBackoffTime:
    def _make_holder(self, retry_policy=None):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = retry_policy
        h._retry_count = 0
        return h

    def test_none_policy_returns_zero(self):
        h = self._make_holder(retry_policy=None)
        assert h.get_backoff_time() == 0

    def test_retry_count_zero_returns_zero(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=3, backoff_factor=1.0))
        h._retry_count = 0
        assert h.get_backoff_time() == 0

    def test_retry_count_one_returns_zero(self):
        """Per source: `self._retry_count <= 1` → 0."""
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=3, backoff_factor=1.0))
        h._retry_count = 1
        assert h.get_backoff_time() == 0

    def test_retry_count_two_with_factor_one(self):
        """backoff = 1.0 * 2^(2-1) = 2.0"""
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=5, backoff_factor=1.0, max_backoff=100))
        h._retry_count = 2
        assert h.get_backoff_time() == 2.0

    def test_retry_count_three_with_factor_one(self):
        """backoff = 1.0 * 2^(3-1) = 4.0"""
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=5, backoff_factor=1.0, max_backoff=100))
        h._retry_count = 3
        assert h.get_backoff_time() == 4.0

    def test_backoff_capped_at_max_backoff(self):
        """When computed backoff exceeds max_backoff, return max_backoff."""
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=99, backoff_factor=100.0, max_backoff=30.0))
        h._retry_count = 5  # 100 * 2^4 = 1600, capped at 30
        assert h.get_backoff_time() == 30.0


# ---------------------------------------------------------------------------
# _sleep_for_backoff
# ---------------------------------------------------------------------------

class TestSleepForBackoff:

    @pytest.mark.asyncio
    async def test_no_sleep_when_backoff_zero(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = None
        h._retry_count = 0
        with patch("volnux.mixins.event.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            result = await h._sleep_for_backoff()
            assert result == 0
            mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sleeps_for_computed_backoff(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(max_attempts=5, backoff_factor=1.0, max_backoff=100)
        h._retry_count = 2  # backoff = 2.0
        with patch("volnux.mixins.event.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            result = await h._sleep_for_backoff()
            assert result == 2.0
            mock_sleep.assert_awaited_once_with(2.0)


# ---------------------------------------------------------------------------
# is_retryable
# ---------------------------------------------------------------------------

class TestIsRetryable:
    def test_none_policy_returns_false(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = None
        assert h.is_retryable(ValueError("boom")) is False

    def test_empty_retry_on_exceptions_returns_true_for_exception(self):
        """When retry_on_exceptions is empty, the policy retries on ANY
        Exception (the `not retry_on_exceptions` short-circuit)."""
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(retry_on_exceptions=[])
        assert h.is_retryable(ValueError("boom")) is True

    def test_empty_retry_on_exceptions_returns_false_for_non_exception(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(retry_on_exceptions=[])
        # Non-exception input should fail the `isinstance(exception, Exception)`
        # check on the return statement.
        assert h.is_retryable("not-an-exception") is False  # type: ignore[arg-type]

    def test_matching_exception_class_returns_true(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(retry_on_exceptions=[ValueError])
        assert h.is_retryable(ValueError("boom")) is True

    def test_non_matching_exception_class_returns_false(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(retry_on_exceptions=[ValueError])
        assert h.is_retryable(TypeError("boom")) is False

    def test_subclass_matches_parent_in_list(self):
        """isinstance check means subclasses of listed classes match."""
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        class _Sub(ValueError):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(retry_on_exceptions=[ValueError])
        assert h.is_retryable(_Sub("boom")) is True


# ---------------------------------------------------------------------------
# is_exhausted
# ---------------------------------------------------------------------------

class TestIsExhausted:
    def test_none_policy_returns_true(self):
        RetryMixin, _, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = None
        assert h.is_exhausted() is True

    def test_retry_count_below_max_returns_false(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(max_attempts=5)
        h._retry_count = 2
        assert h.is_exhausted() is False

    def test_retry_count_equals_max_returns_true(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(max_attempts=5)
        h._retry_count = 5
        assert h.is_exhausted() is True

    def test_retry_count_above_max_returns_true(self):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = RetryPolicy(max_attempts=5)
        h._retry_count = 99
        assert h.is_exhausted() is True


# ---------------------------------------------------------------------------
# _retry
# ---------------------------------------------------------------------------

class TestRetry:
    def _make_holder(self, retry_policy=None):
        RetryMixin, RetryPolicy, _ = _import_retry()

        class _Holder(RetryMixin):
            pass

        h = _Holder()
        h.retry_policy = retry_policy
        h._retry_count = 0
        # _retry reads self._execution_context.__class__ and self._task_id
        # for signal emission — mock them.
        h._execution_context = MagicMock(name="ExecutionContext")
        h._task_id = "test-task-id"
        return h

    @pytest.mark.asyncio
    async def test_none_policy_calls_func_once_and_returns_result(self):
        h = self._make_holder(retry_policy=None)
        call_count = 0

        async def _func(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return True, {"ok": True}

        result = await h._retry(_func, "arg1", kwarg="v")
        assert result == (True, {"ok": True})
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_first_attempt_success_no_retry(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=3, retry_on_exceptions=[ValueError]))
        call_count = 0

        async def _func(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return True, "ok"

        result = await h._retry(_func)
        assert result == (True, "ok")
        assert call_count == 1
        assert h._retry_count == 1

    @pytest.mark.asyncio
    async def test_retryable_then_success(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=3, retry_on_exceptions=[ValueError], backoff_factor=0.0))
        attempt = 0

        async def _func(*args, **kwargs):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise ValueError("first attempt fails")
            return True, "ok"

        # Mock the backoff sleep so the test is fast
        with patch.object(h, "_sleep_for_backoff", new=AsyncMock(return_value=0)):
            result = await h._retry(_func)
        assert result == (True, "ok")
        assert attempt == 2
        assert h._retry_count == 2

    @pytest.mark.asyncio
    async def test_exhaustion_raises_max_retry_error(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=2, retry_on_exceptions=[ValueError], backoff_factor=0.0))

        async def _func(*args, **kwargs):
            raise ValueError("always fails")

        from volnux.exceptions import MaxRetryError
        with patch.object(h, "_sleep_for_backoff", new=AsyncMock(return_value=0)):
            with patch("volnux.signal.signals.event_execution_retry_done.emit_async", new=AsyncMock()):
                with pytest.raises(MaxRetryError) as exc_info:
                    await h._retry(_func)
                # The exception carried should be the most recent ValueError
                assert isinstance(exc_info.value.exception, ValueError)

    @pytest.mark.asyncio
    async def test_non_retryable_exception_propagates(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=5, retry_on_exceptions=[ValueError]))

        async def _func(*args, **kwargs):
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            await h._retry(_func)

    @pytest.mark.asyncio
    async def test_max_retry_error_caught_and_breaks(self):
        """When func raises MaxRetryError, _retry catches it and breaks
        out of the loop, returning (False, None)."""
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=5, retry_on_exceptions=[ValueError]))

        from volnux.exceptions import MaxRetryError

        async def _func(*args, **kwargs):
            raise MaxRetryError(attempt=99, exception=ValueError("inner"), reason="inner exhausted")

        # Should not raise — MaxRetryError is caught inside the try block
        result = await h._retry(_func)
        assert result == (False, None)

    @pytest.mark.asyncio
    async def test_emits_event_execution_retry_signal_on_retry(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=3, retry_on_exceptions=[ValueError], backoff_factor=0.0))

        attempt = 0

        async def _func(*args, **kwargs):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise ValueError("first fails")
            return True, "ok"

        with patch.object(h, "_sleep_for_backoff", new=AsyncMock(return_value=0)):
            with patch("volnux.signal.signals.event_execution_retry.emit_async", new=AsyncMock()) as mock_emit:
                await h._retry(_func)
                mock_emit.assert_awaited()

    @pytest.mark.asyncio
    async def test_emits_event_execution_retry_done_on_exhaustion(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=RetryPolicy(max_attempts=2, retry_on_exceptions=[ValueError], backoff_factor=0.0))

        async def _func(*args, **kwargs):
            raise ValueError("always fails")

        with patch.object(h, "_sleep_for_backoff", new=AsyncMock(return_value=0)):
            with patch("volnux.signal.signals.event_execution_retry_done.emit_async", new=AsyncMock()) as mock_emit:
                from volnux.exceptions import MaxRetryError
                with pytest.raises(MaxRetryError):
                    await h._retry(_func)
                mock_emit.assert_awaited()

    @pytest.mark.asyncio
    async def test_passes_args_and_kwargs_to_func(self):
        RetryMixin, RetryPolicy, _ = _import_retry()
        h = self._make_holder(retry_policy=None)
        received_args = ()
        received_kwargs = {}

        async def _func(*args, **kwargs):
            nonlocal received_args, received_kwargs
            received_args = args
            received_kwargs = kwargs
            return True, None

        await h._retry(_func, "pos1", "pos2", key1="v1", key2="v2")
        assert received_args == ("pos1", "pos2")
        assert received_kwargs == {"key1": "v1", "key2": "v2"}
