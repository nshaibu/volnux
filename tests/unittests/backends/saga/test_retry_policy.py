import pytest

from volnux.backends.saga import RetryPolicy


class TestRetryPolicy:
    def test_defaults_retry_everything(self):
        assert RetryPolicy().is_retryable(ValueError())

    def test_empty_tuple_retries_nothing(self):
        assert not RetryPolicy(retryable_exceptions=()).is_retryable(ValueError())

    def test_only_listed_exceptions_retry(self):
        policy = RetryPolicy(retryable_exceptions=(ConnectionError,))
        assert policy.is_retryable(ConnectionError())
        assert not policy.is_retryable(ValueError())

    def test_exponential_backoff_is_capped(self):
        policy = RetryPolicy(backoff_base=1.0, backoff_max=5.0)
        assert [policy.delay_for_attempt(n) for n in (1, 2, 3, 4)] == [1, 2, 4, 5]

    @pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"backoff_base": -1}])
    def test_invalid_values_rejected(self, kwargs):
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)
