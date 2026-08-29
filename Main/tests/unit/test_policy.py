"""Unit tests for RetryPolicy configuration object."""

from dataclasses import FrozenInstanceError
import pytest
from pgshield.retry.classifier import DEFAULT_RETRYABLE_SQLSTATES
from pgshield.retry.policy import RetryPolicy


class TestRetryPolicy:
    """Test RetryPolicy initialization, defaults, validation, and immutability."""

    def test_default_values(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 5
        assert policy.base_delay == 0.05
        assert policy.max_delay == 2.0
        assert policy.backoff_factor == 2.0
        assert policy.jitter is True
        assert policy.retryable_sqlstates == frozenset(DEFAULT_RETRYABLE_SQLSTATES)

    def test_custom_valid_configuration(self):
        policy = RetryPolicy(
            max_attempts=3,
            base_delay=0.1,
            max_delay=1.0,
            backoff_factor=1.5,
            jitter=False,
            retryable_sqlstates={"40001"},
        )
        assert policy.max_attempts == 3
        assert policy.base_delay == 0.1
        assert policy.max_delay == 1.0
        assert policy.backoff_factor == 1.5
        assert policy.jitter is False
        assert policy.retryable_sqlstates == frozenset({"40001"})

    def test_immutability(self):
        policy = RetryPolicy()
        with pytest.raises(FrozenInstanceError):
            policy.max_attempts = 10  # type: ignore

    def test_invalid_max_attempts_zero_or_negative(self):
        with pytest.raises(ValueError, match="max_attempts must be a positive integer"):
            RetryPolicy(max_attempts=0)

        with pytest.raises(ValueError, match="max_attempts must be a positive integer"):
            RetryPolicy(max_attempts=-1)

    def test_invalid_base_delay_negative(self):
        with pytest.raises(ValueError, match="base_delay must be non-negative"):
            RetryPolicy(base_delay=-0.01)

    def test_invalid_max_delay_less_than_base_delay(self):
        with pytest.raises(ValueError, match="cannot be less than base_delay"):
            RetryPolicy(base_delay=1.0, max_delay=0.5)

    def test_invalid_backoff_factor_less_than_one(self):
        with pytest.raises(ValueError, match="backoff_factor must be >= 1.0"):
            RetryPolicy(backoff_factor=0.9)

    def test_retryable_sqlstates_normalized_to_uppercase(self):
        policy = RetryPolicy(retryable_sqlstates={"40001", "40p01"})
        assert "40P01" in policy.retryable_sqlstates
