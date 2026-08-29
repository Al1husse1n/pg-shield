"""Unit tests for backoff calculation subsystem."""

import pytest
from pgshield.retry.backoff import calculate_backoff
from pgshield.retry.policy import RetryPolicy


class TestBackoffCalculation:
    """Test exponential backoff calculation, jitter behavior, and boundaries."""

    def test_delay_is_never_negative(self):
        policy = RetryPolicy(base_delay=0.05, max_delay=2.0)
        for attempt in range(10):
            delay = calculate_backoff(attempt, policy)
            assert delay >= 0.0

    def test_delay_never_exceeds_max_delay(self):
        policy = RetryPolicy(base_delay=0.05, max_delay=1.0)
        # Even with deterministic max rng
        max_rng = lambda a, b: b
        for attempt in range(20):
            delay = calculate_backoff(attempt, policy, random_func=max_rng)
            assert delay <= 1.0

    def test_deterministic_exponential_growth_without_jitter(self):
        policy = RetryPolicy(
            base_delay=0.05,
            max_delay=2.0,
            backoff_factor=2.0,
            jitter=False,
        )

        assert calculate_backoff(0, policy) == pytest.approx(0.05)
        assert calculate_backoff(1, policy) == pytest.approx(0.10)
        assert calculate_backoff(2, policy) == pytest.approx(0.20)
        assert calculate_backoff(3, policy) == pytest.approx(0.40)
        assert calculate_backoff(4, policy) == pytest.approx(0.80)
        assert calculate_backoff(5, policy) == pytest.approx(1.60)
        assert calculate_backoff(6, policy) == pytest.approx(2.00)  # Capped at max_delay
        assert calculate_backoff(10, policy) == pytest.approx(2.00)

    def test_jitter_produces_values_inside_range(self):
        policy = RetryPolicy(base_delay=0.1, max_delay=1.0, jitter=True)

        for attempt in range(5):
            ceiling = min(1.0, 0.1 * (2.0**attempt))
            for _ in range(20):
                delay = calculate_backoff(attempt, policy)
                assert 0.0 <= delay <= ceiling

    def test_deterministic_random_source_injection(self):
        policy = RetryPolicy(base_delay=0.1, max_delay=2.0, jitter=True)

        # Min RNG: returns lower bound 0.0
        min_rng = lambda a, b: a
        assert calculate_backoff(0, policy, random_func=min_rng) == pytest.approx(0.0)
        assert calculate_backoff(3, policy, random_func=min_rng) == pytest.approx(0.0)

        # Max RNG: returns ceiling
        max_rng = lambda a, b: b
        assert calculate_backoff(0, policy, random_func=max_rng) == pytest.approx(0.1)
        assert calculate_backoff(1, policy, random_func=max_rng) == pytest.approx(0.2)
        assert calculate_backoff(2, policy, random_func=max_rng) == pytest.approx(0.4)

        # Midpoint RNG: returns average of bounds
        mid_rng = lambda a, b: (a + b) / 2.0
        assert calculate_backoff(0, policy, random_func=mid_rng) == pytest.approx(0.05)
        assert calculate_backoff(1, policy, random_func=mid_rng) == pytest.approx(0.10)

    def test_zero_base_or_max_delay(self):
        policy = RetryPolicy(base_delay=0.0, max_delay=0.0)
        assert calculate_backoff(0, policy) == 0.0
        assert calculate_backoff(5, policy) == 0.0

    def test_negative_attempt_raises_value_error(self):
        policy = RetryPolicy()
        with pytest.raises(ValueError, match="Attempt must be non-negative"):
            calculate_backoff(-1, policy)

    def test_large_attempt_does_not_overflow(self):
        policy = RetryPolicy(base_delay=0.05, max_delay=10.0)
        max_rng = lambda a, b: b
        delay = calculate_backoff(10000, policy, random_func=max_rng)
        assert delay == pytest.approx(10.0)
