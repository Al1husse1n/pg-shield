"""Backoff calculation subsystem with jitter for PGShield.

This module is responsible strictly for calculating backoff durations between
transaction retry attempts. It contains no sleep or I/O logic.
"""

from __future__ import annotations

import random
from typing import Callable, Optional

from pgshield.retry.policy import RetryPolicy

# Type alias for a random generator function accepting (min_val, max_val) -> float
RandomFloatGenerator = Callable[[float, float], float]


def calculate_backoff(
    attempt: int,
    policy: RetryPolicy,
    *,
    random_func: Optional[RandomFloatGenerator] = None,
) -> float:
    """Calculate the backoff delay in seconds for a given retry attempt.

    Uses exponential backoff with full jitter:
        ceiling = min(max_delay, base_delay * (backoff_factor ** attempt))
        delay = random.uniform(0.0, ceiling)  # if policy.jitter is True

    Attempt indexing:
        attempt 0: first retry delay (between attempt 1 failure and attempt 2 start)
        attempt 1: second retry delay
        ... and so forth.

    Args:
        attempt: The 0-based index of the retry attempt (0 for first retry).
            Must be non-negative.
        policy: The RetryPolicy configuration.
        random_func: Optional injectable random function taking (a, b) and returning
            a float in [a, b]. Defaults to `random.uniform`. Useful for deterministic tests.

    Returns:
        The calculated delay in seconds (always >= 0.0 and <= policy.max_delay).

    Raises:
        ValueError: If attempt is negative.
    """
    if attempt < 0:
        raise ValueError(f"Attempt must be non-negative (>= 0), got {attempt!r}")

    if policy.base_delay == 0.0 or policy.max_delay == 0.0:
        return 0.0

    # Guard against float overflow for large attempt numbers
    try:
        exponential_multiplier = policy.backoff_factor**attempt
        calculated_ceiling = policy.base_delay * exponential_multiplier
    except OverflowError:
        calculated_ceiling = policy.max_delay

    ceiling = min(policy.max_delay, calculated_ceiling)

    # Ensure ceiling is bounded at base_delay for attempt 0 or max_delay
    ceiling = max(0.0, ceiling)

    if not policy.jitter:
        return ceiling

    rng = random_func if random_func is not None else random.uniform
    delay = rng(0.0, ceiling)

    # Guarantee boundaries regardless of custom rng implementation
    return max(0.0, min(policy.max_delay, delay))
