"""Retry policy configuration for PGShield.

This module defines the immutable configuration rules governing transaction retries,
including attempt limits, delay bounds, and backoff parameters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Set

from pgshield.retry.classifier import DEFAULT_RETRYABLE_SQLSTATES


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Immutable configuration specifying how transaction retries should behave.

    Attributes:
        max_attempts: Total number of attempts allowed for the transaction (including
            the initial attempt). Must be at least 1.
        base_delay: Initial base delay in seconds before the first retry attempt.
            Must be non-negative.
        max_delay: Maximum delay cap in seconds between retry attempts.
            Must be greater than or equal to `base_delay`.
        backoff_factor: Multiplier for exponential backoff between attempts.
            Must be at least 1.0. Defaults to 2.0.
        jitter: Whether to apply randomized jitter (Full Jitter) to backoff delays.
            Defaults to True.
        retryable_sqlstates: Custom set of 5-character SQLSTATE codes to retry.
            Defaults to 40001 (serialization_failure) and 40P01 (deadlock_detected).

    Example:
        >>> policy = RetryPolicy(
        ...     max_attempts=5,
        ...     base_delay=0.05,
        ...     max_delay=2.0,
        ... )
    """

    max_attempts: int = 5
    base_delay: float = 0.05
    max_delay: float = 2.0
    backoff_factor: float = 2.0
    jitter: bool = True
    retryable_sqlstates: FrozenSet[str] = field(
        default_factory=lambda: frozenset(DEFAULT_RETRYABLE_SQLSTATES)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValueError(
                f"max_attempts must be a positive integer >= 1, got {self.max_attempts!r}"
            )
        if self.base_delay < 0:
            raise ValueError(
                f"base_delay must be non-negative (>= 0.0), got {self.base_delay!r}"
            )
        if self.max_delay < self.base_delay:
            raise ValueError(
                f"max_delay ({self.max_delay}) cannot be less than base_delay ({self.base_delay})"
            )
        if self.backoff_factor < 1.0:
            raise ValueError(
                f"backoff_factor must be >= 1.0, got {self.backoff_factor!r}"
            )
        if not isinstance(self.retryable_sqlstates, frozenset):
            # Normalize to frozen set if passed as a set or list
            object.__setattr__(
                self,
                "retryable_sqlstates",
                frozenset(s.upper() for s in self.retryable_sqlstates),
            )
