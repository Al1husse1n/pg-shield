"""Transaction context and metadata tracking for PGShield.

IMPORTANT ARCHITECTURAL NOTE:
Standard Python context managers (PEP 343 / PEP 492) execute their enclosed code
block exactly once. A context manager's `__aexit__` method cannot re-run the body
of the `async with` block upon catching a transient database error.

Therefore, PGShield does NOT provide a fake "retrying context manager" that claims
to replay statements inside an `async with` block.

Instead, automatic whole-transaction retries are canonically orchestrated by:
1. `TransactionRunner.run(operation, ...)`
2. The `@transaction` / `@transactional` decorator

This module provides:
- `TransactionContext`: Live execution metadata available during transaction attempts.
- `AttemptRecord`: Immutable historical record of a single transaction attempt.
- `get_current_context()`: ContextVar-based accessor for the active transaction state.
"""

from __future__ import annotations

import contextvars
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pgshield.retry.classifier import ClassificationResult


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """Historical snapshot of a single transaction attempt.

    Attributes:
        attempt_number: 1-based index of the attempt (1 for initial, 2 for first retry, etc.).
        start_time: Monotonic start timestamp in seconds.
        duration: Duration of the attempt in seconds.
        error: The exception raised during execution or commit, if any.
        classification: The ClassificationResult if an exception occurred, else None.
        backoff_delay: The delay in seconds applied after this attempt (0.0 if not retried).
    """

    attempt_number: int
    start_time: float
    duration: float
    error: Optional[BaseException] = None
    classification: Optional[ClassificationResult] = None
    backoff_delay: float = 0.0

    @property
    def is_success(self) -> bool:
        """Return True if this attempt succeeded without error."""
        return self.error is None

    @property
    def was_retried(self) -> bool:
        """Return True if this attempt encountered a retryable error and backoff delay."""
        return self.backoff_delay > 0.0 or (
            self.classification is not None and self.classification.retryable
        )

    @property
    def sqlstate(self) -> Optional[str]:
        """Convenience accessor for the SQLSTATE code if an error was classified."""
        return self.classification.sqlstate if self.classification else None


@dataclass(slots=True)
class TransactionContext:
    """Runtime context for an active transaction execution managed by TransactionRunner.

    Available inside transactional functions via `get_current_context()`.

    Attributes:
        attempt: Current 1-based attempt index (1 = initial attempt, 2 = 1st retry).
        max_attempts: Total allowed attempts configured for this transaction.
        start_time: Monotonic start timestamp of the entire transaction operation.
        history: List of completed AttemptRecords.
        metadata: Arbitrary user-defined key-value metadata associated with the transaction.
    """

    attempt: int = 1
    max_attempts: int = 1
    start_time: float = field(default_factory=time.monotonic)
    history: List[AttemptRecord] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_retry(self) -> bool:
        """Return True if the current attempt is a retry (i.e. attempt > 1)."""
        return self.attempt > 1

    @property
    def retry_count(self) -> int:
        """Return the number of retries performed so far (attempt - 1)."""
        return max(0, self.attempt - 1)

    @property
    def last_error(self) -> Optional[BaseException]:
        """Return the exception from the most recent failed attempt, if any."""
        if self.history and not self.history[-1].is_success:
            return self.history[-1].error
        return None

    @property
    def total_elapsed_time(self) -> float:
        """Return total elapsed time in seconds since the initial attempt began."""
        return time.monotonic() - self.start_time


# ContextVar holding the active TransactionContext for the current asyncio Task
_CURRENT_TRANSACTION_CONTEXT: contextvars.ContextVar[Optional[TransactionContext]] = (
    contextvars.ContextVar("pgshield_transaction_context", default=None)
)


def get_current_context() -> Optional[TransactionContext]:
    """Retrieve the active TransactionContext for the current async execution context.

    Returns:
        The current TransactionContext instance if executing inside a TransactionRunner
        or @transaction-decorated function, otherwise None.
    """
    return _CURRENT_TRANSACTION_CONTEXT.get()


def _set_current_context(
    ctx: Optional[TransactionContext],
) -> contextvars.Token[Optional[TransactionContext]]:
    """Internal helper to set the active TransactionContext in the ContextVar."""
    return _CURRENT_TRANSACTION_CONTEXT.set(ctx)


def _reset_current_context(token: contextvars.Token[Optional[TransactionContext]]) -> None:
    """Internal helper to restore the previous ContextVar state."""
    _CURRENT_TRANSACTION_CONTEXT.reset(token)
