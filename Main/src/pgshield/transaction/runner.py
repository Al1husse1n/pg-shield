"""Transaction runner implementation for PGShield.

This module provides the central TransactionRunner orchestrating whole-transaction
retries with rollback, error classification, and jittered exponential backoff.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from pgshield.retry.backoff import RandomFloatGenerator, calculate_backoff
from pgshield.retry.classifier import ClassificationResult, classify_error
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.context import (
    AttemptRecord,
    TransactionContext,
    _reset_current_context,
    _set_current_context,
)

T = TypeVar("T")
SleepCallable = Callable[[float], Awaitable[None]]


@runtime_checkable
class TransactionAdapter(Protocol):
    """Protocol defining the transactional interface required by TransactionRunner.

    Integrations for SQLAlchemy, asyncpg, or custom session managers implement this
    protocol to control transaction boundaries.
    """

    async def begin(self) -> None:
        """Begin a database transaction boundary."""
        ...

    async def commit(self) -> None:
        """Commit the current database transaction boundary."""
        ...

    async def rollback(self) -> None:
        """Roll back the current database transaction boundary."""
        ...


class NullTransactionAdapter:
    """A no-op transaction adapter used when transaction boundaries are managed internally."""

    async def begin(self) -> None:
        """No-op begin."""
        pass

    async def commit(self) -> None:
        """No-op commit."""
        pass

    async def rollback(self) -> None:
        """No-op rollback."""
        pass


@dataclass(frozen=True, slots=True)
class TransactionResult(Generic[T]):
    """Result of a transaction execution including value and attempt history.

    Attributes:
        value: The value returned by the transactional operation.
        total_attempts: Total number of attempts executed.
        total_duration: Total execution time in seconds across all attempts.
        history: Sequence of AttemptRecords describing each attempt.
    """

    value: T
    total_attempts: int
    total_duration: float
    history: List[AttemptRecord]

    @property
    def retried(self) -> bool:
        """Return True if the transaction required at least one retry."""
        return self.total_attempts > 1

    @property
    def retry_count(self) -> int:
        """Return the count of retries performed."""
        return max(0, self.total_attempts - 1)


class TransactionRunner:
    """Orchestrates reliable whole-transaction execution and retries.

    Guarantees that:
    1. The entire user operation is executed from the beginning on every attempt.
    2. A database transaction is begun before each attempt.
    3. Successful operations are committed.
    4. Exceptions during execution or commit trigger an immediate rollback.
    5. Exceptions are classified via PostgreSQL SQLSTATE codes.
    6. Only retryable exceptions (40001, 40P01, or custom) trigger a backoff delay and retry.
    7. Non-retryable exceptions or exhausted attempts immediately propagate.
    8. Backoff delay occurs strictly AFTER rollback and BEFORE the next attempt.
    """

    def __init__(
        self,
        policy: Optional[RetryPolicy] = None,
        adapter: Optional[TransactionAdapter] = None,
        *,
        sleep_func: Optional[SleepCallable] = None,
        random_func: Optional[RandomFloatGenerator] = None,
    ) -> None:
        """Initialize TransactionRunner.

        Args:
            policy: RetryPolicy governing attempt limits, delays, and backoff.
                Defaults to standard RetryPolicy().
            adapter: Optional default TransactionAdapter controlling transaction boundaries.
            sleep_func: Async sleep callable for backoff delays (defaults to asyncio.sleep).
            random_func: Injectable random generator for deterministic jitter testing.
        """
        self._policy = policy if policy is not None else RetryPolicy()
        self._default_adapter = adapter
        self._sleep_func = sleep_func if sleep_func is not None else asyncio.sleep
        self._random_func = random_func

    @property
    def policy(self) -> RetryPolicy:
        """Return the configured RetryPolicy."""
        return self._policy

    @property
    def default_adapter(self) -> Optional[TransactionAdapter]:
        """Return the default TransactionAdapter if configured."""
        return self._default_adapter

    async def run(
        self,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        adapter: Optional[TransactionAdapter] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> T:
        """Execute a transactional operation with automatic whole-transaction retries.

        Args:
            operation: An async callable representing the entire transaction.
            *args: Positional arguments passed to the operation on every attempt.
            adapter: Optional override for the TransactionAdapter for this run.
            metadata: Optional user metadata attached to the TransactionContext.
            **kwargs: Keyword arguments passed to the operation on every attempt.

        Returns:
            The return value of the operation upon successful execution and commit.

        Raises:
            The final exception raised if non-retryable or if retry attempts are exhausted.
        """
        result = await self.run_with_result(
            operation,
            *args,
            adapter=adapter,
            metadata=metadata,
            **kwargs,
        )
        return result.value

    async def run_with_result(
        self,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        adapter: Optional[TransactionAdapter] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> TransactionResult[T]:
        """Execute a transactional operation and return a TransactionResult with metadata.

        Args:
            operation: An async callable representing the entire transaction.
            *args: Positional arguments passed to the operation on every attempt.
            adapter: Optional override for the TransactionAdapter for this run.
            metadata: Optional user metadata attached to the TransactionContext.
            **kwargs: Keyword arguments passed to the operation on every attempt.

        Returns:
            A TransactionResult containing the returned value and full attempt history.
        """
        active_adapter = adapter if adapter is not None else self._default_adapter
        overall_start_time = time.monotonic()

        ctx = TransactionContext(
            attempt=1,
            max_attempts=self._policy.max_attempts,
            start_time=overall_start_time,
            metadata=dict(metadata) if metadata is not None else {},
        )
        token = _set_current_context(ctx)

        try:
            for attempt_idx in range(self._policy.max_attempts):
                ctx.attempt = attempt_idx + 1
                attempt_start = time.monotonic()

                # 1. Begin transaction boundary if an adapter is active
                if active_adapter is not None:
                    await active_adapter.begin()

                try:
                    # 2. Execute the entire transactional operation from the beginning
                    result = await operation(*args, **kwargs)

                    # 3. Commit transaction upon successful operation return
                    if active_adapter is not None:
                        await active_adapter.commit()

                    attempt_duration = time.monotonic() - attempt_start
                    ctx.history.append(
                        AttemptRecord(
                            attempt_number=ctx.attempt,
                            start_time=attempt_start,
                            duration=attempt_duration,
                            error=None,
                            classification=None,
                            backoff_delay=0.0,
                        )
                    )

                    total_duration = time.monotonic() - overall_start_time
                    return TransactionResult(
                        value=result,
                        total_attempts=ctx.attempt,
                        total_duration=total_duration,
                        history=list(ctx.history),
                    )

                except BaseException as exc:
                    # Respect cancellation and process exits immediately
                    if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                        if active_adapter is not None:
                            try:
                                await active_adapter.rollback()
                            except Exception:
                                pass
                        raise

                    attempt_duration = time.monotonic() - attempt_start

                    # 4a. Roll back immediately on any transaction failure
                    if active_adapter is not None:
                        try:
                            await active_adapter.rollback()
                        except Exception:
                            # Rollback errors do not mask the primary operational failure
                            pass

                    # 4b. Classify the exception against PostgreSQL SQLSTATE rules
                    classification = classify_error(
                        exc,
                        retryable_sqlstates=self._policy.retryable_sqlstates,
                    )

                    is_last_attempt = (attempt_idx + 1) >= self._policy.max_attempts

                    # 4c. Non-retryable error OR reached max attempts: fail immediately
                    if not classification.retryable or is_last_attempt:
                        ctx.history.append(
                            AttemptRecord(
                                attempt_number=ctx.attempt,
                                start_time=attempt_start,
                                duration=attempt_duration,
                                error=exc,
                                classification=classification,
                                backoff_delay=0.0,
                            )
                        )
                        raise exc

                    # 4d. Calculate jittered exponential backoff for next attempt
                    delay = calculate_backoff(
                        attempt_idx,
                        self._policy,
                        random_func=self._random_func,
                    )
                    ctx.history.append(
                        AttemptRecord(
                            attempt_number=ctx.attempt,
                            start_time=attempt_start,
                            duration=attempt_duration,
                            error=exc,
                            classification=classification,
                            backoff_delay=delay,
                        )
                    )

                    # 4e. Sleep AFTER rollback and BEFORE starting the next attempt
                    if delay > 0.0:
                        await self._sleep_func(delay)

            # Defensive fallback if loop completes unexpectedly
            raise RuntimeError("Transaction attempts exhausted without resolving.")  # pragma: no cover
        finally:
            _reset_current_context(token)
