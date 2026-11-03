"""Metrics abstraction and in-memory collector for PGShield.

This module provides a decoupled metrics protocol and lightweight in-memory
implementation to track transaction counts, retries, concurrency errors,
and latency distributions without requiring external monitoring dependencies.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Final, Optional, Protocol, runtime_checkable

from pgshield.postgres.errors import (
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_SERIALIZATION_FAILURE,
)

# Default maximum number of latency and delay samples retained in memory to bound memory usage
DEFAULT_MAX_SAMPLES: Final[int] = 10_000


@runtime_checkable
class MetricsCollector(Protocol):
    """Protocol for recording PGShield transaction and concurrency metrics.

    Prometheus, OpenTelemetry, StatsD, or custom collectors can implement this
    protocol to receive transaction instrumentation events.
    """

    def record_transaction_started(self, transaction_id: Optional[str] = None) -> None:
        """Record the start of a transaction attempt sequence."""
        ...

    def record_transaction_committed(
        self,
        duration: float,
        attempts: int,
        transaction_id: Optional[str] = None,
    ) -> None:
        """Record a successfully committed transaction."""
        ...

    def record_transaction_failed(
        self,
        duration: float,
        attempts: int,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        """Record a permanently failed/exhausted transaction."""
        ...

    def record_transaction_retry(
        self,
        attempt: int,
        delay: float,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        """Record a retry event and backoff delay."""
        ...

    def record_serialization_failure(self) -> None:
        """Increment the count of serialization failures (SQLSTATE 40001)."""
        ...

    def record_deadlock(self) -> None:
        """Increment the count of deadlocks detected (SQLSTATE 40P01)."""
        ...


class NullMetricsCollector:
    """A zero-overhead, no-op metrics collector used when metrics are disabled."""

    def record_transaction_started(self, transaction_id: Optional[str] = None) -> None:
        pass

    def record_transaction_committed(
        self,
        duration: float,
        attempts: int,
        transaction_id: Optional[str] = None,
    ) -> None:
        pass

    def record_transaction_failed(
        self,
        duration: float,
        attempts: int,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        pass

    def record_transaction_retry(
        self,
        attempt: int,
        delay: float,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        pass

    def record_serialization_failure(self) -> None:
        pass

    def record_deadlock(self) -> None:
        pass


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Immutable snapshot of recorded transaction metrics."""

    transactions_total: int
    transactions_committed_total: int
    transactions_failed_total: int
    transactions_retried_total: int
    serialization_failures_total: int
    deadlocks_total: int
    total_duration_seconds: float
    avg_duration_seconds: float
    max_duration_seconds: float
    total_retry_delay_seconds: float
    avg_retry_delay_seconds: float


class InMemoryMetricsCollector:
    """Thread-safe, lightweight in-memory metrics collector for PGShield.

    Tracks transaction volumes, commitment rates, failure rates, retry frequencies,
    SQLSTATE concurrency errors (40001, 40P01), and timing statistics.
    """

    def __init__(self, max_samples: int = DEFAULT_MAX_SAMPLES) -> None:
        self._lock = threading.Lock()
        self._max_samples = max_samples
        self._transactions_total: int = 0
        self._transactions_committed_total: int = 0
        self._transactions_failed_total: int = 0
        self._transactions_retried_total: int = 0
        self._serialization_failures_total: int = 0
        self._deadlocks_total: int = 0
        self._durations: Deque[float] = deque(maxlen=max_samples)
        self._retry_delays: Deque[float] = deque(maxlen=max_samples)

    @property
    def transactions_total(self) -> int:
        with self._lock:
            return self._transactions_total

    @property
    def transactions_committed_total(self) -> int:
        with self._lock:
            return self._transactions_committed_total

    @property
    def transactions_failed_total(self) -> int:
        with self._lock:
            return self._transactions_failed_total

    @property
    def transactions_retried_total(self) -> int:
        with self._lock:
            return self._transactions_retried_total

    @property
    def serialization_failures_total(self) -> int:
        with self._lock:
            return self._serialization_failures_total

    @property
    def deadlocks_total(self) -> int:
        with self._lock:
            return self._deadlocks_total

    def record_transaction_started(self, transaction_id: Optional[str] = None) -> None:
        with self._lock:
            self._transactions_total += 1

    def record_transaction_committed(
        self,
        duration: float,
        attempts: int,
        transaction_id: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._transactions_committed_total += 1
            self._durations.append(duration)

    def record_transaction_failed(
        self,
        duration: float,
        attempts: int,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._transactions_failed_total += 1
            self._durations.append(duration)
            if sqlstate == SQLSTATE_SERIALIZATION_FAILURE:
                self._serialization_failures_total += 1
            elif sqlstate == SQLSTATE_DEADLOCK_DETECTED:
                self._deadlocks_total += 1

    def record_transaction_retry(
        self,
        attempt: int,
        delay: float,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._transactions_retried_total += 1
            if delay > 0:
                self._retry_delays.append(delay)
            if sqlstate == SQLSTATE_SERIALIZATION_FAILURE:
                self._serialization_failures_total += 1
            elif sqlstate == SQLSTATE_DEADLOCK_DETECTED:
                self._deadlocks_total += 1

    def record_serialization_failure(self) -> None:
        with self._lock:
            self._serialization_failures_total += 1

    def record_deadlock(self) -> None:
        with self._lock:
            self._deadlocks_total += 1

    def snapshot(self) -> MetricsSnapshot:
        """Capture an immutable snapshot of all metrics."""
        with self._lock:
            total_dur = sum(self._durations)
            dur_count = len(self._durations)
            avg_dur = (total_dur / dur_count) if dur_count > 0 else 0.0
            max_dur = max(self._durations) if dur_count > 0 else 0.0

            total_delay = sum(self._retry_delays)
            delay_count = len(self._retry_delays)
            avg_delay = (total_delay / delay_count) if delay_count > 0 else 0.0

            return MetricsSnapshot(
                transactions_total=self._transactions_total,
                transactions_committed_total=self._transactions_committed_total,
                transactions_failed_total=self._transactions_failed_total,
                transactions_retried_total=self._transactions_retried_total,
                serialization_failures_total=self._serialization_failures_total,
                deadlocks_total=self._deadlocks_total,
                total_duration_seconds=total_dur,
                avg_duration_seconds=avg_dur,
                max_duration_seconds=max_dur,
                total_retry_delay_seconds=total_delay,
                avg_retry_delay_seconds=avg_delay,
            )

    def reset(self) -> None:
        """Reset all counters and timings to zero."""
        with self._lock:
            self._transactions_total = 0
            self._transactions_committed_total = 0
            self._transactions_failed_total = 0
            self._transactions_retried_total = 0
            self._serialization_failures_total = 0
            self._deadlocks_total = 0
            self._durations.clear()
            self._retry_delays.clear()
