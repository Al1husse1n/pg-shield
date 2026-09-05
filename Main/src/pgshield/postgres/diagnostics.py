"""Diagnostic reporting and concurrency conflict analysis for PGShield.

This module combines error details, active backend sessions, lock states, and
transaction metadata to generate actionable, machine-readable DiagnosticReports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import logging
import random
from typing import Any, Dict, List, Optional, Sequence, Tuple

_logger = logging.getLogger("pgshield.postgres.diagnostics")

from pgshield.postgres.activity import QueryExecutor, SessionActivity, fetch_activity
from pgshield.postgres.errors import (
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_LOCK_NOT_AVAILABLE,
    SQLSTATE_SERIALIZATION_FAILURE,
    PostgresErrorCategory,
    PostgresErrorDetails,
    parse_postgres_error,
)
from pgshield.postgres.locks import (
    BlockingRelationship,
    LockInfo,
    detect_blocking_chains,
    fetch_locks,
)


class DiagnosticMode(str, Enum):
    """Operational mode controlling when PostgreSQL diagnostic queries are executed."""

    OFF = "off"
    ON_FAILURE = "on_failure"
    SAMPLED = "sampled"
    ALWAYS = "always"


@dataclass(frozen=True, slots=True)
class DiagnosticReport:
    """Structured, machine-readable diagnostic report for a transaction event.

    Attributes:
        transaction_id: Application or transaction identifier if available.
        attempt: Attempt number during which diagnostics were collected.
        sqlstate: 5-character PostgreSQL SQLSTATE code if an error occurred.
        error_category: Categorized error type.
        error_message: Primary error message string.
        error_detail: PostgreSQL error detail string if available.
        error_hint: PostgreSQL error hint string if available.
        duration: Duration of the attempt in seconds.
        isolation_level: Configured transaction isolation level if known.
        pid: PostgreSQL backend PID of the failed transaction if known.
        active_sessions: Snapshot of concurrent sessions from pg_stat_activity.
        locks: Snapshot of locks from pg_locks.
        blocking_chains: Detected blocking relationships among backend processes.
        explanation: Technical explanation of the concurrency conflict.
        warnings: List of diagnostic warnings.
        recommendations: Actionable recommendations for developers/operators.
    """

    transaction_id: Optional[str]
    attempt: int
    sqlstate: Optional[str]
    error_category: PostgresErrorCategory
    error_message: str
    duration: float
    error_detail: Optional[str] = None
    error_hint: Optional[str] = None
    isolation_level: Optional[str] = None
    pid: Optional[int] = None
    active_sessions: List[SessionActivity] = field(default_factory=list)
    locks: List[LockInfo] = field(default_factory=list)
    blocking_chains: List[BlockingRelationship] = field(default_factory=list)
    explanation: str = ""
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    @property
    def has_blocking(self) -> bool:
        """Return True if any active blocking relationships were detected."""
        return len(self.blocking_chains) > 0


def generate_explanation(
    error_details: PostgresErrorDetails,
    blocking_chains: Sequence[BlockingRelationship],
    locks: Sequence[LockInfo],
) -> Tuple[str, List[str], List[str]]:
    """Generate technical explanation, warnings, and recommendations based on error and lock data.

    Returns:
        Tuple of (explanation_text, warnings_list, recommendations_list).
    """
    sqlstate = error_details.sqlstate
    warnings: List[str] = []
    recommendations: List[str] = []

    if sqlstate == SQLSTATE_SERIALIZATION_FAILURE:
        explanation = (
            "Serialization failure detected (SQLSTATE 40001). PostgreSQL's Serializable Snapshot "
            "Isolation (SSI) dependency tracking detected a read/write conflict (rw-antidependency cycle) "
            "between concurrent transactions. SSI conflicts are tracked in memory via predicate locks "
            "(SIREAD locks) and do not manifest as standard blocking locks in pg_locks. The current "
            "lock table may not identify the specific overlapping query."
        )
        warnings.append(
            "pg_locks cannot fully explain SSI serialization failures; do not assume missing blocking "
            "locks indicates absence of conflict."
        )
        recommendations.append("Retry the entire transaction from the beginning with backoff and jitter.")
        recommendations.append("Ensure transaction execution is short and read-write sets are minimal.")
        recommendations.append(
            "Consider declaring pure read transactions as READ ONLY to reduce predicate lock tracking."
        )

    elif sqlstate == SQLSTATE_DEADLOCK_DETECTED:
        explanation = (
            "Deadlock detected (SQLSTATE 40P01). Two or more concurrent transactions were waiting for "
            "mutually conflicting locks held by each other, forming a cycle in the lock wait graph. "
            "PostgreSQL's deadlock detector aborted this transaction to break the cycle."
        )
        if blocking_chains:
            explanation += f" Detected {len(blocking_chains)} blocking relationship(s) in active locks."
        recommendations.append("Retry the transaction from the beginning with randomized jitter.")
        recommendations.append(
            "Ensure concurrent transactions acquire table and row locks in a consistent global order."
        )
        recommendations.append(
            "Keep transactions as brief as possible to reduce the window of lock contention."
        )

    elif sqlstate == SQLSTATE_LOCK_NOT_AVAILABLE:
        explanation = (
            "Lock not available (SQLSTATE 55P03). A lock acquisition request with NOWAIT failed because "
            "another transaction held a conflicting lock."
        )
        recommendations.append("Retry after a backoff delay or verify lock acquisition strategy.")

    elif error_details.category == PostgresErrorCategory.INTEGRITY_VIOLATION:
        explanation = (
            f"Integrity constraint violation ({error_details.sqlstate}). This is a deterministic data "
            f"or business logic error (e.g. unique or foreign key constraint violation) and is not "
            f"a transient concurrency conflict."
        )
        recommendations.append("Do not retry automatically without correcting the underlying data.")

    elif error_details.category == PostgresErrorCategory.STATEMENT_TIMEOUT:
        explanation = (
            f"Statement timeout exceeded (SQLSTATE {error_details.sqlstate}). The query was canceled "
            f"because it exceeded PostgreSQL's statement_timeout setting."
        )
        recommendations.append("Inspect slow queries or adjust statement_timeout if appropriate.")

    elif error_details.message == "No error occurred" or (
        error_details.sqlstate is None
        and error_details.category == PostgresErrorCategory.UNKNOWN
        and not error_details.driver_error_type
    ):
        explanation = "Transaction completed without error."

    else:
        explanation = f"Database error encountered: {error_details.message}"
        if error_details.sqlstate:
            explanation = f"Database error (SQLSTATE {error_details.sqlstate}): {error_details.message}"

    return explanation, warnings, recommendations


def build_diagnostic_report(
    *,
    error: Optional[BaseException] = None,
    attempt: int = 1,
    duration: float = 0.0,
    transaction_id: Optional[str] = None,
    isolation_level: Optional[str] = None,
    pid: Optional[int] = None,
    sessions: Optional[Sequence[SessionActivity]] = None,
    locks: Optional[Sequence[LockInfo]] = None,
) -> DiagnosticReport:
    """Construct a comprehensive DiagnosticReport from error and database diagnostics.

    Args:
        error: The exception encountered, if any.
        attempt: Attempt number.
        duration: Duration of the attempt in seconds.
        transaction_id: Transaction identifier.
        isolation_level: Isolation level.
        pid: Backend PID.
        sessions: Active sessions from pg_stat_activity.
        locks: Active locks from pg_locks.

    Returns:
        Structured DiagnosticReport.
    """
    error_details = (
        parse_postgres_error(error)
        if error is not None
        else PostgresErrorDetails(
            sqlstate=None,
            category=PostgresErrorCategory.UNKNOWN,
            message="No error occurred",
        )
    )

    lock_list = list(locks) if locks is not None else []
    session_list = list(sessions) if sessions is not None else []
    blocking_chains = detect_blocking_chains(lock_list)

    explanation, warnings, recommendations = generate_explanation(
        error_details, blocking_chains, lock_list
    )

    return DiagnosticReport(
        transaction_id=transaction_id,
        attempt=attempt,
        sqlstate=error_details.sqlstate,
        error_category=error_details.category,
        error_message=error_details.message,
        error_detail=error_details.detail,
        error_hint=error_details.hint,
        duration=duration,
        isolation_level=isolation_level,
        pid=pid,
        active_sessions=session_list,
        locks=lock_list,
        blocking_chains=blocking_chains,
        explanation=explanation,
        warnings=warnings,
        recommendations=recommendations,
    )


class DiagnosticCollector:
    """Orchestrates collecting diagnostic reports from PostgreSQL on demand.

    Configurable with DiagnosticMode (off, on_failure, sampled, always) to ensure
    zero runtime overhead during ordinary successful transactions.
    """

    def __init__(
        self,
        mode: DiagnosticMode = DiagnosticMode.OFF,
        *,
        sample_rate: float = 0.05,
        executor: Optional[QueryExecutor] = None,
    ) -> None:
        """Initialize DiagnosticCollector.

        Args:
            mode: DiagnosticMode determining when diagnostics run. Defaults to OFF.
            sample_rate: Sampling probability (0.0 to 1.0) when mode is SAMPLED.
            executor: Optional separate QueryExecutor for running diagnostic queries.
        """
        self.mode = mode
        self.sample_rate = max(0.0, min(1.0, sample_rate))
        self.executor = executor

    def should_collect(self, has_error: bool) -> bool:
        """Determine if diagnostic queries should be executed for a transaction event."""
        if self.mode == DiagnosticMode.OFF:
            return False
        if self.mode == DiagnosticMode.ALWAYS:
            return True
        if self.mode == DiagnosticMode.ON_FAILURE:
            return has_error
        if self.mode == DiagnosticMode.SAMPLED:
            return random.random() < self.sample_rate
        return False

    async def collect(
        self,
        *,
        error: Optional[BaseException] = None,
        attempt: int = 1,
        duration: float = 0.0,
        transaction_id: Optional[str] = None,
        isolation_level: Optional[str] = None,
        pid: Optional[int] = None,
        executor_override: Optional[QueryExecutor] = None,
    ) -> Optional[DiagnosticReport]:
        """Collect and construct a DiagnosticReport if allowed by the active DiagnosticMode.

        Args:
            error: Exception raised during transaction, if any.
            attempt: Current attempt number.
            duration: Attempt execution duration in seconds.
            transaction_id: Transaction identifier.
            isolation_level: Transaction isolation level.
            pid: Backend process ID.
            executor_override: Optional QueryExecutor override (e.g. dedicated diagnostic connection).

        Returns:
            DiagnosticReport if collection was performed, else None.
        """
        has_error = error is not None
        if not self.should_collect(has_error):
            return None

        active_executor = executor_override or self.executor            #incase you want a different connection/executor other than defined initially

        sessions: List[SessionActivity] = []
        locks: List[LockInfo] = []

        if active_executor is not None:
            try:
                sessions = await fetch_activity(active_executor)
            except Exception as exc:
                _logger.debug("Failed to fetch pg_stat_activity during diagnostics: %s", exc)

            try:
                locks = await fetch_locks(active_executor)
            except Exception as exc:
                _logger.debug("Failed to fetch pg_locks during diagnostics: %s", exc)

        return build_diagnostic_report(
            error=error,
            attempt=attempt,
            duration=duration,
            transaction_id=transaction_id,
            isolation_level=isolation_level,
            pid=pid,
            sessions=sessions,
            locks=locks,
        )
