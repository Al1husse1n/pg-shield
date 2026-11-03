"""PostgreSQL-specific utilities, diagnostics, and lock analysis for PGShield.

Provides:
- errors.py: SQLSTATE extraction and error categorization
  (PostgresErrorDetails, PostgresErrorCategory)
- activity.py: pg_stat_activity inspection (SessionActivity, fetch_activity)
- locks.py: pg_locks inspection & blocking analysis (LockInfo, BlockingRelationship, fetch_locks)
- diagnostics.py: Unified DiagnosticReport and DiagnosticCollector
"""

from pgshield.postgres.activity import (
    ACTIVITY_QUERY_BASE,
    ACTIVITY_QUERY_WITH_SNIPPET,
    QueryExecutor,
    SessionActivity,
    build_activity_query,
    fetch_activity,
    parse_activity_rows,
)
from pgshield.postgres.diagnostics import (
    DiagnosticCollector,
    DiagnosticMode,
    DiagnosticReport,
    build_diagnostic_report,
    generate_explanation,
)
from pgshield.postgres.errors import (
    SQLSTATE_CHECK_VIOLATION,
    SQLSTATE_CONNECTION_EXCEPTION,
    SQLSTATE_CONNECTION_FAILURE,
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_FOREIGN_KEY_VIOLATION,
    SQLSTATE_LOCK_NOT_AVAILABLE,
    SQLSTATE_NOT_NULL_VIOLATION,
    SQLSTATE_QUERY_CANCELED,
    SQLSTATE_SERIALIZATION_FAILURE,
    SQLSTATE_UNIQUE_VIOLATION,
    PostgresErrorCategory,
    PostgresErrorDetails,
    categorize_sqlstate,
    parse_postgres_error,
)
from pgshield.postgres.locks import (
    LOCKS_QUERY_BASE,
    AuthoritativeBlocking,
    BlockingRelationship,
    LockInfo,
    build_locks_query,
    detect_blocking_chains,
    fetch_blocking_pids,
    fetch_locks,
    fetch_locks_with_authoritative,
    lock_modes_conflict,
    parse_lock_rows,
)

__all__ = [
    # errors
    "PostgresErrorDetails",
    "PostgresErrorCategory",
    "parse_postgres_error",
    "categorize_sqlstate",
    "SQLSTATE_SERIALIZATION_FAILURE",
    "SQLSTATE_DEADLOCK_DETECTED",
    "SQLSTATE_UNIQUE_VIOLATION",
    "SQLSTATE_FOREIGN_KEY_VIOLATION",
    "SQLSTATE_NOT_NULL_VIOLATION",
    "SQLSTATE_CHECK_VIOLATION",
    "SQLSTATE_LOCK_NOT_AVAILABLE",
    "SQLSTATE_QUERY_CANCELED",
    "SQLSTATE_CONNECTION_EXCEPTION",
    "SQLSTATE_CONNECTION_FAILURE",
    # activity
    "SessionActivity",
    "QueryExecutor",
    "parse_activity_rows",
    "build_activity_query",
    "fetch_activity",
    "ACTIVITY_QUERY_BASE",
    "ACTIVITY_QUERY_WITH_SNIPPET",
    # locks
    "LockInfo",
    "BlockingRelationship",
    "AuthoritativeBlocking",
    "parse_lock_rows",
    "detect_blocking_chains",
    "build_locks_query",
    "fetch_locks",
    "fetch_blocking_pids",
    "fetch_locks_with_authoritative",
    "lock_modes_conflict",
    "LOCKS_QUERY_BASE",
    # diagnostics
    "DiagnosticReport",
    "DiagnosticMode",
    "DiagnosticCollector",
    "build_diagnostic_report",
    "generate_explanation",
]
