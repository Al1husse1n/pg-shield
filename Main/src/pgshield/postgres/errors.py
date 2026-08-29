"""PostgreSQL error introspection and normalization utilities for PGShield.

This module is responsible for extracting structured PostgreSQL error details
(SQLSTATE, category, detail, hint, schema/table/constraint context) across various
database drivers (asyncpg, psycopg2, psycopg3, SQLAlchemy) without coupling to them.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Optional

# Key PostgreSQL SQLSTATE Codes
SQLSTATE_SERIALIZATION_FAILURE: Final[str] = "40001"
SQLSTATE_DEADLOCK_DETECTED: Final[str] = "40P01"
SQLSTATE_UNIQUE_VIOLATION: Final[str] = "23505"
SQLSTATE_FOREIGN_KEY_VIOLATION: Final[str] = "23503"
SQLSTATE_NOT_NULL_VIOLATION: Final[str] = "23502"
SQLSTATE_CHECK_VIOLATION: Final[str] = "23514"
SQLSTATE_LOCK_NOT_AVAILABLE: Final[str] = "55P03"
SQLSTATE_QUERY_CANCELED: Final[str] = "57014"
SQLSTATE_ADMIN_SHUTDOWN: Final[str] = "57P01"
SQLSTATE_CRASH_SHUTDOWN: Final[str] = "57P02"
SQLSTATE_CANNOT_CONNECT_NOW: Final[str] = "57P03"
SQLSTATE_CONNECTION_EXCEPTION: Final[str] = "08000"
SQLSTATE_CONNECTION_FAILURE: Final[str] = "08006"


class PostgresErrorCategory(str, Enum):
    """Categorized PostgreSQL error types for diagnostics and reporting."""

    SERIALIZATION_FAILURE = "serialization_failure"
    DEADLOCK_DETECTED = "deadlock_detected"
    LOCK_NOT_AVAILABLE = "lock_not_available"
    INTEGRITY_VIOLATION = "integrity_violation"
    STATEMENT_TIMEOUT = "statement_timeout"
    CONNECTION_ERROR = "connection_error"
    QUERY_CANCELED = "query_canceled"
    SYNTAX_OR_ACCESS_ERROR = "syntax_or_access_error"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class PostgresErrorDetails:
    """Normalized, driver-agnostic details extracted from a PostgreSQL exception.

    Attributes:
        sqlstate: The 5-character PostgreSQL SQLSTATE code (e.g. "40001").
        category: Broad categorization of the error.
        message: The primary error message.
        detail: Optional auxiliary error detail provided by PostgreSQL.
        hint: Optional guidance/hint provided by PostgreSQL.
        table_name: Associated table name if constraint/relation error.
        column_name: Associated column name if applicable.
        constraint_name: Associated constraint name if applicable.
        driver_error_type: Name of the underlying exception class.
    """

    sqlstate: Optional[str]
    category: PostgresErrorCategory
    message: str
    detail: Optional[str] = None
    hint: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    constraint_name: Optional[str] = None
    driver_error_type: Optional[str] = None

    @property
    def is_serialization_failure(self) -> bool:
        """Return True if this error represents a 40001 serialization failure."""
        return self.sqlstate == SQLSTATE_SERIALIZATION_FAILURE

    @property
    def is_deadlock(self) -> bool:
        """Return True if this error represents a 40P01 deadlock."""
        return self.sqlstate == SQLSTATE_DEADLOCK_DETECTED

    @property
    def is_concurrency_conflict(self) -> bool:
        """Return True if this error represents a transaction concurrency conflict (40001 or 40P01)."""
        return self.is_serialization_failure or self.is_deadlock


def categorize_sqlstate(sqlstate: Optional[str]) -> PostgresErrorCategory:
    """Determine the PostgresErrorCategory from a 5-character SQLSTATE code."""
    if sqlstate is None:
        return PostgresErrorCategory.UNKNOWN

    code = sqlstate.upper()
    if code == SQLSTATE_SERIALIZATION_FAILURE:
        return PostgresErrorCategory.SERIALIZATION_FAILURE
    if code == SQLSTATE_DEADLOCK_DETECTED:
        return PostgresErrorCategory.DEADLOCK_DETECTED
    if code == SQLSTATE_LOCK_NOT_AVAILABLE:
        return PostgresErrorCategory.LOCK_NOT_AVAILABLE
    if code == SQLSTATE_QUERY_CANCELED:
        return PostgresErrorCategory.QUERY_CANCELED
    if code.startswith("23"):  # Class 23: Integrity Constraint Violation
        return PostgresErrorCategory.INTEGRITY_VIOLATION
    if code.startswith("08"):  # Class 08: Connection Exception
        return PostgresErrorCategory.CONNECTION_ERROR
    if code.startswith("42"):  # Class 42: Syntax Error or Access Rule Violation
        return PostgresErrorCategory.SYNTAX_OR_ACCESS_ERROR
    if code.startswith("57"):  # Class 57: Operator Intervention
        return PostgresErrorCategory.QUERY_CANCELED

    return PostgresErrorCategory.UNKNOWN


def _extract_attribute(target: Any, *attr_names: str) -> Optional[Any]:
    """Helper to safely extract the first matching attribute from an object."""
    for name in attr_names:
        val = getattr(target, name, None)
        if val is not None:
            return val
    return None


def parse_postgres_error(error: BaseException) -> PostgresErrorDetails:
    """Parse and normalize an arbitrary exception into structured PostgresErrorDetails.

    Safely inspects exception attributes across asyncpg, psycopg2, psycopg3,
    and SQLAlchemy wrapped exceptions without importing those libraries.

    Args:
        error: The exception to inspect.

    Returns:
        A PostgresErrorDetails instance with extracted error metadata.
    """
    if not isinstance(error, BaseException):
        return PostgresErrorDetails(
            sqlstate=None,
            category=PostgresErrorCategory.UNKNOWN,
            message=str(error),
        )

    # Locate underlying driver exception if wrapped (e.g. SQLAlchemy DBAPIError.orig)
    underlying: BaseException = error
    orig = getattr(error, "orig", None)
    if isinstance(orig, BaseException):
        underlying = orig
    elif isinstance(getattr(error, "__cause__", None), BaseException):
        underlying = getattr(error, "__cause__")

    # Extract SQLSTATE
    raw_sqlstate = _extract_attribute(underlying, "sqlstate", "pgcode", "code")
    sqlstate: Optional[str] = None
    if isinstance(raw_sqlstate, str) and len(raw_sqlstate) == 5:
        sqlstate = raw_sqlstate.upper()

    # Extract diagnostic fields
    detail = _extract_attribute(underlying, "detail", "pgerror_detail")
    if detail is not None:
        detail = str(detail)

    hint = _extract_attribute(underlying, "hint", "pgerror_hint")
    if hint is not None:
        hint = str(hint)

    table_name = _extract_attribute(underlying, "table_name", "table")
    if table_name is not None:
        table_name = str(table_name)

    column_name = _extract_attribute(underlying, "column_name", "column")
    if column_name is not None:
        column_name = str(column_name)

    constraint_name = _extract_attribute(underlying, "constraint_name", "constraint")
    if constraint_name is not None:
        constraint_name = str(constraint_name)

    category = categorize_sqlstate(sqlstate)

    return PostgresErrorDetails(
        sqlstate=sqlstate,
        category=category,
        message=str(error),
        detail=detail,
        hint=hint,
        table_name=table_name,
        column_name=column_name,
        constraint_name=constraint_name,
        driver_error_type=type(underlying).__name__,
    )
