"""Error classification subsystem for PostgreSQL transactions.

Extracts SQLSTATE codes from exceptions and decides if they are retryable.
Requires driver-origin evidence to avoid misclassifying user exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import AbstractSet, Callable, Final, FrozenSet, Optional

from pgshield._driver_evidence import (
    _has_postgres_driver_evidence,
)

# Standard PostgreSQL SQLSTATE Codes
SQLSTATE_SERIALIZATION_FAILURE: Final[str] = "40001"
SQLSTATE_DEADLOCK_DETECTED: Final[str] = "40P01"
SQLSTATE_UNIQUE_VIOLATION: Final[str] = "23505"
SQLSTATE_FOREIGN_KEY_VIOLATION: Final[str] = "23503"
SQLSTATE_CHECK_VIOLATION: Final[str] = "23514"
SQLSTATE_NOT_NULL_VIOLATION: Final[str] = "23502"

# Default set of SQLSTATEs considered retryable by PGShield
DEFAULT_RETRYABLE_SQLSTATES: Final[FrozenSet[str]] = frozenset(
    {
        SQLSTATE_SERIALIZATION_FAILURE,
        SQLSTATE_DEADLOCK_DETECTED,
    }
)


class ErrorCategory(str, Enum):
    """Broad categories for classified errors."""

    SERIALIZATION_FAILURE = "serialization_failure"
    DEADLOCK_DETECTED = "deadlock_detected"
    INTEGRITY_VIOLATION = "integrity_violation"
    NON_RETRYABLE = "non_retryable"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Structured result of classifying an exception."""

    retryable: bool
    sqlstate: Optional[str]
    category: ErrorCategory
    reason: str


# Type alias for custom SQLSTATE extractor functions
SQLStateExtractor = Callable[[BaseException], Optional[str]]


def extract_sqlstate(error: BaseException) -> Optional[str]:
    """Extract a 5-character PostgreSQL SQLSTATE from an exception.

    Inspects common driver attributes across asyncpg, psycopg2, psycopg3,
    and wrapped exceptions (like SQLAlchemy's DBAPIError.orig) without importing them.

    Args:
        error: The exception to inspect.

    Returns:
        The 5-character uppercase SQLSTATE string if found, otherwise None.
    """
    if not isinstance(error, BaseException):
        return None

    seen: set[int] = set()
    stack: list[BaseException] = [error]

    while stack:
        err = stack.pop()
        if id(err) in seen:
            continue
        seen.add(id(err))

        # Check direct attributes on the exception only if there is driver evidence
        if _has_postgres_driver_evidence(err):
            for attr in ("sqlstate", "pgcode", "code"):
                val = getattr(err, attr, None)
                if isinstance(val, str) and len(val) == 5:
                    return val.upper()

        # Check wrapped/underlying driver errors (pushed so orig is popped first)
        for attr in ("__context__", "__cause__", "orig"):
            nxt = getattr(err, attr, None)
            if isinstance(nxt, BaseException) and nxt is not err:
                stack.append(nxt)

    return None


def categorize_sqlstate(sqlstate: Optional[str]) -> ErrorCategory:
    """Map a SQLSTATE code to an ErrorCategory.

    Args:
        sqlstate: The 5-character SQLSTATE code.

    Returns:
        The corresponding ErrorCategory.
    """
    if sqlstate is None:
        return ErrorCategory.UNKNOWN

    if sqlstate == SQLSTATE_SERIALIZATION_FAILURE:
        return ErrorCategory.SERIALIZATION_FAILURE
    if sqlstate == SQLSTATE_DEADLOCK_DETECTED:
        return ErrorCategory.DEADLOCK_DETECTED
    if sqlstate.startswith("23"):  # Class 23: Integrity Constraint Violation
        return ErrorCategory.INTEGRITY_VIOLATION

    return ErrorCategory.NON_RETRYABLE


def classify_error(
    error: BaseException,
    *,
    retryable_sqlstates: Optional[AbstractSet[str]] = None,
    custom_extractor: Optional[SQLStateExtractor] = None,
) -> ClassificationResult:
    """Classify an exception to determine if it should trigger a transaction retry.

    Args:
        error: The exception raised during transaction execution.
        retryable_sqlstates: Optional custom set of SQLSTATE codes to treat as retryable.
            Defaults to {40001 (serialization_failure), 40P01 (deadlock_detected)}.
        custom_extractor: Optional callable to extract SQLSTATE from custom exceptions.

    Returns:
        A ClassificationResult containing retry decision, SQLSTATE, category, and reason.
    """
    target_sqlstates = (
        retryable_sqlstates if retryable_sqlstates is not None else DEFAULT_RETRYABLE_SQLSTATES
    )

    sqlstate: Optional[str] = None
    if custom_extractor is not None:
        try:
            sqlstate = custom_extractor(error)
            if sqlstate is not None:
                sqlstate = sqlstate.upper()
        except Exception:
            sqlstate = None

    if sqlstate is None:
        sqlstate = extract_sqlstate(error)

    category = categorize_sqlstate(sqlstate)

    if sqlstate is not None:
        if sqlstate in target_sqlstates:
            return ClassificationResult(
                retryable=True,
                sqlstate=sqlstate,
                category=category,
                reason=f"PostgreSQL SQLSTATE {sqlstate} ({category.value}) is retryable",
            )
        else:
            return ClassificationResult(
                retryable=False,
                sqlstate=sqlstate,
                category=category,
                reason=f"PostgreSQL SQLSTATE {sqlstate} ({category.value}) is not retryable",
            )

    return ClassificationResult(
        retryable=False,
        sqlstate=None,
        category=ErrorCategory.UNKNOWN,
        reason=f"Exception {type(error).__name__} does not carry a recognized PostgreSQL SQLSTATE",
    )


def is_retryable(
    error: BaseException,
    *,
    retryable_sqlstates: Optional[AbstractSet[str]] = None,
    custom_extractor: Optional[SQLStateExtractor] = None,
) -> bool:
    """Convenience boolean check for whether an exception should be retried.

    Args:
        error: The exception raised during transaction execution.
        retryable_sqlstates: Optional custom set of SQLSTATE codes to treat as retryable.
        custom_extractor: Optional callable to extract SQLSTATE from custom exceptions.

    Returns:
        True if the error is retryable, False otherwise.
    """
    return classify_error(
        error,
        retryable_sqlstates=retryable_sqlstates,
        custom_extractor=custom_extractor,
    ).retryable
