"""Retry subsystem for PGShield.

Provides:
- Error classification for PostgreSQL SQLSTATEs (classifier.py)
- Configuration policies for retry execution (policy.py)
- Jittered exponential backoff calculation (backoff.py)
"""

from pgshield.retry.backoff import calculate_backoff
from pgshield.retry.classifier import (
    DEFAULT_RETRYABLE_SQLSTATES,
    SQLSTATE_CHECK_VIOLATION,
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_FOREIGN_KEY_VIOLATION,
    SQLSTATE_NOT_NULL_VIOLATION,
    SQLSTATE_SERIALIZATION_FAILURE,
    SQLSTATE_UNIQUE_VIOLATION,
    ClassificationResult,
    ErrorCategory,
    classify_error,
    extract_sqlstate,
    is_retryable,
)
from pgshield.retry.policy import RetryPolicy

__all__ = [
    "RetryPolicy",
    "calculate_backoff",
    "classify_error",
    "is_retryable",
    "extract_sqlstate",
    "ClassificationResult",
    "ErrorCategory",
    "DEFAULT_RETRYABLE_SQLSTATES",
    "SQLSTATE_SERIALIZATION_FAILURE",
    "SQLSTATE_DEADLOCK_DETECTED",
    "SQLSTATE_UNIQUE_VIOLATION",
    "SQLSTATE_FOREIGN_KEY_VIOLATION",
    "SQLSTATE_CHECK_VIOLATION",
    "SQLSTATE_NOT_NULL_VIOLATION",
]
