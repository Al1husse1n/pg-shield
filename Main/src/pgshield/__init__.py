"""PGShield: PostgreSQL concurrency and retry toolkit for Python applications."""

from pgshield.retry import (
    ClassificationResult,
    ErrorCategory,
    RetryPolicy,
    calculate_backoff,
    classify_error,
    is_retryable,
)

__version__ = "0.1.0"

__all__ = [
    "RetryPolicy",
    "calculate_backoff",
    "classify_error",
    "is_retryable",
    "ClassificationResult",
    "ErrorCategory",
]
