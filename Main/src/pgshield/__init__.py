"""PGShield: PostgreSQL concurrency and retry toolkit for Python applications."""

from pgshield.retry import (
    ClassificationResult,
    ErrorCategory,
    RetryPolicy,
    calculate_backoff,
    classify_error,
    extract_sqlstate,
    is_retryable,
)
from pgshield.transaction import (
    AttemptRecord,
    NullTransactionAdapter,
    TransactionAdapter,
    TransactionContext,
    TransactionResult,
    TransactionRunner,
    get_current_context,
    transaction,
    transactional,
)

__version__ = "0.1.0"

__all__ = [
    # Retry subsystem
    "RetryPolicy",
    "calculate_backoff",
    "classify_error",
    "is_retryable",
    "extract_sqlstate",
    "ClassificationResult",
    "ErrorCategory",
    # Transaction subsystem
    "TransactionRunner",
    "TransactionAdapter",
    "NullTransactionAdapter",
    "TransactionResult",
    "TransactionContext",
    "AttemptRecord",
    "get_current_context",
    "transaction",
    "transactional",
]
