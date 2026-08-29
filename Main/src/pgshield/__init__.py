"""PGShield: PostgreSQL concurrency and retry toolkit for Python applications."""

from pgshield.instrumentation import (
    InMemoryMetricsCollector,
    LoggedTransactionEvent,
    MetricsCollector,
    MetricsSnapshot,
    NullMetricsCollector,
    TransactionEventType,
    TransactionLogger,
    sanitize_value,
)
from pgshield.integrations import (
    AsyncpgConnectionAdapter,
    AsyncpgTransactionRunner,
    SQLAlchemySessionAdapter,
    SQLAlchemyTransactionRunner,
    extract_asyncpg_sqlstate,
    extract_sqlalchemy_sqlstate,
    run_asyncpg_transaction,
    run_sqlalchemy_transaction,
)
from pgshield.postgres import (
    DiagnosticCollector,
    DiagnosticMode,
    DiagnosticReport,
    LockInfo,
    PostgresErrorCategory,
    PostgresErrorDetails,
    SessionActivity,
    build_diagnostic_report,
    parse_postgres_error,
)
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
    # PostgreSQL subsystem
    "PostgresErrorDetails",
    "PostgresErrorCategory",
    "parse_postgres_error",
    "SessionActivity",
    "LockInfo",
    "DiagnosticReport",
    "DiagnosticMode",
    "DiagnosticCollector",
    "build_diagnostic_report",
    # Instrumentation subsystem
    "TransactionLogger",
    "LoggedTransactionEvent",
    "TransactionEventType",
    "sanitize_value",
    "MetricsCollector",
    "InMemoryMetricsCollector",
    "NullMetricsCollector",
    "MetricsSnapshot",
    # Integrations
    "SQLAlchemySessionAdapter",
    "SQLAlchemyTransactionRunner",
    "run_sqlalchemy_transaction",
    "extract_sqlalchemy_sqlstate",
    "AsyncpgConnectionAdapter",
    "AsyncpgTransactionRunner",
    "run_asyncpg_transaction",
    "extract_asyncpg_sqlstate",
]
