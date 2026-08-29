"""Observability and instrumentation layer for PGShield.

Provides:
- logging.py: Structured transaction event logging and credential sanitization
  (TransactionLogger, LoggedTransactionEvent, TransactionEventType, sanitize_value)
- metrics.py: Metrics collector protocol and in-memory metrics implementation
  (MetricsCollector, InMemoryMetricsCollector, NullMetricsCollector, MetricsSnapshot)
"""

from pgshield.instrumentation.logging import (
    DEFAULT_LOGGER_NAME,
    LoggedTransactionEvent,
    TransactionEventType,
    TransactionLogger,
    sanitize_value,
)
from pgshield.instrumentation.metrics import (
    InMemoryMetricsCollector,
    MetricsCollector,
    MetricsSnapshot,
    NullMetricsCollector,
)

__all__ = [
    # logging
    "TransactionLogger",
    "LoggedTransactionEvent",
    "TransactionEventType",
    "sanitize_value",
    "DEFAULT_LOGGER_NAME",
    # metrics
    "MetricsCollector",
    "InMemoryMetricsCollector",
    "NullMetricsCollector",
    "MetricsSnapshot",
]
