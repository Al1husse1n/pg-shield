"""Structured logging for PGShield transaction events.

This module provides focused, structured logging for transaction lifecycle events
(start, commit, rollback, retry, exhaustion/failure) without leaking sensitive
credentials or payload data.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Final, Mapping, Optional

# Standard Logger name
DEFAULT_LOGGER_NAME: Final[str] = "pgshield.transaction"

# Sensitive keyword patterns to sanitize in metadata and logs
SENSITIVE_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(password|secret|token|api_key|authorization|auth|credential|dsn|private_key)",
    re.IGNORECASE,
)
REDACTED_VALUE: Final[str] = "[REDACTED]"


class TransactionEventType(str, Enum):
    """Lifecycle event types emitted by PGShield."""

    TRANSACTION_STARTED = "transaction_started"
    TRANSACTION_COMMITTED = "transaction_committed"
    TRANSACTION_ROLLED_BACK = "transaction_rolled_back"
    TRANSACTION_RETRY = "transaction_retry"
    TRANSACTION_FAILED = "transaction_failed"


def sanitize_value(val: Any, _visited: Optional[set[int]] = None) -> Any:
    """Recursively redact sensitive key-value pairs or connection strings."""
    if isinstance(val, Mapping):
        if _visited is None:
            _visited = set()
        val_id = id(val)
        if val_id in _visited:
            return "<cyclic-reference>"
        _visited.add(val_id)

        sanitized: Dict[str, Any] = {}
        for k, v in val.items():
            if isinstance(k, str) and SENSITIVE_KEY_PATTERN.search(k):
                sanitized[k] = REDACTED_VALUE
            else:
                sanitized[k] = sanitize_value(v, _visited)
        return sanitized
    elif isinstance(val, (list, tuple)):
        if _visited is None:
            _visited = set()
        val_id = id(val)
        if val_id in _visited:
            return ["<cyclic-reference>"]
        _visited.add(val_id)

        return [sanitize_value(item, _visited) for item in val]
    elif isinstance(val, str):
        # Basic credential masking in URLs (e.g. postgresql://user:pass@host/db)
        if "://" in val and "@" in val:
            return re.sub(r"://([^:]+):([^@]+)@", r"://\1:[REDACTED]@", val)
        return val
    return val


@dataclass(frozen=True, slots=True)
class LoggedTransactionEvent:
    """Structured representation of a logged transaction event.

    Attributes:
        event_type: The TransactionEventType.
        transaction_id: Optional identifier for the transaction.
        attempt: Attempt number associated with the event.
        duration: Duration of the attempt or total transaction in seconds.
        sqlstate: PostgreSQL SQLSTATE code if an error occurred.
        error_category: Categorization of the error if applicable.
        retry_delay: Backoff delay applied before next attempt if retrying.
        error_message: Error message if an error occurred.
        metadata: Sanitized application metadata dictionary.
    """

    event_type: TransactionEventType
    transaction_id: Optional[str] = None
    attempt: int = 1
    duration: float = 0.0
    sqlstate: Optional[str] = None
    error_category: Optional[str] = None
    retry_delay: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to a dictionary suitable for structured logging / JSON."""
        data: Dict[str, Any] = {
            "event": self.event_type.value,
            "attempt": self.attempt,
            "duration_ms": round(self.duration * 1000.0, 2),
        }
        if self.transaction_id is not None:
            data["transaction_id"] = self.transaction_id
        if self.sqlstate is not None:
            data["sqlstate"] = self.sqlstate
        if self.error_category is not None:
            data["error_category"] = self.error_category
        if self.retry_delay > 0.0:
            data["retry_delay_ms"] = round(self.retry_delay * 1000.0, 2)
        if self.error_message is not None:
            data["error_message"] = self.error_message
        if self.metadata:
            data["metadata"] = self.metadata
        return data


class TransactionLogger:
    """Logger wrapper providing structured logging for PGShield transactions."""

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        enabled: bool = True,
    ) -> None:
        """Initialize TransactionLogger.

        Args:
            logger: Standard logging.Logger instance to emit logs to.
                Defaults to logging.getLogger("pgshield.transaction").
            enabled: Master switch to enable or silence transaction logging.
        """
        self._logger = logger if logger is not None else logging.getLogger(DEFAULT_LOGGER_NAME)
        self.enabled = enabled

    @property
    def logger(self) -> logging.Logger:
        """Return the underlying logging.Logger instance."""
        return self._logger

    def log_started(
        self,
        *,
        attempt: int = 1,
        transaction_id: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LoggedTransactionEvent:
        """Log transaction_started event."""
        sanitized_meta = sanitize_value(dict(metadata)) if metadata else {}
        event = LoggedTransactionEvent(
            event_type=TransactionEventType.TRANSACTION_STARTED,
            transaction_id=transaction_id,
            attempt=attempt,
            metadata=sanitized_meta,
        )
        if self.enabled:
            tx_tag = f" [{transaction_id}]" if transaction_id else ""
            self._logger.debug(
                "Transaction started%s (attempt %d)",
                tx_tag,
                attempt,
                extra={"pgshield": event.to_dict()},
            )
        return event

    def log_committed(
        self,
        *,
        attempt: int = 1,
        duration: float = 0.0,
        transaction_id: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LoggedTransactionEvent:
        """Log transaction_committed event."""
        sanitized_meta = sanitize_value(dict(metadata)) if metadata else {}
        event = LoggedTransactionEvent(
            event_type=TransactionEventType.TRANSACTION_COMMITTED,
            transaction_id=transaction_id,
            attempt=attempt,
            duration=duration,
            metadata=sanitized_meta,
        )
        if self.enabled:
            tx_tag = f" [{transaction_id}]" if transaction_id else ""
            self._logger.info(
                "Transaction committed%s in %.2fms (attempt %d)",
                tx_tag,
                duration * 1000.0,
                attempt,
                extra={"pgshield": event.to_dict()},
            )
        return event

    def log_rolled_back(
        self,
        *,
        attempt: int = 1,
        duration: float = 0.0,
        transaction_id: Optional[str] = None,
        sqlstate: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LoggedTransactionEvent:
        """Log transaction_rolled_back event."""
        sanitized_meta = sanitize_value(dict(metadata)) if metadata else {}
        event = LoggedTransactionEvent(
            event_type=TransactionEventType.TRANSACTION_ROLLED_BACK,
            transaction_id=transaction_id,
            attempt=attempt,
            duration=duration,
            sqlstate=sqlstate,
            error_message=error_message,
            metadata=sanitized_meta,
        )
        if self.enabled:
            tx_tag = f" [{transaction_id}]" if transaction_id else ""
            code_tag = f" (SQLSTATE {sqlstate})" if sqlstate else ""
            self._logger.warning(
                "Transaction rolled back%s%s (attempt %d, duration %.2fms)",
                tx_tag,
                code_tag,
                attempt,
                duration * 1000.0,
                extra={"pgshield": event.to_dict()},
            )
        return event

    def log_retry(
        self,
        *,
        attempt: int,
        retry_delay: float,
        duration: float = 0.0,
        transaction_id: Optional[str] = None,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LoggedTransactionEvent:
        """Log transaction_retry event."""
        sanitized_meta = sanitize_value(dict(metadata)) if metadata else {}
        event = LoggedTransactionEvent(
            event_type=TransactionEventType.TRANSACTION_RETRY,
            transaction_id=transaction_id,
            attempt=attempt,
            duration=duration,
            sqlstate=sqlstate,
            error_category=error_category,
            retry_delay=retry_delay,
            error_message=error_message,
            metadata=sanitized_meta,
        )
        if self.enabled:
            tx_tag = f" [{transaction_id}]" if transaction_id else ""
            code_tag = f" (SQLSTATE {sqlstate})" if sqlstate else ""
            cat_tag = f" [{error_category}]" if error_category else ""
            self._logger.warning(
                "Retrying transaction%s%s%s (attempt %d failed in %.2fms; backoff delay %.2fms)",
                tx_tag,
                cat_tag,
                code_tag,
                attempt,
                duration * 1000.0,
                retry_delay * 1000.0,
                extra={"pgshield": event.to_dict()},
            )
        return event

    def log_failed(
        self,
        *,
        attempt: int,
        duration: float = 0.0,
        transaction_id: Optional[str] = None,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LoggedTransactionEvent:
        """Log transaction_failed event."""
        sanitized_meta = sanitize_value(dict(metadata)) if metadata else {}
        event = LoggedTransactionEvent(
            event_type=TransactionEventType.TRANSACTION_FAILED,
            transaction_id=transaction_id,
            attempt=attempt,
            duration=duration,
            sqlstate=sqlstate,
            error_category=error_category,
            error_message=error_message,
            metadata=sanitized_meta,
        )
        if self.enabled:
            tx_tag = f" [{transaction_id}]" if transaction_id else ""
            code_tag = f" (SQLSTATE {sqlstate})" if sqlstate else ""
            self._logger.error(
                "Transaction failed permanently%s%s after %d attempt(s) (total %.2fms): %s",
                tx_tag,
                code_tag,
                attempt,
                duration * 1000.0,
                error_message or "Unknown error",
                extra={"pgshield": event.to_dict()},
            )
        return event
