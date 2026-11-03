"""Unit tests for structured transaction logging and credential sanitization."""

import logging
from typing import Any

from pgshield.instrumentation.logging import (
    TransactionEventType,
    TransactionLogger,
    sanitize_value,
)


class TestLoggingSanitization:
    """Test credential sanitization and redaction."""

    def test_sanitize_sensitive_dictionary_keys(self):
        data = {
            "user_id": 42,
            "password": "super_secret_password",
            "api_key": "sk-1234567890",
            "auth_token": "bearer xyz",
            "secret_key": "private_val",
            "metadata": {
                "nested_token": "token_abc",
                "normal_field": "allowed",
            },
        }

        sanitized = sanitize_value(data)
        assert sanitized["user_id"] == 42
        assert sanitized["password"] == "[REDACTED]"
        assert sanitized["api_key"] == "[REDACTED]"
        assert sanitized["auth_token"] == "[REDACTED]"
        assert sanitized["secret_key"] == "[REDACTED]"
        assert sanitized["metadata"]["nested_token"] == "[REDACTED]"
        assert sanitized["metadata"]["normal_field"] == "allowed"

    def test_sanitize_connection_urls(self):
        url = "postgresql://my_user:secret_pass123@db.example.com:5432/production"
        sanitized = sanitize_value(url)
        assert "secret_pass123" not in sanitized
        assert sanitized == "postgresql://my_user:[REDACTED]@db.example.com:5432/production"

    def test_sanitize_lists_and_primitives(self):
        items = [{"token": "abc"}, {"name": "test"}, 123]
        sanitized = sanitize_value(items)
        assert sanitized[0]["token"] == "[REDACTED]"
        assert sanitized[1]["name"] == "test"
        assert sanitized[2] == 123

    def test_sanitize_cyclic_structure(self):
        d: dict[str, Any] = {"name": "alice"}
        d["self"] = d  # Cycle!
        sanitized = sanitize_value(d)
        assert sanitized["name"] == "alice"
        assert sanitized["self"] == "<cyclic-reference>"


class TestTransactionLogger:
    """Test TransactionLogger event emissions and formats."""

    def test_log_started_event(self, caplog):
        caplog.set_level(logging.DEBUG)
        logger = TransactionLogger(logging.getLogger("test.pgshield"))

        event = logger.log_started(attempt=1, transaction_id="tx-100", metadata={"user_id": 5})
        assert event.event_type == TransactionEventType.TRANSACTION_STARTED
        assert event.attempt == 1
        assert event.transaction_id == "tx-100"
        assert event.metadata == {"user_id": 5}
        assert "Transaction started [tx-100] (attempt 1)" in caplog.text

    def test_log_committed_event(self, caplog):
        caplog.set_level(logging.INFO)
        logger = TransactionLogger(logging.getLogger("test.pgshield"))

        event = logger.log_committed(attempt=1, duration=0.045, transaction_id="tx-200")
        assert event.event_type == TransactionEventType.TRANSACTION_COMMITTED
        assert event.duration == 0.045
        assert "Transaction committed [tx-200] in 45.00ms" in caplog.text

    def test_log_rolled_back_event(self, caplog):
        caplog.set_level(logging.WARNING)
        logger = TransactionLogger(logging.getLogger("test.pgshield"))

        event = logger.log_rolled_back(
            attempt=1,
            duration=0.02,
            transaction_id="tx-300",
            sqlstate="40001",
            error_message="conflict",
        )
        assert event.event_type == TransactionEventType.TRANSACTION_ROLLED_BACK
        assert event.sqlstate == "40001"
        assert "Transaction rolled back [tx-300] (SQLSTATE 40001)" in caplog.text

    def test_log_retry_event(self, caplog):
        caplog.set_level(logging.WARNING)
        logger = TransactionLogger(logging.getLogger("test.pgshield"))

        event = logger.log_retry(
            attempt=1,
            retry_delay=0.1,
            duration=0.05,
            transaction_id="tx-400",
            sqlstate="40P01",
            error_category="deadlock_detected",
            error_message="deadlock",
        )
        assert event.event_type == TransactionEventType.TRANSACTION_RETRY
        assert event.retry_delay == 0.1
        assert "Retrying transaction [tx-400] [deadlock_detected] (SQLSTATE 40P01)" in caplog.text

    def test_log_failed_event(self, caplog):
        caplog.set_level(logging.ERROR)
        logger = TransactionLogger(logging.getLogger("test.pgshield"))

        event = logger.log_failed(
            attempt=3,
            duration=0.35,
            transaction_id="tx-500",
            sqlstate="40001",
            error_category="serialization_failure",
            error_message="attempts exhausted",
        )
        assert event.event_type == TransactionEventType.TRANSACTION_FAILED
        assert event.attempt == 3
        assert (
            "Transaction failed permanently [tx-500] (SQLSTATE 40001) after 3 attempt(s)"
            in caplog.text
        )

    def test_logger_disabled(self, caplog):
        caplog.set_level(logging.DEBUG)
        logger = TransactionLogger(logging.getLogger("test.pgshield"), enabled=False)

        logger.log_started(attempt=1, transaction_id="disabled-tx")
        assert "disabled-tx" not in caplog.text
