"""Unit tests verifying TransactionRunner calling instrumentation (logger, metrics)."""

import logging
import pytest

from pgshield.instrumentation.logging import TransactionLogger
from pgshield.instrumentation.metrics import InMemoryMetricsCollector
from pgshield.postgres.diagnostics import DiagnosticCollector, DiagnosticMode
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.runner import TransactionRunner


class MockDatabaseError(Exception):
    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate


class MockSleepRecorder:
    def __init__(self):
        self.delays = []

    async def __call__(self, delay: float):
        self.delays.append(delay)


class TestRunnerInstrumentation:
    """Test suite for TransactionRunner instrumentation integration."""

    @pytest.mark.asyncio
    async def test_runner_emits_logs_and_metrics_on_success(self, caplog):
        caplog.set_level(logging.DEBUG)
        raw_logger = logging.getLogger("test.runner.success")
        tx_logger = TransactionLogger(raw_logger, enabled=True)
        metrics = InMemoryMetricsCollector()
        sleeper = MockSleepRecorder()

        runner = TransactionRunner(
            logger=tx_logger,
            metrics=metrics,
            sleep_func=sleeper,
        )

        async def op():
            return "ok"

        res = await runner.run(op, metadata={"transaction_id": "tx-success-1", "user": "alice"})
        assert res == "ok"

        # Metrics verification
        assert metrics.transactions_total == 1
        assert metrics.transactions_committed_total == 1
        assert metrics.transactions_failed_total == 0
        assert metrics.transactions_retried_total == 0

        # Log verification
        assert "Transaction started [tx-success-1]" in caplog.text
        assert "Transaction committed [tx-success-1]" in caplog.text

    @pytest.mark.asyncio
    async def test_runner_emits_logs_and_metrics_on_retry_and_success(self, caplog):
        caplog.set_level(logging.DEBUG)
        raw_logger = logging.getLogger("test.runner.retry")
        tx_logger = TransactionLogger(raw_logger, enabled=True)
        metrics = InMemoryMetricsCollector()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)

        runner = TransactionRunner(
            policy=policy,
            logger=tx_logger,
            metrics=metrics,
            sleep_func=sleeper,
        )

        attempts = 0

        async def op():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise MockDatabaseError("serialization conflict", sqlstate="40001")
            return "recovered"

        res = await runner.run(op, metadata={"transaction_id": "tx-retry-1"})
        assert res == "recovered"
        assert attempts == 2

        # Metrics verification
        assert metrics.transactions_total == 1
        assert metrics.transactions_retried_total == 1
        assert metrics.serialization_failures_total == 1
        assert metrics.transactions_committed_total == 1
        assert metrics.transactions_failed_total == 0

        # Log verification
        assert "Transaction rolled back [tx-retry-1] (SQLSTATE 40001)" in caplog.text
        assert "Retrying transaction [tx-retry-1] [serialization_failure] (SQLSTATE 40001)" in caplog.text
        assert "Transaction committed [tx-retry-1]" in caplog.text

    @pytest.mark.asyncio
    async def test_runner_emits_logs_and_metrics_on_failure_exhaustion(self, caplog):
        caplog.set_level(logging.DEBUG)
        raw_logger = logging.getLogger("test.runner.exhaust")
        tx_logger = TransactionLogger(raw_logger, enabled=True)
        metrics = InMemoryMetricsCollector()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=2, base_delay=0.05, jitter=False)

        runner = TransactionRunner(
            policy=policy,
            logger=tx_logger,
            metrics=metrics,
            sleep_func=sleeper,
        )

        attempts = 0

        async def op():
            nonlocal attempts
            attempts += 1
            raise MockDatabaseError("deadlock conflict", sqlstate="40P01")

        with pytest.raises(MockDatabaseError):
            await runner.run(op, metadata={"transaction_id": "tx-deadlock-1"})

        assert attempts == 2

        # Metrics verification
        assert metrics.transactions_total == 1
        assert metrics.transactions_retried_total == 1
        assert metrics.transactions_failed_total == 1
        assert metrics.deadlocks_total == 2  # 1 retry event + 1 final failure event

        # Log verification
        assert "Transaction failed permanently [tx-deadlock-1] (SQLSTATE 40P01) after 2 attempt(s)" in caplog.text

    @pytest.mark.asyncio
    async def test_runner_with_custom_extractor(self):
        class CustomAppError(Exception):
            def get_code(self):
                return "40001"

        attempts = 0
        sleeper = MockSleepRecorder()

        runner = TransactionRunner(
            policy=RetryPolicy(max_attempts=3, base_delay=0.01),
            sleep_func=sleeper,
            custom_extractor=lambda e: getattr(e, "get_code", lambda: None)(),
        )

        async def op():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise CustomAppError("custom conflict")
            return "ok"

        res = await runner.run(op)
        assert res == "ok"
        assert attempts == 2

    @pytest.mark.asyncio
    async def test_runner_collects_diagnostics_on_success_with_always_mode(self):
        diagnostics = DiagnosticCollector(mode=DiagnosticMode.ALWAYS)
        runner = TransactionRunner(diagnostics=diagnostics)

        async def op():
            return "success_value"

        result = await runner.run_with_result(op)
        assert result.value == "success_value"
        assert result.diagnostic_report is not None
        assert result.diagnostic_report.explanation == "Transaction completed without error."
