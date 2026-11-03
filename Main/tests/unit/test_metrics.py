"""Unit tests for metrics collector abstraction and InMemoryMetricsCollector."""

import pytest

from pgshield.instrumentation.metrics import (
    InMemoryMetricsCollector,
    NullMetricsCollector,
)


class TestMetricsCollector:
    """Test suite for metrics recording and aggregation."""

    def test_null_metrics_collector_no_op(self):
        collector = NullMetricsCollector()
        collector.record_transaction_started("tx-1")
        collector.record_transaction_committed(0.05, 1, "tx-1")
        collector.record_transaction_failed(0.1, 2, "40001", "serialization_failure")
        collector.record_transaction_retry(1, 0.05, "40001", "serialization_failure")
        collector.record_serialization_failure()
        collector.record_deadlock()

    def test_in_memory_metrics_collector_lifecycle(self):
        metrics = InMemoryMetricsCollector()

        # Initial state
        assert metrics.transactions_total == 0
        assert metrics.transactions_committed_total == 0
        assert metrics.transactions_failed_total == 0
        assert metrics.transactions_retried_total == 0
        assert metrics.serialization_failures_total == 0
        assert metrics.deadlocks_total == 0

        # Record start
        metrics.record_transaction_started("tx-1")
        assert metrics.transactions_total == 1

        # Record commit
        metrics.record_transaction_committed(0.05, 1, "tx-1")
        assert metrics.transactions_committed_total == 1

        # Record retried transaction with serialization failure
        metrics.record_transaction_started("tx-2")
        metrics.record_transaction_retry(
            1, 0.1, sqlstate="40001", error_category="serialization_failure"
        )
        metrics.record_transaction_committed(0.25, 2, "tx-2")

        assert metrics.transactions_total == 2
        assert metrics.transactions_committed_total == 2
        assert metrics.transactions_retried_total == 1
        assert metrics.serialization_failures_total == 1

        # Record failed transaction with deadlock
        metrics.record_transaction_started("tx-3")
        metrics.record_transaction_retry(
            1, 0.1, sqlstate="40P01", error_category="deadlock_detected"
        )
        metrics.record_transaction_failed(
            0.3, 2, sqlstate="40P01", error_category="deadlock_detected"
        )

        assert metrics.transactions_total == 3
        assert metrics.transactions_failed_total == 1
        assert metrics.transactions_retried_total == 2
        assert metrics.deadlocks_total == 2  # retry + fail

        # Snapshot checks
        snap = metrics.snapshot()
        assert snap.transactions_total == 3
        assert snap.transactions_committed_total == 2
        assert snap.transactions_failed_total == 1
        assert snap.transactions_retried_total == 2
        assert snap.serialization_failures_total == 1
        assert snap.deadlocks_total == 2
        assert snap.total_duration_seconds == pytest.approx(0.05 + 0.25 + 0.30)
        assert snap.avg_duration_seconds == pytest.approx((0.05 + 0.25 + 0.30) / 3)
        assert snap.max_duration_seconds == pytest.approx(0.30)
        assert snap.total_retry_delay_seconds == pytest.approx(0.20)
        assert snap.avg_retry_delay_seconds == pytest.approx(0.10)

        # Reset checks
        metrics.reset()
        assert metrics.transactions_total == 0
        assert metrics.transactions_committed_total == 0
        assert metrics.snapshot().avg_duration_seconds == 0.0

    def test_bounded_memory_under_high_volume(self):
        """Verify recording 100,000 transactions bounds sample collections."""
        metrics = InMemoryMetricsCollector(max_samples=10_000)

        for i in range(100_000):
            metrics.record_transaction_committed(0.01, 1, f"tx-{i}")
            if i % 10 == 0:
                metrics.record_transaction_retry(1, 0.005)

        assert metrics.transactions_committed_total == 100_000
        assert metrics.transactions_retried_total == 10_000

        # Internal collections must be strictly bounded to max_samples
        assert len(metrics._durations) == 10_000
        assert len(metrics._retry_delays) == 10_000

        snap = metrics.snapshot()
        assert snap.transactions_committed_total == 100_000
        assert snap.avg_duration_seconds == pytest.approx(0.01)
