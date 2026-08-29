"""Unit tests for PostgreSQL diagnostics and report construction."""

import pytest
from pgshield.postgres.activity import SessionActivity
from pgshield.postgres.diagnostics import (
    DiagnosticCollector,
    DiagnosticMode,
    DiagnosticReport,
    build_diagnostic_report,
)
from pgshield.postgres.errors import (
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_SERIALIZATION_FAILURE,
    SQLSTATE_UNIQUE_VIOLATION,
    PostgresErrorCategory,
)
from pgshield.postgres.locks import LockInfo


class DummyDriverException(Exception):
    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate


class MockQueryExecutor:
    def __init__(self, activity_rows=None, lock_rows=None):
        self.activity_rows = activity_rows or []
        self.lock_rows = lock_rows or []

    async def execute_query(self, sql: str, params=None):
        if "pg_stat_activity" in sql:
            return self.activity_rows
        if "pg_locks" in sql:
            return self.lock_rows
        return []


class TestPostgresDiagnostics:
    """Test suite for DiagnosticReport generation and explanations."""

    def test_report_for_40001_serialization_failure(self):
        err = DummyDriverException("could not serialize access due to read/write dependencies", sqlstate=SQLSTATE_SERIALIZATION_FAILURE)

        report = build_diagnostic_report(
            error=err,
            attempt=1,
            duration=0.15,
            transaction_id="tx-test-1",
            isolation_level="SERIALIZABLE",
            pid=1234,
        )

        assert report.sqlstate == "40001"
        assert report.error_category == PostgresErrorCategory.SERIALIZATION_FAILURE
        assert "Serializable Snapshot Isolation (SSI)" in report.explanation
        assert "rw-antidependency" in report.explanation
        assert any("pg_locks cannot fully explain SSI" in w for w in report.warnings)
        assert any("Retry the entire transaction" in r for r in report.recommendations)

    def test_report_for_40p01_deadlock_with_blocking_locks(self):
        err = DummyDriverException("deadlock detected", sqlstate=SQLSTATE_DEADLOCK_DETECTED)

        locks = [
            LockInfo(pid=10, lock_type="relation", mode="ExclusiveLock", granted=True, relation_name="tbl_a"),
            LockInfo(pid=11, lock_type="relation", mode="ShareLock", granted=False, relation_name="tbl_a"),
        ]

        report = build_diagnostic_report(
            error=err,
            attempt=2,
            duration=0.50,
            transaction_id="tx-test-2",
            locks=locks,
        )

        assert report.sqlstate == "40P01"
        assert report.error_category == PostgresErrorCategory.DEADLOCK_DETECTED
        assert report.has_blocking is True
        assert len(report.blocking_chains) == 1
        assert "Deadlock detected" in report.explanation
        assert any("consistent global order" in r for r in report.recommendations)

    def test_report_for_integrity_violation(self):
        err = DummyDriverException("duplicate key violates unique constraint", sqlstate=SQLSTATE_UNIQUE_VIOLATION)

        report = build_diagnostic_report(error=err)
        assert report.sqlstate == "23505"
        assert report.error_category == PostgresErrorCategory.INTEGRITY_VIOLATION
        assert "deterministic data or business logic error" in report.explanation
        assert any("Do not retry automatically" in r for r in report.recommendations)

    def test_diagnostic_collector_mode_checks(self):
        collector_off = DiagnosticCollector(mode=DiagnosticMode.OFF)
        assert collector_off.should_collect(has_error=True) is False
        assert collector_off.should_collect(has_error=False) is False

        collector_failure = DiagnosticCollector(mode=DiagnosticMode.ON_FAILURE)
        assert collector_failure.should_collect(has_error=True) is True
        assert collector_failure.should_collect(has_error=False) is False

        collector_always = DiagnosticCollector(mode=DiagnosticMode.ALWAYS)
        assert collector_always.should_collect(has_error=True) is True
        assert collector_always.should_collect(has_error=False) is True

    @pytest.mark.asyncio
    async def test_diagnostic_collector_collect_flow(self):
        executor = MockQueryExecutor(
            activity_rows=[{"pid": 20, "database": "prod", "state": "active"}],
            lock_rows=[{"pid": 20, "lock_type": "relation", "mode": "RowExclusiveLock", "granted": True}],
        )

        collector = DiagnosticCollector(mode=DiagnosticMode.ON_FAILURE, executor=executor)

        # Successful transaction with ON_FAILURE -> None
        res_none = await collector.collect(error=None)
        assert res_none is None

        # Failed transaction with ON_FAILURE -> Collects and builds report
        err = DummyDriverException("could not serialize", sqlstate="40001")
        report = await collector.collect(error=err, attempt=1, duration=0.05)

        assert report is not None
        assert report.sqlstate == "40001"
        assert len(report.active_sessions) == 1
        assert report.active_sessions[0].pid == 20
        assert len(report.locks) == 1
        assert report.locks[0].pid == 20
