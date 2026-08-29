"""Unit tests for PostgreSQL pg_stat_activity session inspection."""

import pytest
from pgshield.postgres.activity import (
    SessionActivity,
    build_activity_query,
    fetch_activity,
    parse_activity_rows,
)


class MockQueryExecutor:
    """Mock QueryExecutor returning predefined rows."""

    def __init__(self, rows):
        self.rows = rows
        self.executed_sqls = []

    async def execute_query(self, sql: str, params=None):
        self.executed_sqls.append(sql)
        return self.rows


class TestPostgresActivity:
    """Test suite for pg_stat_activity parsing and query construction."""

    def test_parse_activity_rows_basic(self):
        raw_rows = [
            {
                "pid": 12345,
                "database": "production",
                "username": "app_user",
                "application_name": "fastapi_worker_1",
                "client_addr": "10.0.0.5",
                "state": "active",
                "query_start": "2026-08-29 06:00:00",
                "xact_start": "2026-08-29 06:00:00",
                "wait_event_type": "Lock",
                "wait_event": "relation",
            },
            {
                "pid": 12346,
                "database": "production",
                "username": "app_user",
                "application_name": "fastapi_worker_2",
                "client_addr": "10.0.0.6",
                "state": "idle in transaction",
                "wait_event_type": "Client",
                "wait_event": "ClientRead",
            },
        ]

        activities = parse_activity_rows(raw_rows)
        assert len(activities) == 2

        act1 = activities[0]
        assert act1.pid == 12345
        assert act1.database == "production"
        assert act1.is_waiting_on_lock is True
        assert act1.is_idle_in_transaction is False

        act2 = activities[1]
        assert act2.pid == 12346
        assert act2.is_waiting_on_lock is False
        assert act2.is_idle_in_transaction is True

    def test_query_snippet_sanitization_and_toggle(self):
        raw_rows = [
            {
                "pid": 9999,
                "database": "app",
                "query_snippet": "SELECT * FROM users WHERE id = 123",
            }
        ]

        # By default include_query_snippet is False
        without_snippet = parse_activity_rows(raw_rows, include_query_snippet=False)
        assert without_snippet[0].query_snippet is None

        # When enabled
        with_snippet = parse_activity_rows(raw_rows, include_query_snippet=True)
        assert with_snippet[0].query_snippet == "SELECT * FROM users WHERE id = 123"

    def test_build_activity_query_filters(self):
        q_default = build_activity_query()
        assert "pid <> pg_backend_pid()" in q_default
        assert "LEFT(query" not in q_default

        q_snippet = build_activity_query(include_query_snippet=True)
        assert "LEFT(query, 120)" in q_snippet

        q_filtered = build_activity_query(
            filter_database="ecommerce",
            filter_pids=[100, 200],
        )
        assert "datname = 'ecommerce'" in q_filtered
        assert "pid IN (100, 200)" in q_filtered

    @pytest.mark.asyncio
    async def test_fetch_activity_with_executor(self):
        mock_data = [{"pid": 555, "database": "testdb", "state": "active"}]
        executor = MockQueryExecutor(mock_data)

        results = await fetch_activity(executor, filter_database="testdb")
        assert len(results) == 1
        assert results[0].pid == 555
        assert len(executor.executed_sqls) == 1
        assert "datname = 'testdb'" in executor.executed_sqls[0]
