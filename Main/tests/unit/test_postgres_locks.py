"""Unit tests for PostgreSQL pg_locks inspection and blocking analysis."""

import pytest
from pgshield.postgres.locks import (
    BlockingRelationship,
    LockInfo,
    build_locks_query,
    detect_blocking_chains,
    fetch_locks,
    parse_lock_rows,
)


class MockQueryExecutor:
    """Mock QueryExecutor."""

    def __init__(self, rows):
        self.rows = rows
        self.executed_sqls = []

    async def execute_query(self, sql: str, params=None):
        self.executed_sqls.append(sql)
        return self.rows


class TestPostgresLocks:
    """Test suite for pg_locks parsing and blocking detection."""

    def test_parse_lock_rows_basic(self):
        raw_rows = [
            {
                "pid": 101,
                "lock_type": "relation",
                "mode": "RowExclusiveLock",
                "granted": True,
                "relation_name": "orders",
                "schema_name": "public",
                "page": None,
                "tuple": None,
                "transaction_id": None,
                "virtual_transaction": "3/10",
            },
            {
                "pid": 102,
                "lock_type": "relation",
                "mode": "AccessExclusiveLock",
                "granted": False,
                "relation_name": "orders",
                "schema_name": "public",
                "page": None,
                "tuple": None,
                "transaction_id": None,
                "virtual_transaction": "4/20",
            },
        ]

        locks = parse_lock_rows(raw_rows)
        assert len(locks) == 2

        l1 = locks[0]
        assert l1.pid == 101
        assert l1.lock_type == "relation"
        assert l1.mode == "RowExclusiveLock"
        assert l1.granted is True
        assert l1.is_waiting is False
        assert l1.qualified_relation == "public.orders"

        l2 = locks[1]
        assert l2.pid == 102
        assert l2.granted is False
        assert l2.is_waiting is True
        assert l2.qualified_relation == "public.orders"

    def test_detect_blocking_chains_relation_lock(self):
        """PID 200 holds RowExclusiveLock on public.accounts, PID 201 waits for ShareLock."""
        locks = [
            LockInfo(
                pid=200,
                lock_type="relation",
                mode="RowExclusiveLock",
                granted=True,
                relation_name="accounts",
                schema_name="public",
            ),
            LockInfo(
                pid=201,
                lock_type="relation",
                mode="ShareLock",
                granted=False,
                relation_name="accounts",
                schema_name="public",
            ),
        ]

        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 1

        b = blocking[0]
        assert b.blocked_pid == 201
        assert b.blocking_pid == 200
        assert b.lock_type == "relation"
        assert b.waiting_mode == "ShareLock"
        assert b.holding_mode == "RowExclusiveLock"
        assert "relation=public.accounts" in b.resource_description

    def test_detect_blocking_chains_tuple_or_xid_lock(self):
        """PID 300 holds transactionid lock, PID 301 is waiting on that transaction ID."""
        locks = [
            LockInfo(
                pid=300,
                lock_type="transactionid",
                mode="ExclusiveLock",
                granted=True,
                transaction_id="123456",
            ),
            LockInfo(
                pid=301,
                lock_type="transactionid",
                mode="ShareLock",
                granted=False,
                transaction_id="123456",
            ),
        ]

        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 1
        assert blocking[0].blocked_pid == 301
        assert blocking[0].blocking_pid == 300
        assert "xid=123456" in blocking[0].resource_description

    def test_build_locks_query_filters(self):
        q_default = build_locks_query()
        assert "FROM pg_locks" in q_default
        assert "pid <> pg_backend_pid()" in q_default

        q_filtered = build_locks_query(filter_pids=[401, 402])
        assert "l.pid IN (401, 402)" in q_filtered

    @pytest.mark.asyncio
    async def test_fetch_locks_with_executor(self):
        mock_data = [
            {
                "pid": 500,
                "lock_type": "relation",
                "mode": "ExclusiveLock",
                "granted": True,
                "relation_name": "items",
            }
        ]
        executor = MockQueryExecutor(mock_data)
        res = await fetch_locks(executor, filter_pids=[500])

        assert len(res) == 1
        assert res[0].pid == 500
        assert res[0].relation_name == "items"
        assert "l.pid IN (500)" in executor.executed_sqls[0]

    def test_detect_blocking_chains_with_different_virtual_transactions(self):
        """Processes have distinct virtual_transaction IDs but contest the same relation."""
        locks = [
            LockInfo(
                pid=101,
                lock_type="relation",
                mode="RowExclusiveLock",
                granted=True,
                relation_name="orders",
                schema_name="public",
                virtual_transaction="3/10",
            ),
            LockInfo(
                pid=102,
                lock_type="relation",
                mode="AccessExclusiveLock",
                granted=False,
                relation_name="orders",
                schema_name="public",
                virtual_transaction="4/20",
            ),
        ]
        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 1
        assert blocking[0].blocked_pid == 102
        assert blocking[0].blocking_pid == 101

    def test_build_locks_query_order_by_placement(self):
        q = build_locks_query(filter_pids=[10, 20])
        # Ensure WHERE filter is before ORDER BY
        pid_pos = q.index("l.pid IN (10, 20)")
        order_pos = q.index("ORDER BY")
        assert pid_pos < order_pos
