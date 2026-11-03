"""Unit tests for PostgreSQL pg_locks inspection and blocking analysis."""

import pytest
from pgshield.postgres.locks import (
    AuthoritativeBlocking,
    BlockingRelationship,
    LockInfo,
    build_locks_query,
    detect_blocking_chains,
    fetch_blocking_pids,
    fetch_locks,
    fetch_locks_with_authoritative,
    lock_modes_conflict,
    parse_lock_rows,
)


class MockQueryExecutor:
    """Mock QueryExecutor."""

    def __init__(self, rows=None, blocking_pids=None):
        self.rows = rows or []
        self.blocking_pids = blocking_pids or []
        self.executed_sqls = []

    async def execute_query(self, sql: str, params=None):
        self.executed_sqls.append(sql)
        if "pg_blocking_pids" in sql:
            return [{"blocking_pids": self.blocking_pids}]
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

    def test_lock_compatibility_access_share_and_row_exclusive_no_conflict(self):
        """a. AccessShareLock holder + RowExclusiveLock waiter -> NO relationship."""
        locks = [
            LockInfo(pid=1, lock_type="relation", mode="AccessShareLock", granted=True, relation_name="t1"),
            LockInfo(pid=2, lock_type="relation", mode="RowExclusiveLock", granted=False, relation_name="t1"),
        ]
        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 0

    def test_lock_compatibility_row_exclusive_and_access_exclusive_conflict(self):
        """b. RowExclusiveLock holder + AccessExclusiveLock waiter -> relationship."""
        locks = [
            LockInfo(pid=1, lock_type="relation", mode="RowExclusiveLock", granted=True, relation_name="t1"),
            LockInfo(pid=2, lock_type="relation", mode="AccessExclusiveLock", granted=False, relation_name="t1"),
        ]
        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 1
        assert blocking[0].blocking_pid == 1
        assert blocking[0].blocked_pid == 2
        assert blocking[0].authoritative is False

    def test_lock_compatibility_multiple_access_share_and_access_exclusive(self):
        """c. Two AccessShareLock holders + one AccessExclusiveLock waiter -> two relationships."""
        locks = [
            LockInfo(pid=1, lock_type="relation", mode="AccessShareLock", granted=True, relation_name="t1"),
            LockInfo(pid=2, lock_type="relation", mode="AccessShareLock", granted=True, relation_name="t1"),
            LockInfo(pid=3, lock_type="relation", mode="AccessExclusiveLock", granted=False, relation_name="t1"),
        ]
        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 2
        assert {b.blocking_pid for b in blocking} == {1, 2}
        assert all(b.blocked_pid == 3 for b in blocking)

    def test_lock_compatibility_unknown_mode_failsafe_conflict(self):
        """d. Unknown mode (e.g. 'SomeFutureLock') + AccessExclusiveLock waiter -> relationship (fail-safe)."""
        locks = [
            LockInfo(pid=1, lock_type="relation", mode="SomeFutureLock", granted=True, relation_name="t1"),
            LockInfo(pid=2, lock_type="relation", mode="AccessExclusiveLock", granted=False, relation_name="t1"),
        ]
        blocking = detect_blocking_chains(locks)
        assert len(blocking) == 1
        assert blocking[0].blocking_pid == 1
        assert blocking[0].blocked_pid == 2

    @pytest.mark.asyncio
    async def test_fetch_locks_with_authoritative_blocking(self):
        """fetch_locks_with_authoritative returns locks and AuthoritativeBlocking records from pg_blocking_pids."""
        mock_lock_rows = [
            {
                "pid": 102,
                "lock_type": "relation",
                "mode": "AccessExclusiveLock",
                "granted": False,
                "relation_name": "orders",
            }
        ]
        executor = MockQueryExecutor(rows=mock_lock_rows, blocking_pids=[42, 43])

        pids = await fetch_blocking_pids(executor, pid=102)
        assert pids == [42, 43]

        locks, authoritative = await fetch_locks_with_authoritative(executor, filter_pids=[102])
        assert len(locks) == 1
        assert locks[0].pid == 102
        assert len(authoritative) == 1
        assert authoritative[0].blocked_pid == 102
        assert authoritative[0].blocking_pids == (42, 43)
