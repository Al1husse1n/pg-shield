"""Integration tests against real PostgreSQL instance for PGShield.

All tests in this module execute against a live PostgreSQL database provided by
the PGSHIELD_TEST_DSN environment variable. They are decorated with @pytest.mark.integration
and are deselected by default during local unit test runs.
"""

import asyncio

import asyncpg
import asyncpg.exceptions
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from pgshield.integrations.asyncpg import (
    AsyncpgQueryExecutor,
    AsyncpgTransactionRunner,
    run_asyncpg_transaction,
)
from pgshield.integrations.sqlalchemy import (
    run_sqlalchemy_transaction,
)
from pgshield.postgres.locks import (
    fetch_blocking_pids,
    fetch_locks_with_authoritative,
)
from pgshield.retry.policy import RetryPolicy

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Existing integration tests updated with fixtures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_real_postgres_asyncpg_successful_transaction(asyncpg_conn: asyncpg.Connection):
    """Verify transaction commit and query execution on real PostgreSQL using asyncpg."""
    await asyncpg_conn.execute(
        "CREATE TEMP TABLE test_pgshield_items (id SERIAL PRIMARY KEY, val TEXT);"
    )

    async def insert_item(c: asyncpg.Connection) -> int:
        row = await c.fetchrow(
            "INSERT INTO test_pgshield_items (val) VALUES ($1) RETURNING id;",
            "hello_pgshield",
        )
        return row["id"]

    item_id = await run_asyncpg_transaction(asyncpg_conn, insert_item)
    assert item_id is not None

    count = await asyncpg_conn.fetchval("SELECT count(*) FROM test_pgshield_items;")
    assert count == 1


@pytest.mark.asyncio
async def test_real_postgres_sqlalchemy_rollback_on_error(sqlalchemy_session: AsyncSession):
    """Verify transaction rollback on non-retryable error using real PostgreSQL + SQLAlchemy."""
    await sqlalchemy_session.execute(text("CREATE TEMP TABLE test_sa_items (id INT PRIMARY KEY);"))
    await sqlalchemy_session.commit()

    async def first_insert(s: AsyncSession):
        await s.execute(text("INSERT INTO test_sa_items (id) VALUES (1);"))

    await run_sqlalchemy_transaction(sqlalchemy_session, first_insert)

    # Duplicate insert (unique violation 23505) -> should rollback and propagate
    async def duplicate_insert(s: AsyncSession):
        await s.execute(text("INSERT INTO test_sa_items (id) VALUES (1);"))

    with pytest.raises(Exception) as exc_info:
        await run_sqlalchemy_transaction(sqlalchemy_session, duplicate_insert)

    assert "23505" in str(exc_info.value) or "unique" in str(exc_info.value).lower()

    # Check only 1 item exists in table after rollback
    res = await sqlalchemy_session.execute(text("SELECT count(*) FROM test_sa_items;"))
    assert res.scalar() == 1


# ---------------------------------------------------------------------------
# Phase 3 Required Integration Tests (a through f)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_40001_is_retried_to_success(pg_dsn: str):
    """(a) Two concurrent SERIALIZABLE transactions update the same row.

    One transaction deterministically receives SQLSTATE 40001 (serialization_failure),
    and PGShield retries the loser until both transfers complete cleanly.
    Coroutines are deterministically synchronized using asyncio.Event.
    """
    conn_setup = await asyncpg.connect(pg_dsn)
    await conn_setup.execute(
        "CREATE TABLE IF NOT EXISTS test_accounts (id INT PRIMARY KEY, balance INT);"
    )
    await conn_setup.execute(
        "TRUNCATE TABLE test_accounts; INSERT INTO test_accounts (id, balance) VALUES (1, 100);"
    )
    await conn_setup.close()

    conn1 = await asyncpg.connect(pg_dsn)
    conn2 = await asyncpg.connect(pg_dsn)
    await conn1.execute("SET default_transaction_isolation = 'serializable';")
    await conn2.execute("SET default_transaction_isolation = 'serializable';")

    # Events for deterministic concurrency choreography
    t1_read = asyncio.Event()
    t2_read = asyncio.Event()
    t1_committed = asyncio.Event()

    t2_attempts = 0

    async def tx1_workflow():
        async with conn1.transaction():
            # Step 1: T1 reads balance
            await conn1.fetchval("SELECT balance FROM test_accounts WHERE id = 1;")
            t1_read.set()
            # Wait for T2 to also read the same initial state before T1 updates
            await t2_read.wait()
            # Step 3: T1 updates and commits
            await conn1.execute("UPDATE test_accounts SET balance = balance + 10 WHERE id = 1;")
        t1_committed.set()

    async def tx2_operation(c: asyncpg.Connection):
        nonlocal t2_attempts
        t2_attempts += 1
        if t2_attempts == 1:
            # Attempt 1: Wait until T1 has read, read initial balance, then signal T1
            await t1_read.wait()
            val = await c.fetchval("SELECT balance FROM test_accounts WHERE id = 1;")
            t2_read.set()
            # Wait until T1 commits its conflicting write
            await t1_committed.wait()
            # This write produces deterministic 40001 serialization_failure
            await c.execute("UPDATE test_accounts SET balance = balance + 20 WHERE id = 1;")
            return val
        else:
            # Attempt 2 (Retry): Read fresh committed state and apply update
            val = await c.fetchval("SELECT balance FROM test_accounts WHERE id = 1;")
            await c.execute("UPDATE test_accounts SET balance = balance + 20 WHERE id = 1;")
            return val

    runner2 = AsyncpgTransactionRunner(
        policy=RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
    )

    try:
        t1_task = asyncio.create_task(tx1_workflow())
        t2_task = asyncio.create_task(run_asyncpg_transaction(conn2, tx2_operation, runner=runner2))
        await asyncio.gather(t1_task, t2_task)

        # Verification: T2 experienced serialization failure and retried to success
        assert t2_attempts == 2, f"Expected 2 attempts for T2, got {t2_attempts}"

        # Final DB state verification: 100 + 10 + 20 = 130
        final_balance = await conn1.fetchval("SELECT balance FROM test_accounts WHERE id = 1;")
        assert final_balance == 130
    finally:
        await conn1.execute("DROP TABLE IF EXISTS test_accounts;")
        await conn1.close()
        await conn2.close()


@pytest.mark.asyncio
async def test_40p01_deadlock_is_retried_to_success(pg_dsn: str):
    """(b) Two concurrent transactions acquire locks in opposite order causing 40P01.

    The loser receives 40P01 (deadlock_detected) and PGShield retries to success.
    Coroutines are deterministically synchronized using asyncio.Event.
    """
    conn_setup = await asyncpg.connect(pg_dsn)
    await conn_setup.execute(
        "CREATE TABLE IF NOT EXISTS test_deadlock_records (id INT PRIMARY KEY, val TEXT);"
    )
    await conn_setup.execute(
        "TRUNCATE TABLE test_deadlock_records; "
        "INSERT INTO test_deadlock_records (id, val) VALUES (1, 'v1'), (2, 'v2');"
    )
    await conn_setup.close()

    conn1 = await asyncpg.connect(pg_dsn)
    conn2 = await asyncpg.connect(pg_dsn)
    # Fast deadlock detection on both backends
    await conn1.execute("SET deadlock_timeout = '50ms';")
    await conn2.execute("SET deadlock_timeout = '50ms';")

    t1_locked_1 = asyncio.Event()
    t2_locked_2 = asyncio.Event()

    t1_attempts = 0
    t2_attempts = 0

    async def tx1_op(c: asyncpg.Connection):
        nonlocal t1_attempts
        t1_attempts += 1
        if t1_attempts == 1:
            await c.execute("UPDATE test_deadlock_records SET val = 't1_1' WHERE id = 1;")
            t1_locked_1.set()
            await t2_locked_2.wait()
            # Opposing lock acquisition -> causes deadlock
            await c.execute("UPDATE test_deadlock_records SET val = 't1_2' WHERE id = 2;")
        else:
            await c.execute("UPDATE test_deadlock_records SET val = 't1_final_1' WHERE id = 1;")
            await c.execute("UPDATE test_deadlock_records SET val = 't1_final_2' WHERE id = 2;")

    async def tx2_op(c: asyncpg.Connection):
        nonlocal t2_attempts
        t2_attempts += 1
        if t2_attempts == 1:
            await t1_locked_1.wait()
            await c.execute("UPDATE test_deadlock_records SET val = 't2_2' WHERE id = 2;")
            t2_locked_2.set()
            # Opposing lock acquisition -> causes deadlock
            await c.execute("UPDATE test_deadlock_records SET val = 't2_1' WHERE id = 1;")
        else:
            await c.execute("UPDATE test_deadlock_records SET val = 't2_final_2' WHERE id = 2;")
            await c.execute("UPDATE test_deadlock_records SET val = 't2_final_1' WHERE id = 1;")

    policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)
    runner1 = AsyncpgTransactionRunner(policy=policy)
    runner2 = AsyncpgTransactionRunner(policy=policy)

    try:
        t1_task = asyncio.create_task(run_asyncpg_transaction(conn1, tx1_op, runner=runner1))
        t2_task = asyncio.create_task(run_asyncpg_transaction(conn2, tx2_op, runner=runner2))
        await asyncio.gather(t1_task, t2_task)

        # Exactly one transaction was selected as deadlock victim and retried
        assert t1_attempts + t2_attempts >= 3, (
            f"Expected at least one retry due to deadlock, got t1={t1_attempts}, t2={t2_attempts}"
        )
    finally:
        await conn1.execute("DROP TABLE IF EXISTS test_deadlock_records;")
        await conn1.close()
        await conn2.close()


@pytest.mark.asyncio
async def test_rollback_observed_on_server_between_attempts(pg_dsn: str):
    """(c) Verify rollback is executed on the server before backoff sleep.

    Captures pre-backoff observation while the transaction is active on attempt 1,
    then captures during-backoff observation inside sleep_func to prove ordering:
    the transaction was open pre-backoff, and rollback occurred before backoff sleep.
    """
    runner_conn = await asyncpg.connect(pg_dsn)
    observer_conn = await asyncpg.connect(pg_dsn)

    runner_pid = await runner_conn.fetchval("SELECT pg_backend_pid();")

    pre_backoff_observation = None
    during_backoff_observation = None

    async def inspecting_sleep(delay: float):
        nonlocal during_backoff_observation
        # Query pg_stat_activity during the backoff delay
        row = await observer_conn.fetchrow(
            "SELECT state, xact_start FROM pg_stat_activity WHERE pid = $1;",
            runner_pid,
        )
        if row:
            during_backoff_observation = {
                "state": row["state"],
                "xact_start": row["xact_start"],
            }
        await asyncio.sleep(delay)

    attempts = 0

    async def flaky_op(conn: asyncpg.Connection):
        nonlocal attempts, pre_backoff_observation
        attempts += 1
        if attempts == 1:
            # Active statement within the transaction to register activity in PostgreSQL
            await conn.execute("SELECT 1;")
            # Pre-backoff observation: prove there WAS an open transaction before rollback/backoff
            row = await observer_conn.fetchrow(
                "SELECT state, xact_start FROM pg_stat_activity WHERE pid = $1;",
                runner_pid,
            )
            if row:
                pre_backoff_observation = {
                    "state": row["state"],
                    "xact_start": row["xact_start"],
                }
            raise asyncpg.exceptions.SerializationError("serialization failure")
        return "success"

    policy = RetryPolicy(max_attempts=2, base_delay=0.02, jitter=False)
    runner = AsyncpgTransactionRunner(policy=policy, sleep_func=inspecting_sleep)

    try:
        result = await run_asyncpg_transaction(runner_conn, flaky_op, runner=runner)
        assert result == "success"
        assert attempts == 2

        # 1. Assert pre-backoff observation proves an open transaction existed before backoff
        assert pre_backoff_observation is not None
        assert pre_backoff_observation["xact_start"] is not None

        # 2. Assert during-backoff observation proves the transaction was closed before backoff
        assert during_backoff_observation is not None
        assert during_backoff_observation["state"] == "idle"
        assert during_backoff_observation["xact_start"] is None
    finally:
        await runner_conn.close()
        await observer_conn.close()


@pytest.mark.asyncio
async def test_whole_callable_reexecuted_per_attempt(asyncpg_conn: asyncpg.Connection):
    """(d) Whole callable re-executed per attempt.

    A side-effect counter at the very top of the callable increments on every attempt.
    """
    executions = 0

    async def operation(conn: asyncpg.Connection) -> str:
        nonlocal executions
        executions += 1
        if executions == 1:
            raise asyncpg.exceptions.SerializationError("deterministic failure on attempt 1")
        await conn.execute("SELECT 1;")
        return "complete"

    policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
    runner = AsyncpgTransactionRunner(policy=policy)

    result = await run_asyncpg_transaction(asyncpg_conn, operation, runner=runner)
    assert result == "complete"
    assert executions == 2, f"Expected callable to re-execute from top, count was {executions}"


@pytest.mark.asyncio
async def test_sqlalchemy_already_in_transaction_refused(sqlalchemy_session: AsyncSession):
    """(e) Refuse SQLAlchemy AsyncSession already inside an active transaction."""
    await sqlalchemy_session.begin()

    expected_msg = (
        "PGShield cannot begin a transaction on a session that is already in a transaction. "
        "Pass a session with no active transaction, "
        "or wrap your own boundary around a fresh session."
    )

    with pytest.raises(RuntimeError) as exc_info:
        await run_sqlalchemy_transaction(
            sqlalchemy_session,
            lambda s: s.execute(text("SELECT 1")),
        )

    assert str(exc_info.value) == expected_msg
    await sqlalchemy_session.rollback()


@pytest.mark.asyncio
async def test_asyncpg_already_in_transaction_refused(asyncpg_conn: asyncpg.Connection):
    """(f) Refuse asyncpg.Connection already inside an active transaction."""
    tx = asyncpg_conn.transaction()
    await tx.start()

    expected_msg = (
        "PGShield cannot begin a transaction on an asyncpg connection "
        "that is already in a transaction. "
        "Acquire a fresh connection from your pool for each retryable operation, "
        "or wrap your own boundary."
    )

    try:
        with pytest.raises(RuntimeError) as exc_info:
            await run_asyncpg_transaction(
                asyncpg_conn,
                lambda c: c.execute("SELECT 1"),
            )

        assert str(exc_info.value) == expected_msg
    finally:
        await tx.rollback()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_pg_blocking_pids_matches_authoritative_channel(pg_dsn: str):
    """Verify that pg_blocking_pids accurately detects real-time blocking relationships.

    Tests the authoritative diagnostic channel against a live PostgreSQL server
    where conn2 is blocked by conn1 holding an exclusive row lock.
    """
    conn_setup = await asyncpg.connect(pg_dsn)
    try:
        await conn_setup.execute(
            "CREATE TABLE IF NOT EXISTS test_auth_channel (id INT PRIMARY KEY, val TEXT);"
        )
        await conn_setup.execute(
            "TRUNCATE TABLE test_auth_channel; "
            "INSERT INTO test_auth_channel (id, val) VALUES (1, 'initial');"
        )
    finally:
        await conn_setup.close()

    conn1 = await asyncpg.connect(pg_dsn)
    conn2 = await asyncpg.connect(pg_dsn)
    observer = await asyncpg.connect(pg_dsn)

    task2 = None
    try:
        conn1_pid = await conn1.fetchval("SELECT pg_backend_pid();")
        conn2_pid = await conn2.fetchval("SELECT pg_backend_pid();")

        # conn1 starts a transaction and acquires an exclusive row lock
        tx1 = conn1.transaction()
        await tx1.start()
        await conn1.execute("SELECT * FROM test_auth_channel WHERE id = 1 FOR UPDATE;")

        # conn2 starts a transaction and requests the same row lock (will block)
        tx2 = conn2.transaction()
        await tx2.start()

        task2 = asyncio.create_task(
            conn2.execute("SELECT * FROM test_auth_channel WHERE id = 1 FOR UPDATE;")
        )

        # Wait until conn2 appears as waiting in pg_locks
        for _ in range(50):
            waiting = await observer.fetchval(
                "SELECT count(*) FROM pg_locks WHERE pid = $1 AND NOT granted;",
                conn2_pid,
            )
            if waiting and waiting > 0:
                break
            await asyncio.sleep(0.05)

        observer_executor = AsyncpgQueryExecutor(observer)

        try:
            blocking_pids = await fetch_blocking_pids(observer_executor, pid=conn2_pid)
        except (asyncpg.exceptions.InsufficientPrivilegeError, PermissionError) as exc:
            pytest.skip(
                f"pg_blocking_pids requires pg_monitor or superuser to view other sessions: {exc}"
            )
        except Exception as exc:
            if (
                "permission denied" in str(exc).lower()
                or "insufficient privilege" in str(exc).lower()
            ):
                pytest.skip(
                    "pg_blocking_pids requires pg_monitor or superuser to view other sessions"
                )
            raise

        # Assert conn1's PID is in the returned list
        assert conn1_pid in blocking_pids

        # Also call fetch_locks_with_authoritative and verify conn2's entry
        locks, authoritative_blocking = await fetch_locks_with_authoritative(observer_executor)
        conn2_auth = [item for item in authoritative_blocking if item.blocked_pid == conn2_pid]
        assert len(conn2_auth) >= 1
        assert conn1_pid in conn2_auth[0].blocking_pids

    finally:
        if task2 is not None and not task2.done():
            task2.cancel()
            try:
                await task2
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await conn2.close()
        except Exception:
            pass
        try:
            await tx1.rollback()
        except Exception:
            pass
        try:
            await conn1.close()
        except Exception:
            pass
        try:
            await observer.execute("DROP TABLE IF EXISTS test_auth_channel;")
            await observer.close()
        except Exception:
            pass
