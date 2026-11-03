"""Unit tests for asyncpg driver integration in PGShield."""

from unittest.mock import AsyncMock, MagicMock

import asyncpg
import asyncpg.exceptions
import pytest
from asyncpg.transaction import Transaction

from pgshield.integrations.asyncpg import (
    AsyncpgConnectionAdapter,
    AsyncpgQueryExecutor,
    AsyncpgTransactionRunner,
    extract_asyncpg_sqlstate,
    run_asyncpg_transaction,
)
from pgshield.retry.policy import RetryPolicy


class MockSleepRecorder:
    def __init__(self):
        self.delays = []

    async def __call__(self, delay: float):
        self.delays.append(delay)


class TestAsyncpgIntegration:
    """Test suite for asyncpg Connection adapter and error extraction."""

    @pytest.mark.asyncio
    async def test_connection_adapter_lifecycle(self):
        mock_conn = MagicMock(spec=asyncpg.Connection)
        mock_conn.is_in_transaction.return_value = False
        mock_tx = MagicMock(spec=Transaction)
        mock_tx.start = AsyncMock()
        mock_tx.commit = AsyncMock()
        mock_tx.rollback = AsyncMock()
        mock_conn.transaction.return_value = mock_tx

        adapter = AsyncpgConnectionAdapter(mock_conn)

        # 1. Begin
        await adapter.begin()
        mock_conn.transaction.assert_called_once()
        mock_tx.start.assert_awaited_once()

        # 2. Commit
        await adapter.commit()
        mock_tx.commit.assert_awaited_once()

        # 3. Rollback
        await adapter.begin()
        await adapter.rollback()
        mock_tx.rollback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connection_adapter_refuses_already_in_transaction(self):
        """begin() on a connection already in a transaction raises RuntimeError."""
        mock_conn = MagicMock(spec=asyncpg.Connection)
        mock_conn.is_in_transaction.return_value = True

        adapter = AsyncpgConnectionAdapter(mock_conn)

        expected_msg = (
            "PGShield cannot begin a transaction on an asyncpg connection "
            "that is already in a transaction. "
            "Acquire a fresh connection from your pool for each retryable operation, "
            "or wrap your own boundary."
        )
        with pytest.raises(RuntimeError) as exc_info:
            await adapter.begin()

        assert str(exc_info.value) == expected_msg
        mock_conn.transaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_connection_adapter_begin_clean_connection(self):
        """begin() on a clean connection starts a transaction."""
        mock_conn = MagicMock(spec=asyncpg.Connection)
        mock_conn.is_in_transaction.return_value = False
        mock_tx = MagicMock(spec=Transaction)
        mock_tx.start = AsyncMock()
        mock_conn.transaction.return_value = mock_tx

        adapter = AsyncpgConnectionAdapter(mock_conn)
        await adapter.begin()
        mock_conn.transaction.assert_called_once()
        mock_tx.start.assert_awaited_once()

    def test_extract_sqlstate_from_asyncpg_exceptions(self):
        # asyncpg exceptions carry sqlstate attribute
        err_40001 = asyncpg.exceptions.SerializationError("could not serialize access")
        assert extract_asyncpg_sqlstate(err_40001) == "40001"

        err_40p01 = asyncpg.exceptions.DeadlockDetectedError("deadlock detected")
        assert extract_asyncpg_sqlstate(err_40p01) == "40P01"

        err_23505 = asyncpg.exceptions.UniqueViolationError("duplicate key")
        assert extract_asyncpg_sqlstate(err_23505) == "23505"

    @pytest.mark.asyncio
    async def test_run_asyncpg_transaction_retries_on_40001(self):
        mock_conn = MagicMock(spec=asyncpg.Connection)
        mock_conn.is_in_transaction.return_value = False
        mock_tx = MagicMock(spec=Transaction)
        mock_tx.start = AsyncMock()
        mock_tx.commit = AsyncMock()
        mock_tx.rollback = AsyncMock()
        mock_conn.transaction.return_value = mock_tx

        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)
        runner = AsyncpgTransactionRunner(policy=policy, sleep_func=sleeper)

        attempts = 0

        async def business_logic(conn: asyncpg.Connection) -> dict:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise asyncpg.exceptions.SerializationError("could not serialize access")
            return {"status": "success", "attempts": attempts}

        result = await run_asyncpg_transaction(
            mock_conn,
            business_logic,
            runner=runner,
        )

        assert result == {"status": "success", "attempts": 2}
        assert attempts == 2
        assert len(sleeper.delays) == 1
        assert mock_tx.rollback.await_count == 1
        assert mock_tx.commit.await_count == 1

    @pytest.mark.asyncio
    async def test_run_asyncpg_transaction_fails_immediately_on_23505(self):
        mock_conn = MagicMock(spec=asyncpg.Connection)
        mock_conn.is_in_transaction.return_value = False
        mock_tx = MagicMock(spec=Transaction)
        mock_tx.start = AsyncMock()
        mock_tx.commit = AsyncMock()
        mock_tx.rollback = AsyncMock()
        mock_conn.transaction.return_value = mock_tx

        sleeper = MockSleepRecorder()
        runner = AsyncpgTransactionRunner(sleep_func=sleeper)

        attempts = 0

        async def business_logic(conn: asyncpg.Connection):
            nonlocal attempts
            attempts += 1
            raise asyncpg.exceptions.UniqueViolationError("unique constraint violated")

        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await run_asyncpg_transaction(
                mock_conn,
                business_logic,
                runner=runner,
            )

        assert attempts == 1
        assert len(sleeper.delays) == 0
        assert mock_tx.rollback.await_count == 1
        assert mock_tx.commit.await_count == 0

    @pytest.mark.asyncio
    async def test_asyncpg_query_executor_protocol_and_execution(self):
        """AsyncpgQueryExecutor satisfies QueryExecutor protocol and returns dicts."""
        from pgshield.postgres.activity import QueryExecutor

        mock_conn = MagicMock(spec=asyncpg.Connection)
        fake_record = {"pid": 1234, "state": "active"}
        mock_conn.fetch = AsyncMock(return_value=[fake_record])

        executor = AsyncpgQueryExecutor(mock_conn)
        assert isinstance(executor, QueryExecutor)

        rows = await executor.execute_query(
            "SELECT pid, state FROM pg_stat_activity WHERE pid = $1", [1234]
        )
        mock_conn.fetch.assert_awaited_once_with(
            "SELECT pid, state FROM pg_stat_activity WHERE pid = $1", 1234
        )
        assert rows == [{"pid": 1234, "state": "active"}]
        assert isinstance(rows[0], dict)
