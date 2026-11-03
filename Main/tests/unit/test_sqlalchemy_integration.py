"""Unit tests for SQLAlchemy AsyncSession integration in PGShield."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from pgshield.integrations.sqlalchemy import (
    SQLAlchemyQueryExecutor,
    SQLAlchemySessionAdapter,
    SQLAlchemyTransactionRunner,
    extract_sqlalchemy_sqlstate,
    run_sqlalchemy_transaction,
)
from pgshield.retry.policy import RetryPolicy


class MockDriverError(Exception):
    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate


MockDriverError.__module__ = "asyncpg.exceptions"


class MockSleepRecorder:
    def __init__(self):
        self.delays = []

    async def __call__(self, delay: float):
        self.delays.append(delay)


class TestSQLAlchemyIntegration:
    """Test suite for SQLAlchemy AsyncSession adapter and error extraction."""

    @pytest.mark.asyncio
    async def test_session_adapter_lifecycle(self):
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.in_transaction.return_value = False
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        adapter = SQLAlchemySessionAdapter(mock_session)

        # 1. Begin
        await adapter.begin()
        mock_session.begin.assert_awaited_once()

        # 2. Commit
        mock_session.in_transaction.return_value = True
        await adapter.commit()
        mock_session.commit.assert_awaited_once()

        # 3. Rollback
        await adapter.rollback()
        mock_session.rollback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_session_adapter_refuses_already_in_transaction(self):
        """begin() on a session already in a transaction raises RuntimeError."""
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.in_transaction.return_value = True
        mock_session.begin = AsyncMock()

        adapter = SQLAlchemySessionAdapter(mock_session)

        expected_msg = (
            "PGShield cannot begin a transaction on a session that is already in a transaction. "
            "Pass a session with no active transaction, "
            "or wrap your own boundary around a fresh session."
        )
        with pytest.raises(RuntimeError) as exc_info:
            await adapter.begin()

        assert str(exc_info.value) == expected_msg
        mock_session.begin.assert_not_called()

    @pytest.mark.asyncio
    async def test_session_adapter_begin_clean_session(self):
        """begin() on a clean session calls session.begin()."""
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.in_transaction.return_value = False
        mock_session.begin = AsyncMock()

        adapter = SQLAlchemySessionAdapter(mock_session)
        await adapter.begin()
        mock_session.begin.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_session_adapter_commit_and_rollback(self):
        """commit() calls session.commit(); rollback() calls session.rollback()."""
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        adapter = SQLAlchemySessionAdapter(mock_session)
        await adapter.commit()
        mock_session.commit.assert_awaited_once()

        await adapter.rollback()
        mock_session.rollback.assert_awaited_once()

    def test_extract_sqlstate_from_dbapi_error(self):
        inner_serialization = MockDriverError("could not serialize", sqlstate="40001")
        sa_error = OperationalError(
            statement="SELECT 1",
            params={},
            orig=inner_serialization,
        )

        extracted = extract_sqlalchemy_sqlstate(sa_error)
        assert extracted == "40001"

    def test_extract_sqlstate_from_integrity_error(self):
        inner_unique = MockDriverError("duplicate key", sqlstate="23505")
        sa_error = IntegrityError(
            statement="INSERT INTO tbl VALUES (1)",
            params={},
            orig=inner_unique,
        )

        extracted = extract_sqlalchemy_sqlstate(sa_error)
        assert extracted == "23505"

    @pytest.mark.asyncio
    async def test_run_sqlalchemy_transaction_retries_on_wrapped_40001(self):
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.in_transaction.return_value = False
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)
        runner = SQLAlchemyTransactionRunner(policy=policy, sleep_func=sleeper)

        attempts = 0

        async def business_logic(session: AsyncSession) -> dict:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                inner = MockDriverError("serialization failure", sqlstate="40001")
                raise OperationalError(statement="UPDATE accounts", params={}, orig=inner)
            return {"status": "success", "attempts": attempts}

        result = await run_sqlalchemy_transaction(
            mock_session,
            business_logic,
            runner=runner,
        )

        assert result == {"status": "success", "attempts": 2}
        assert attempts == 2
        assert len(sleeper.delays) == 1
        assert mock_session.rollback.await_count == 1
        assert mock_session.commit.await_count == 1

    @pytest.mark.asyncio
    async def test_run_sqlalchemy_transaction_fails_immediately_on_wrapped_23505(self):
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.in_transaction.return_value = False
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        sleeper = MockSleepRecorder()
        runner = SQLAlchemyTransactionRunner(sleep_func=sleeper)

        attempts = 0

        async def business_logic(session: AsyncSession):
            nonlocal attempts
            attempts += 1
            inner = MockDriverError("unique constraint", sqlstate="23505")
            raise IntegrityError(statement="INSERT INTO users", params={}, orig=inner)

        with pytest.raises(IntegrityError):
            await run_sqlalchemy_transaction(
                mock_session,
                business_logic,
                runner=runner,
            )

        assert attempts == 1
        assert len(sleeper.delays) == 0
        assert mock_session.rollback.await_count == 1
        assert mock_session.commit.await_count == 0

    @pytest.mark.asyncio
    async def test_sqlalchemy_query_executor_protocol_and_execution(self):
        """SQLAlchemyQueryExecutor satisfies QueryExecutor protocol and returns dicts."""
        from pgshield.postgres.activity import QueryExecutor

        mock_session = MagicMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_mapping = MagicMock()
        mock_mapping.all.return_value = [{"col1": "val1", "col2": 42}]
        mock_result.mappings.return_value = mock_mapping
        mock_session.execute = AsyncMock(return_value=mock_result)

        executor = SQLAlchemyQueryExecutor(mock_session)
        assert isinstance(executor, QueryExecutor)

        rows = await executor.execute_query(
            "SELECT col1, col2 FROM tbl WHERE col2 = :val", {"val": 42}
        )
        assert rows == [{"col1": "val1", "col2": 42}]
        assert isinstance(rows[0], dict)
        mock_session.execute.assert_awaited_once()
