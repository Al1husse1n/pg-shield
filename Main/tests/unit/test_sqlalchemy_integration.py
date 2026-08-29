"""Unit tests for SQLAlchemy AsyncSession integration in PGShield."""

from unittest.mock import AsyncMock, MagicMock
import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from pgshield.integrations.sqlalchemy import (
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
