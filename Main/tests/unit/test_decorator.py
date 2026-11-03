"""Unit tests for the @transaction decorator."""

import asyncio
from typing import List
import pytest

from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.context import get_current_context
from pgshield.transaction.decorator import transaction, transactional
from pgshield.transaction.runner import TransactionRunner


class MockDatabaseError(Exception):
    """Mock database exception."""

    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate

MockDatabaseError.__module__ = "asyncpg.exceptions"


class MockSleepRecorder:
    """Mock sleep recorder."""

    def __init__(self):
        self.delays: List[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class TestTransactionDecorator:
    """Test suite for the @transaction and @transactional decorators."""

    @pytest.mark.asyncio
    async def test_decorator_without_parentheses(self):
        """Test @transaction on an async function without kwargs."""
        call_count = 0

        @transaction
        async def fetch_user(user_id: int) -> dict:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise MockDatabaseError("40001 serialization", sqlstate="40001")
            return {"id": user_id, "name": "Alice"}

        # Preserve metadata
        assert fetch_user.__name__ == "fetch_user"

        result = await fetch_user(42)
        assert result == {"id": 42, "name": "Alice"}
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_decorator_with_custom_policy_kwargs(self):
        """Test @transaction with policy parameters and mock sleep."""
        sleeper = MockSleepRecorder()
        call_count = 0

        @transaction(max_attempts=3, base_delay=0.1, jitter=False, sleep_func=sleeper)
        async def update_inventory(item_id: str, quantity: int) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise MockDatabaseError("deadlock", sqlstate="40P01")
            return True

        result = await update_inventory("item_99", 5)
        assert result is True
        assert call_count == 3
        assert len(sleeper.delays) == 2
        assert sleeper.delays[0] == pytest.approx(0.10)
        assert sleeper.delays[1] == pytest.approx(0.20)

    @pytest.mark.asyncio
    async def test_transactional_alias(self):
        """Test that @transactional behaves identically to @transaction."""
        call_count = 0

        @transactional(max_attempts=2)
        async def compute() -> int:
            nonlocal call_count
            call_count += 1
            return 100

        res = await compute()
        assert res == 100
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_arguments_and_keyword_arguments(self):
        """Test that positional and keyword arguments are passed verbatim on each retry."""
        args_seen = []
        kwargs_seen = []

        @transaction(max_attempts=3)
        async def multi_arg_fn(a: int, b: str, *, flag: bool = False) -> str:
            args_seen.append((a, b))
            kwargs_seen.append(flag)
            if len(args_seen) == 1:
                raise MockDatabaseError("serialization", sqlstate="40001")
            return f"{a}-{b}-{flag}"

        result = await multi_arg_fn(10, "hello", flag=True)
        assert result == "10-hello-True"
        assert len(args_seen) == 2
        assert args_seen[0] == (10, "hello")
        assert args_seen[1] == (10, "hello")
        assert kwargs_seen == [True, True]

    @pytest.mark.asyncio
    async def test_decorator_with_preconfigured_runner(self):
        """Test injecting a pre-configured TransactionRunner into @transaction."""
        sleeper = MockSleepRecorder()
        custom_policy = RetryPolicy(max_attempts=4, base_delay=0.05, jitter=False)
        custom_runner = TransactionRunner(policy=custom_policy, sleep_func=sleeper)

        attempts = 0

        @transaction(runner=custom_runner)
        async def sample_op() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 4:
                raise MockDatabaseError("serialization", sqlstate="40001")
            return "runner_success"

        result = await sample_op()
        assert result == "runner_success"
        assert attempts == 4
        assert len(sleeper.delays) == 3
