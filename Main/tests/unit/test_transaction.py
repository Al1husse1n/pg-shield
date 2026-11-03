"""Unit tests for the transaction execution layer (runner, context, adapter)."""

import asyncio
from typing import List, Optional
import pytest

from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.context import (
    AttemptRecord,
    TransactionContext,
    get_current_context,
)
from pgshield.transaction.runner import (
    NullTransactionAdapter,
    TransactionAdapter,
    TransactionResult,
    TransactionRunner,
)


class MockDatabaseError(Exception):
    """Mock database exception with SQLSTATE."""

    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate

MockDatabaseError.__module__ = "asyncpg.exceptions"


class MockTransactionAdapter:
    """Mock database transaction adapter tracking lifecycle calls."""

    def __init__(self, commit_error: Optional[Exception] = None):
        self.begins: int = 0
        self.commits: int = 0
        self.rollbacks: int = 0
        self.commit_error = commit_error
        self.commit_error_trigger_attempt: int = 1

    async def begin(self) -> None:
        self.begins += 1

    async def commit(self) -> None:
        self.commits += 1
        if self.commit_error and self.commits == self.commit_error_trigger_attempt:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollbacks += 1


class MockSleepRecorder:
    """Mock sleep callable capturing all requested delays without actual waiting."""

    def __init__(self):
        self.delays: List[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class TestTransactionRunner:
    """Test suite for TransactionRunner execution and retry orchestration."""

    @pytest.mark.asyncio
    async def test_1_successful_transaction_executes_once(self):
        """1. A successful transaction executes once, begins, commits, and returns value."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            return "success_value"

        result = await runner.run(op)

        assert result == "success_value"
        assert call_count == 1
        assert adapter.begins == 1
        assert adapter.commits == 1
        assert adapter.rollbacks == 0
        assert len(sleeper.delays) == 0

    @pytest.mark.asyncio
    async def test_2_non_retryable_exception_propagates_and_rolls_back(self):
        """2. Non-retryable exception executes once, rolls back, and propagates immediately."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            raise ValueError("invalid application data")

        with pytest.raises(ValueError, match="invalid application data"):
            await runner.run(op)

        assert call_count == 1
        assert adapter.begins == 1
        assert adapter.commits == 0
        assert adapter.rollbacks == 1
        assert len(sleeper.delays) == 0

    @pytest.mark.asyncio
    async def test_3_serialization_failure_40001_retries_whole_operation(self):
        """3. 40001 serialization failure: rolls back, delays with backoff, re-runs from start."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.1, jitter=False)
        runner = TransactionRunner(policy=policy, adapter=adapter, sleep_func=sleeper)

        call_count = 0
        execution_trace: List[str] = []

        async def op():
            nonlocal call_count
            call_count += 1
            execution_trace.append(f"read_attempt_{call_count}")
            execution_trace.append(f"business_logic_{call_count}")
            if call_count == 1:
                raise MockDatabaseError("could not serialize access", sqlstate="40001")
            execution_trace.append(f"write_attempt_{call_count}")
            return "serializable_success"

        result = await runner.run(op)

        assert result == "serializable_success"
        assert call_count == 2
        assert adapter.begins == 2
        assert adapter.commits == 1
        assert adapter.rollbacks == 1
        assert len(sleeper.delays) == 1
        assert sleeper.delays[0] == pytest.approx(0.10)
        assert execution_trace == [
            "read_attempt_1",
            "business_logic_1",
            "read_attempt_2",
            "business_logic_2",
            "write_attempt_2",
        ]

    @pytest.mark.asyncio
    async def test_4_deadlock_detected_40p01_retries_whole_operation(self):
        """4. 40P01 deadlock detected: rolls back, delays with backoff, re-runs whole operation."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.1, jitter=False)
        runner = TransactionRunner(policy=policy, adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise MockDatabaseError("deadlock detected", sqlstate="40P01")
            return "recovered_from_deadlock"

        result = await runner.run(op)

        assert result == "recovered_from_deadlock"
        assert call_count == 3
        assert adapter.begins == 3
        assert adapter.commits == 1
        assert adapter.rollbacks == 2
        assert len(sleeper.delays) == 2
        assert sleeper.delays[0] == pytest.approx(0.10)  # attempt 0
        assert sleeper.delays[1] == pytest.approx(0.20)  # attempt 1

    @pytest.mark.asyncio
    async def test_5_retry_limit_exhaustion_raises_original_error(self):
        """5. Reaching max_attempts raises the final exception after all rollbacks."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        runner = TransactionRunner(policy=policy, adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            raise MockDatabaseError(f"serialization failure {call_count}", sqlstate="40001")

        with pytest.raises(MockDatabaseError) as exc_info:
            await runner.run(op)

        assert "serialization failure 3" in str(exc_info.value)
        assert exc_info.value.sqlstate == "40001"
        assert call_count == 3
        assert adapter.begins == 3
        assert adapter.commits == 0
        assert adapter.rollbacks == 3
        assert len(sleeper.delays) == 2  # Delays between attempts 1->2 and 2->3

    @pytest.mark.asyncio
    async def test_6_operation_state_recreated_on_each_attempt(self):
        """6. State inside operation is executed fresh from the top on every retry."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        attempts_seen: List[int] = []

        async def op():
            ctx = get_current_context()
            assert ctx is not None
            attempts_seen.append(ctx.attempt)
            if ctx.attempt < 2:
                raise MockDatabaseError("conflict", sqlstate="40001")
            return f"done_at_attempt_{ctx.attempt}"

        result = await runner.run(op)
        assert result == "done_at_attempt_2"
        assert attempts_seen == [1, 2]

    @pytest.mark.asyncio
    async def test_7_commit_failure_never_retried_due_to_ambiguous_outcome(self):
        """7. Commit failure is never retried even with retryable SQLSTATE due to ambiguous outcome."""
        # Adapter will raise 40001 on the first commit attempt
        adapter = MockTransactionAdapter(
            commit_error=MockDatabaseError("commit serialization failure", sqlstate="40001")
        )
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        op_executions = 0

        async def op():
            nonlocal op_executions
            op_executions += 1
            return {"status": "ok"}

        with pytest.raises(MockDatabaseError, match="commit serialization failure"):
            await runner.run(op)

        assert op_executions == 1
        assert adapter.begins == 1
        assert adapter.commits == 1
        assert adapter.rollbacks == 1  # Rolled back after failed commit attempt 1
        assert len(sleeper.delays) == 0  # No backoff delay or retry

    @pytest.mark.asyncio
    async def test_8_returned_value_and_metadata_preserved(self):
        """8. Return value and TransactionResult metadata are complete and accurate."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)
        runner = TransactionRunner(policy=policy, adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise MockDatabaseError("retry me", sqlstate="40001")
            return {"user_id": 42, "balance": 1000}

        res: TransactionResult = await runner.run_with_result(op, metadata={"trace_id": "xyz-123"})

        assert res.value == {"user_id": 42, "balance": 1000}
        assert res.total_attempts == 2
        assert res.retried is True
        assert res.retry_count == 1
        assert len(res.history) == 2
        assert res.history[0].is_success is False
        assert res.history[0].sqlstate == "40001"
        assert res.history[0].backoff_delay == pytest.approx(0.05)
        assert res.history[1].is_success is True

    @pytest.mark.asyncio
    async def test_9_original_exception_preserved_on_non_retryable_sqlstate(self):
        """9. Non-retryable SQLSTATE (e.g. 23505 UniqueViolation) is preserved without retry."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            raise MockDatabaseError("duplicate key value violates unique constraint", sqlstate="23505")

        with pytest.raises(MockDatabaseError) as exc_info:
            await runner.run(op)

        assert exc_info.value.sqlstate == "23505"
        assert call_count == 1
        assert adapter.begins == 1
        assert adapter.rollbacks == 1
        assert adapter.commits == 0
        assert len(sleeper.delays) == 0

    @pytest.mark.asyncio
    async def test_10_cancellation_causes_immediate_rollback_and_propagates(self):
        """10. asyncio.CancelledError causes rollback and is never retried."""
        adapter = MockTransactionAdapter()
        sleeper = MockSleepRecorder()
        runner = TransactionRunner(adapter=adapter, sleep_func=sleeper)

        call_count = 0

        async def op():
            nonlocal call_count
            call_count += 1
            raise asyncio.CancelledError("task was cancelled")

        with pytest.raises(asyncio.CancelledError):
            await runner.run(op)

        assert call_count == 1
        assert adapter.rollbacks == 1
        assert len(sleeper.delays) == 0

    @pytest.mark.asyncio
    async def test_ambiguous_commit_failure_never_retried_with_fake_adapter(self, fake_adapter):
        """Ambiguous commit failure raises original error and never retries user callable."""
        fake_adapter.fail_on_commit = True
        fake_adapter.error_factory = lambda stage: MockDatabaseError(
            f"simulated {stage} network drop", sqlstate="40001"
        )
        sleeper = MockSleepRecorder()
        policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)
        runner = TransactionRunner(policy=policy, adapter=fake_adapter, sleep_func=sleeper)

        op_calls = 0

        async def business_op():
            nonlocal op_calls
            op_calls += 1
            return "transfer_complete"

        with pytest.raises(MockDatabaseError, match="simulated commit network drop") as exc_info:
            await runner.run(business_op)

        assert exc_info.value.sqlstate == "40001"
        assert op_calls == 1  # User callable must NEVER be re-executed on commit failure
        assert fake_adapter.begins == 1
        assert fake_adapter.commits == 1
        assert fake_adapter.rollbacks == 1  # Clean rollback attempted after commit failure
        assert len(sleeper.delays) == 0  # No backoff delay
