"""End-to-end verification and integration test suite for PGShield.

Validates the full PGShield concurrency toolkit across:
- Test 1: Normal transaction execution (single attempt, commit, metrics/logs)
- Test 2: 40001 serialization failure (rollback, backoff, replay, commit, retry metric)
- Test 3: 40P01 deadlock detected (rollback, backoff, whole-operation replay, diagnostics)
- Test 4: Non-retryable error (23505 unique constraint violation, immediate rollback & propagation)
- Test 5: Retry limit exhaustion (stops at max_attempts without infinite loops)
- Test 6: Deterministic jittered exponential backoff bounds & growth
- Test 7: Concurrency diagnostics with pg_stat_activity / pg_locks and SSI accuracy
- Test 8: Full FastAPI application integration with AsyncSession, logging, and metrics
"""

import logging
from typing import Dict, List

import pytest
from fastapi import FastAPI, HTTPException, status
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from pgshield import (
    DiagnosticCollector,
    DiagnosticMode,
    DiagnosticReport,
    InMemoryMetricsCollector,
    LockInfo,
    PostgresErrorCategory,
    RetryPolicy,
    TransactionLogger,
    TransactionRunner,
    calculate_backoff,
)


class MockDatabaseError(Exception):
    def __init__(self, message: str, sqlstate: str):
        super().__init__(message)
        self.sqlstate = sqlstate


MockDatabaseError.__module__ = "asyncpg.exceptions"


class MockSleepRecorder:
    def __init__(self):
        self.delays: List[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class MockDbAdapter:
    """Mock database adapter tracking transaction lifecycle."""

    def __init__(self):
        self.begins = 0
        self.commits = 0
        self.rollbacks = 0

    async def begin(self):
        self.begins += 1

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


# =========================================================================
# TEST 1 — NORMAL TRANSACTION
# =========================================================================
@pytest.mark.asyncio
async def test_1_normal_transaction(caplog):
    """Verify normal transaction executes once, commits, and logs/metrics record success."""
    caplog.set_level(logging.DEBUG)
    adapter = MockDbAdapter()
    metrics = InMemoryMetricsCollector()
    raw_logger = logging.getLogger("test.pgshield.t1")
    logger = TransactionLogger(raw_logger, enabled=True)
    sleeper = MockSleepRecorder()

    runner = TransactionRunner(
        adapter=adapter,
        metrics=metrics,
        logger=logger,
        sleep_func=sleeper,
    )

    # Simulated row state
    account = {"id": 1, "balance": 100}
    trace: List[str] = []

    async def transfer():
        trace.append("read")
        account["balance"] -= 30
        trace.append("modify")
        return account["balance"]

    result = await runner.run(transfer, metadata={"transaction_id": "tx-normal-1"})

    assert result == 70
    assert trace == ["read", "modify"]
    assert adapter.begins == 1
    assert adapter.commits == 1
    assert adapter.rollbacks == 0
    assert len(sleeper.delays) == 0

    # Metrics
    assert metrics.transactions_total == 1
    assert metrics.transactions_committed_total == 1
    assert metrics.transactions_failed_total == 0
    assert metrics.transactions_retried_total == 0

    # Logs
    assert "Transaction started [tx-normal-1]" in caplog.text
    assert "Transaction committed [tx-normal-1]" in caplog.text


# =========================================================================
# TEST 2 — SERIALIZATION FAILURE (40001)
# =========================================================================
@pytest.mark.asyncio
async def test_2_serialization_failure_whole_operation_replay(caplog):
    """Verify 40001 causes rollback, backoff, whole-op replay from start, and commit."""
    caplog.set_level(logging.DEBUG)
    adapter = MockDbAdapter()
    metrics = InMemoryMetricsCollector()
    logger = TransactionLogger(logging.getLogger("test.pgshield.t2"), enabled=True)
    sleeper = MockSleepRecorder()
    policy = RetryPolicy(max_attempts=3, base_delay=0.05, jitter=False)

    runner = TransactionRunner(
        policy=policy,
        adapter=adapter,
        metrics=metrics,
        logger=logger,
        sleep_func=sleeper,
    )

    attempts = 0
    full_execution_trace: List[str] = []

    async def transfer_with_conflict():
        nonlocal attempts
        attempts += 1
        full_execution_trace.append(f"read_attempt_{attempts}")
        full_execution_trace.append(f"business_logic_attempt_{attempts}")

        if attempts == 1:
            # Simulate PostgreSQL 40001 serialization failure at write/commit time
            raise MockDatabaseError(
                "could not serialize access due to read/write dependencies among transactions",
                sqlstate="40001",
            )

        full_execution_trace.append(f"write_attempt_{attempts}")
        return {"status": "transferred", "attempt": attempts}

    result = await runner.run(transfer_with_conflict, metadata={"transaction_id": "tx-40001"})

    assert result == {"status": "transferred", "attempt": 2}
    assert attempts == 2
    assert adapter.begins == 2
    assert adapter.rollbacks == 1
    assert adapter.commits == 1
    assert len(sleeper.delays) == 1
    assert sleeper.delays[0] == pytest.approx(0.05)

    # Prove the read/business-logic sequence executed again from the start
    assert full_execution_trace == [
        "read_attempt_1",
        "business_logic_attempt_1",
        "read_attempt_2",
        "business_logic_attempt_2",
        "write_attempt_2",
    ]

    # Metrics
    assert metrics.transactions_total == 1
    assert metrics.transactions_retried_total == 1
    assert metrics.serialization_failures_total == 1
    assert metrics.transactions_committed_total == 1

    # Logs
    assert "Retrying transaction [tx-40001] [serialization_failure] (SQLSTATE 40001)" in caplog.text
    assert "Transaction committed [tx-40001]" in caplog.text


# =========================================================================
# TEST 3 — DEADLOCK (40P01)
# =========================================================================
@pytest.mark.asyncio
async def test_3_deadlock_detected_and_retried():
    """Verify 40P01 deadlock is caught, rolled back, and retried successfully."""
    adapter = MockDbAdapter()
    metrics = InMemoryMetricsCollector()
    sleeper = MockSleepRecorder()
    policy = RetryPolicy(max_attempts=3, base_delay=0.1, jitter=False)

    runner = TransactionRunner(
        policy=policy,
        adapter=adapter,
        metrics=metrics,
        sleep_func=sleeper,
    )

    attempts = 0

    async def deadlock_operation():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise MockDatabaseError("deadlock detected between tx A and B", sqlstate="40P01")
        return "deadlock_resolved"

    result = await runner.run(deadlock_operation)

    assert result == "deadlock_resolved"
    assert attempts == 2
    assert adapter.begins == 2
    assert adapter.rollbacks == 1
    assert adapter.commits == 1
    assert metrics.deadlocks_total == 1
    assert metrics.transactions_committed_total == 1


# =========================================================================
# TEST 4 — NON-RETRYABLE ERROR (23505)
# =========================================================================
@pytest.mark.asyncio
async def test_4_non_retryable_error_rolls_back_and_fails_fast():
    """Verify 23505 unique constraint violation is never retried and propagates."""
    adapter = MockDbAdapter()
    metrics = InMemoryMetricsCollector()
    sleeper = MockSleepRecorder()

    runner = TransactionRunner(
        adapter=adapter,
        metrics=metrics,
        sleep_func=sleeper,
    )

    attempts = 0

    async def duplicate_insert():
        nonlocal attempts
        attempts += 1
        raise MockDatabaseError("duplicate key value violates unique constraint", sqlstate="23505")

    with pytest.raises(MockDatabaseError) as exc_info:
        await runner.run(duplicate_insert)

    assert exc_info.value.sqlstate == "23505"
    assert attempts == 1
    assert adapter.begins == 1
    assert adapter.rollbacks == 1
    assert adapter.commits == 0
    assert len(sleeper.delays) == 0
    assert metrics.transactions_failed_total == 1
    assert metrics.transactions_retried_total == 0


# =========================================================================
# TEST 5 — RETRY LIMIT
# =========================================================================
@pytest.mark.asyncio
async def test_5_retry_limit_exhaustion():
    """Verify continuous 40001 stops at max_attempts without infinite loops."""
    adapter = MockDbAdapter()
    metrics = InMemoryMetricsCollector()
    sleeper = MockSleepRecorder()
    policy = RetryPolicy(max_attempts=4, base_delay=0.01)

    runner = TransactionRunner(
        policy=policy,
        adapter=adapter,
        metrics=metrics,
        sleep_func=sleeper,
    )

    attempts = 0

    async def persistent_conflict():
        nonlocal attempts
        attempts += 1
        raise MockDatabaseError(f"serialization error attempt {attempts}", sqlstate="40001")

    with pytest.raises(MockDatabaseError) as exc_info:
        await runner.run(persistent_conflict)

    assert "attempt 4" in str(exc_info.value)
    assert attempts == 4
    assert adapter.begins == 4
    assert adapter.rollbacks == 4
    assert adapter.commits == 0
    assert len(sleeper.delays) == 3  # Delays after attempts 1, 2, 3
    assert metrics.transactions_failed_total == 1
    assert metrics.transactions_retried_total == 3


# =========================================================================
# TEST 6 — BACKOFF & DETERMINISTIC RANDOMNESS
# =========================================================================
def test_6_backoff_growth_and_boundaries():
    """Verify exponential growth, max_delay cap, and non-negative delays."""
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.05,
        max_delay=0.50,
        backoff_factor=2.0,
        jitter=False,
    )

    # Exponential without jitter
    assert calculate_backoff(0, policy) == pytest.approx(0.05)
    assert calculate_backoff(1, policy) == pytest.approx(0.10)
    assert calculate_backoff(2, policy) == pytest.approx(0.20)
    assert calculate_backoff(3, policy) == pytest.approx(0.40)
    assert calculate_backoff(4, policy) == pytest.approx(0.50)  # Capped at max_delay

    # With jitter and deterministic RNG
    jitter_policy = RetryPolicy(base_delay=0.1, max_delay=1.0, jitter=True)

    # Minimum possible RNG (lower bound)
    def min_rng(a: float, b: float) -> float:
        return a

    for att in range(5):
        assert calculate_backoff(att, jitter_policy, random_func=min_rng) == 0.0

    # Maximum possible RNG (upper ceiling)
    def max_rng(a: float, b: float) -> float:
        return b

    assert calculate_backoff(0, jitter_policy, random_func=max_rng) == pytest.approx(0.1)
    assert calculate_backoff(1, jitter_policy, random_func=max_rng) == pytest.approx(0.2)
    assert calculate_backoff(2, jitter_policy, random_func=max_rng) == pytest.approx(0.4)


# =========================================================================
# TEST 7 — DIAGNOSTICS & SSI ACCURACY
# =========================================================================
def test_7_diagnostics_report_and_ssi_distinction():
    """Verify DiagnosticReport generation, lock analysis, and SSI accuracy."""
    err_40001 = MockDatabaseError("could not serialize access", sqlstate="40001")
    report_ssi: DiagnosticReport = (
        DiagnosticCollector(mode=DiagnosticMode.OFF).collect.__wrapped__(  # type: ignore
            error=err_40001,
            attempt=1,
            duration=0.05,
        )
        if hasattr(DiagnosticCollector.collect, "__wrapped__")
        else None
    )  # or via build_diagnostic_report

    from pgshield.postgres.diagnostics import build_diagnostic_report

    # 40001 SSI Report
    report_ssi = build_diagnostic_report(error=err_40001, attempt=1, duration=0.05)
    assert report_ssi.error_category == PostgresErrorCategory.SERIALIZATION_FAILURE
    assert "Serializable Snapshot Isolation (SSI)" in report_ssi.explanation
    assert any("pg_locks cannot fully explain SSI" in w for w in report_ssi.warnings)

    # 40P01 Deadlock Report with locks
    err_40p01 = MockDatabaseError("deadlock detected", sqlstate="40P01")
    locks = [
        LockInfo(
            pid=100,
            lock_type="relation",
            mode="ExclusiveLock",
            granted=True,
            relation_name="accounts",
        ),
        LockInfo(
            pid=101, lock_type="relation", mode="ShareLock", granted=False, relation_name="accounts"
        ),
    ]
    report_deadlock = build_diagnostic_report(error=err_40p01, attempt=2, locks=locks)
    assert report_deadlock.error_category == PostgresErrorCategory.DEADLOCK_DETECTED
    assert report_deadlock.has_blocking is True
    assert report_deadlock.blocking_chains[0].blocked_pid == 101
    assert report_deadlock.blocking_chains[0].blocking_pid == 100


# =========================================================================
# TEST 8 — FASTAPI APPLICATION INTEGRATION
# =========================================================================
class TransferRequest(BaseModel):
    account_from: int
    account_to: int
    amount: float


class TransferResponse(BaseModel):
    status: str
    transferred_amount: float
    attempts: int


@pytest.mark.asyncio
async def test_8_fastapi_endpoint_integration():
    """Verify FastAPI route using PGShield for whole-transaction retries."""
    app = FastAPI(title="PGShield Banking API")
    metrics = InMemoryMetricsCollector()
    logger = TransactionLogger(logging.getLogger("fastapi.banking"), enabled=True)
    sleeper = MockSleepRecorder()
    policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)

    # In-memory database simulation for testing the endpoint
    accounts_db: Dict[int, float] = {1: 500.0, 2: 200.0}
    conflict_countdown = 1  # Will fail with 40001 on attempt 1, then succeed on attempt 2

    runner = TransactionRunner(
        policy=policy,
        metrics=metrics,
        logger=logger,
        sleep_func=sleeper,
    )

    @app.post("/transfer", response_model=TransferResponse)
    async def transfer_funds(req: TransferRequest):
        attempts_recorded = 0

        async def _do_transfer():
            nonlocal attempts_recorded, conflict_countdown
            attempts_recorded += 1

            # Read balances
            if req.account_from not in accounts_db or req.account_to not in accounts_db:
                raise ValueError("Account not found")

            bal_from = accounts_db[req.account_from]
            if bal_from < req.amount:
                raise ValueError("Insufficient funds")

            # Simulate concurrency conflict
            if conflict_countdown > 0:
                conflict_countdown -= 1
                raise MockDatabaseError("could not serialize access", sqlstate="40001")

            # Write updates
            accounts_db[req.account_from] -= req.amount
            accounts_db[req.account_to] += req.amount
            return attempts_recorded

        try:
            total_attempts = await runner.run(
                _do_transfer,
                metadata={"transaction_id": f"tx-transfer-{req.account_from}-{req.account_to}"},
            )
            return TransferResponse(
                status="completed",
                transferred_amount=req.amount,
                attempts=total_attempts,
            )
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # Test HTTP request via httpx AsyncClient
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {"account_from": 1, "account_to": 2, "amount": 150.0}
        response = await client.post("/transfer", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert data["transferred_amount"] == 150.0
        assert data["attempts"] == 2

        # Verify database balances after whole-operation execution
        assert accounts_db[1] == 350.0
        assert accounts_db[2] == 350.0

        # Verify metrics telemetry
        assert metrics.transactions_total == 1
        assert metrics.transactions_committed_total == 1
        assert metrics.transactions_retried_total == 1
        assert metrics.serialization_failures_total == 1
