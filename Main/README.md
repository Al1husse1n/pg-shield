# PGShield: PostgreSQL Concurrency & Retry Toolkit for Python

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/dependencies-zero_core-success.svg)](#installation)
[![Tests](https://img.shields.io/badge/tests-108%20passed%2C%202%20skipped-brightgreen.svg)](#testing)
[![License](https://img.shields.io/badge/license-MIT-informational.svg)](pyproject.toml)

**PGShield** is a production-grade concurrency resilience and retry orchestration toolkit for PostgreSQL applications written in Python.

When building concurrent systems on PostgreSQL, transaction conflicts are not exceptions—they are standard concurrency control signals. PostgreSQL relies on transaction aborts to preserve data integrity under strict isolation levels (`SERIALIZABLE`, `REPEATABLE READ`) and row lock contention. Standard database drivers and ORMs catch these errors and abort the transaction, but leave the complex burden of rollback cleanup, backoff pacing, jitter calculation, and whole-transaction re-execution entirely to application code.

PGShield provides a robust, decoupled layer that automates whole-transaction retries for recoverable concurrency failures, surfaces deep PostgreSQL engine diagnostics (`pg_stat_activity`, `pg_locks`, and blocking chains), redacts sensitive credentials in structured logs, and records comprehensive execution metrics.

---

## Table of Contents

- [What PGShield Is (and Is Not)](#what-pgshield-is-and-is-not)
- [The Core Problem: Why Whole-Transaction Retries Are Required](#the-core-problem-why-whole-transaction-retries-are-required)
- [Architecture](#architecture)
- [Transaction-Level Retry Orchestration](#transaction-level-retry-orchestration)
- [SQLSTATE-Aware Retry Classification](#sqlstate-aware-retry-classification)
- [RetryPolicy Configuration](#retrypolicy-configuration)
- [Exponential Backoff with Full Jitter](#exponential-backoff-with-full-jitter)
- [PostgreSQL Error Normalization](#postgresql-error-normalization)
- [Supported Integrations](#supported-integrations)
  - [SQLAlchemy (`AsyncSession`)](#sqlalchemy-asyncsession)
  - [asyncpg (`asyncpg.Connection`)](#asyncpg-asyncpgconnection)
- [Diagnostics Subsystem & QueryExecutor](#diagnostics-subsystem--queryexecutor)
  - [Application vs. Diagnostic Execution](#application-vs-diagnostic-execution)
  - [The QueryExecutor Protocol](#the-queryexecutor-protocol)
  - [Dedicated Diagnostic Connections](#dedicated-diagnostic-connections)
- [Active Session Inspection (`pg_stat_activity`)](#active-session-inspection-pg_stat_activity)
- [Lock Contention Analysis (`pg_locks`)](#lock-contention-analysis-pg_locks)
- [Diagnostic Reports & Modes](#diagnostic-reports--modes)
- [Structured Logging & Credential Redaction](#structured-logging--credential-redaction)
- [Telemetry & Telemetry Metrics](#telemetry--telemetry-metrics)
- [Transaction Context & Attempt History](#transaction-context--attempt-history)
- [Declarative Decorators (`@transaction`, `@transactional`)](#declarative-decorators-transaction-transactional)
- [FastAPI Integration Example](#fastapi-integration-example)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Complete Real-World Example: Bank Transfer](#complete-real-world-example-bank-transfer)
- [Safety & Correctness Considerations](#safety--correctness-considerations)
- [Performance & Runtime Overhead](#performance--runtime-overhead)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Public API Reference](#public-api-reference)
- [Known Limitations](#known-limitations)
- [Design Philosophy](#design-philosophy)

---

## What PGShield Is (and Is Not)

### What PGShield Is

* **A whole-transaction retry orchestrator:** Ensures that when a retryable PostgreSQL concurrency error occurs, the failed transaction is cleanly rolled back, delayed with randomized backoff, and the entire business logic callable is re-executed from the very beginning.
* **A SQLSTATE classification engine:** Distinguishes transient concurrency aborts (`40001`, `40P01`) from permanent data and constraint errors (`23505`, `23503`, `42P01`).
* **An observability engine for PostgreSQL concurrency:** Inspects PostgreSQL runtime state (`pg_stat_activity`, `pg_locks`, lock blocking graphs) when transactions fail or on demand.
* **A zero-dependency core library:** The core retry and transaction engine requires only Python 3.13+ standard library components (`asyncio`, `contextvars`, `dataclasses`).

### What PGShield Is NOT

* **Not an ORM or Query Builder:** PGShield does not map tables to Python classes, generate SQL, or manage database schemas. You continue using SQLAlchemy, SQLModel, Tortoise, or raw SQL.
* **Not a database driver:** PGShield does not replace `asyncpg`, `psycopg`, or `psycopg2`. It wraps driver connections and sessions.
* **Not an automatic isolation level modifier:** PGShield does not alter your PostgreSQL connection's transaction isolation level. If your workload requires `SERIALIZABLE` or `REPEATABLE READ`, your application or connection pool must configure it explicitly.
* **Not a distributed lock manager:** PGShield does not use Redis, ZooKeeper, or etcd. It relies strictly on PostgreSQL's native MVCC and locking primitives.

---

## The Core Problem: Why Whole-Transaction Retries Are Required

PostgreSQL guarantees ACID transactions through Multi-Version Concurrency Control (MVCC) and lock managers. Under higher isolation levels or concurrent writes, conflicts are resolved by aborting one of the competing transactions:

* **`40001` — `serialization_failure`:** In `SERIALIZABLE` (or `REPEATABLE READ`), PostgreSQL detects that concurrent read/write sets could lead to an inconsistent state (a read-write anti-dependency cycle). PostgreSQL aborts the transaction with: `could not serialize access due to read/write dependencies among transactions`.
* **`40P01` — `deadlock_detected`:** Two or more concurrent transactions hold locks that the other requires, forming a cycle in the lock wait graph. PostgreSQL's deadlock detector fires and aborts one transaction to resolve the cycle.

### Conceptual Timeline of a Serialization Failure

```text
Time    Transaction A (Client 1)              Transaction B (Client 2)
 ───    ────────────────────────              ────────────────────────
 T1     BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
 T2                                           BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
 T3     SELECT balance FROM accounts          
        WHERE id = 1; (reads 100)             
 T4                                           SELECT balance FROM accounts 
                                              WHERE id = 1; (reads 100)
 T5     UPDATE accounts SET balance = 70      
        WHERE id = 1;                         
 T6                                           UPDATE accounts SET balance = 60 
                                              WHERE id = 1;
 T7     COMMIT; (Succeeds)                    
 T8                                           COMMIT;
                                                │
                                                └── PostgreSQL detects SSI conflict!
                                                    Aborts Transaction B:
                                                    ERROR: 40001 serialization_failure
```

### Why Retrying a Single Statement Fails

A common anti-pattern is attempting to catch the database exception around a single SQL query:

```python
# ❌ WRONG: Attempting to retry only the failed query
async def update_balance(session, account_id, amount):
    balance = await session.fetch_balance(account_id)  # Read on attempt 1
    new_balance = balance - amount
    try:
        await session.execute_update(account_id, new_balance)
    except ConcurrencyError:
        # Fails! Transaction is already in an ABORT state in PostgreSQL.
        # Furthermore, 'balance' was read before the conflict and is now stale!
        await session.execute_update(account_id, new_balance)
```

In PostgreSQL:
1. Once any error occurs inside a transaction block, PostgreSQL places the entire transaction into an **aborted state**. Every subsequent query on that connection—including a retried query—fails with `current transaction is aborted, commands ignored until end of transaction block` until an explicit `ROLLBACK` is issued.
2. Even if PostgreSQL permitted statement retries, the snapshot read at `T4` was stale. Retrying only the `UPDATE` statement would overwrite Transaction A's committed changes, corrupting account balances.

### The Correct Response: Whole-Transaction Replay

```text
Transaction Fails (40001 / 40P01)
       │
       ▼
   ROLLBACK  (Resets connection state in PostgreSQL)
       │
       ▼
Backoff + Full Jitter  (Relieves database contention)
       │
       ▼
Re-execute ENTIRE Callable from the beginning
(Reads fresh snapshot -> executes business logic -> writes updates)
       │
       ▼
     COMMIT
```

---

## Architecture

PGShield is built as a set of decoupled, single-responsibility modules:

```mermaid
flowchart TD
    App[Application Business Logic] --> Runner[TransactionRunner / @transaction]
    
    subgraph PGShield Core Orchestration
        Runner --> Ctx[TransactionContext & Attempt History]
        Runner --> Classify[Error Classifier]
        Runner --> Policy[RetryPolicy]
        Runner --> Backoff[Exponential Backoff + Full Jitter]
        Runner --> Log[TransactionLogger - Credential Redaction]
        Runner --> Metrics[MetricsCollector - In-Memory / Protocol]
    end

    subgraph Integration Layer
        Runner --> Adapter{TransactionAdapter}
        Adapter -->|SQLAlchemySessionAdapter| SA[SQLAlchemy AsyncSession]
        Adapter -->|AsyncpgConnectionAdapter| APG[asyncpg.Connection]
    end

    SA --> Driver[(PostgreSQL Server)]
    APG --> Driver

    subgraph Diagnostics Subsystem Optional
        Runner -.->|on failure / sampled| Diag[DiagnosticCollector]
        Diag --> QExec[QueryExecutor / Dedicated Conn]
        QExec --> Activity[pg_stat_activity]
        QExec --> Locks[pg_locks & Blocking Chains]
        Activity --> Report[DiagnosticReport]
        Locks --> Report
    end
```

### Subsystem Overview

| Subsystem | Modules | Core Responsibility |
| :--- | :--- | :--- |
| **`pgshield.retry`** | `policy.py`, `backoff.py`, `classifier.py` | Immutable retry policies, full-jitter exponential backoff math, and SQLSTATE-aware error classification. |
| **`pgshield.transaction`** | `runner.py`, `context.py`, `decorator.py` | Whole-transaction execution lifecycle, `ContextVar`-based runtime attempt tracking, and declarative decorators. |
| **`pgshield.postgres`** | `errors.py`, `activity.py`, `locks.py`, `diagnostics.py` | Driver-agnostic PostgreSQL error normalization, session inspection (`pg_stat_activity`), lock analysis (`pg_locks`), and `DiagnosticReport` synthesis. |
| **`pgshield.instrumentation`** | `logging.py`, `metrics.py` | Structured event logging with automatic password/URL redaction, and thread-safe metrics collection. |
| **`pgshield.integrations`** | `sqlalchemy.py`, `asyncpg.py` | Concrete `TransactionAdapter` implementations for SQLAlchemy `AsyncSession` and `asyncpg.Connection`. |

---

## Transaction-Level Retry Orchestration

The `TransactionRunner` is the central orchestrator in PGShield. It guarantees the following lifecycle invariants:

1. **Explicit Transaction Boundary:** Calls `adapter.begin()` before each execution attempt.
2. **Whole Callable Execution:** Calls `await operation(*args, **kwargs)` from the very first line of code on every attempt.
3. **Clean Commit:** If the callable returns without error, calls `adapter.commit()`.
4. **Immediate Rollback on Any Exception:** If an exception occurs during operation execution or commit, `adapter.rollback()` is executed immediately. If `commit()` raises, the error is immediately surfaced to the caller and NEVER retried due to ambiguous commit outcome. Rollback errors never mask the primary operational failure.
5. **Immediate Cancellation Propagation:** If an `asyncio.CancelledError`, `KeyboardInterrupt`, or `SystemExit` occurs, the transaction is rolled back and the signal is re-raised immediately without retrying.
6. **SQLSTATE Classification:** The exception is inspected and classified against standard PostgreSQL error codes.
7. **Backoff Pacing strictly AFTER Rollback:** Delay occurs strictly *after* the failed transaction has been rolled back in PostgreSQL, freeing held locks before sleeping.
8. **Bounded Attempts:** If the maximum attempts configured in `RetryPolicy` are reached, the original exception is re-raised.

```mermaid
flowchart TD
    Start([Start Transaction]) --> Begin[adapter.begin]
    Begin --> Exec[Execute User Callable]
    Exec --> Success{Succeeded?}
    Success -- Yes --> Commit[adapter.commit]
    Commit --> CommitSuccess{Commit OK?}
    CommitSuccess -- Yes --> Done([Return Result])
    
    Success -- No --> Rollback[adapter.rollback]
    CommitSuccess -- No --> RollbackCommit[adapter.rollback]
    RollbackCommit --> FailCommit[Record Metric & Re-raise Commit Error]
    
    Rollback --> CancelCheck{Cancelled / Exit?}
    CancelCheck -- Yes --> ReRaiseCancelled[Re-raise Immediately]
    CancelCheck -- No --> Classify{Retryable SQLSTATE?}
    
    Classify -- No --> Fail[Record Metric & Re-raise Error]
    Classify -- Yes --> LimitCheck{Attempt < max_attempts?}
    LimitCheck -- No --> Fail
    
    LimitCheck -- Yes --> Backoff[Calculate Jittered Backoff]
    Backoff --> Sleep[asyncio.sleep delay]
    Sleep --> NextAttempt[Increment Attempt Counter]
    NextAttempt --> Begin
```

### Why Python Context Managers Cannot Retry Blocks

Developers frequently ask why PGShield uses callables and decorators rather than an `async with` context manager:

```python
# ❌ IMPOSSIBLE IN STANDARD PYTHON:
async with pgshield.transaction():
    await do_work_1()
    await do_work_2()  # If this raises 40001...
```

Under Python's PEP 343 and PEP 492 specifications, context managers execute their enclosed body **exactly once**. A context manager's `__aexit__` method can catch an exception and suppress it, but **it cannot jump the instruction pointer back to the start of the `async with` block**. 

To replay the sequence from the beginning, the operations must be encapsulated in an async callable passed to `TransactionRunner.run()` or decorated with `@transaction`.

---

## SQLSTATE-Aware Retry Classification

Blindly retrying every database exception is dangerous. Retrying an integrity violation (such as a duplicate primary key) wastes database resources and masks business logic bugs. 

PGShield inspects exceptions using PostgreSQL's standard 5-character **SQLSTATE** codes defined in the SQL standard and PostgreSQL documentation.

### Implemented Retryable Codes

By default, PGShield considers only transient concurrency conflicts retryable:

| SQLSTATE | Error Name | Description | Default Action |
| :--- | :--- | :--- | :--- |
| **`40001`** | `serialization_failure` | Read/write dependency conflict under SSI or snapshot isolation. | **RETRY** with backoff & jitter |
| **`40P01`** | `deadlock_detected` | Deadlock cycle detected in PostgreSQL lock graph. | **RETRY** with backoff & jitter |

### Implemented Non-Retryable Codes (Immediate Fail-Fast)

Deterministic constraint and syntax violations are **never** retried:

| SQLSTATE | Error Name | Reason Not Retried | Default Action |
| :--- | :--- | :--- | :--- |
| **`23505`** | `unique_violation` | Duplicate key violates unique constraint; cannot succeed without data changes. | **FAIL FAST** (Rollback & Raise) |
| **`23503`** | `foreign_key_violation` | Referenced key does not exist; deterministic schema violation. | **FAIL FAST** (Rollback & Raise) |
| **`23514`** | `check_violation` | Row data violates column check constraint. | **FAIL FAST** (Rollback & Raise) |
| **`23502`** | `not_null_violation` | NULL value supplied for NOT NULL column. | **FAIL FAST** (Rollback & Raise) |
| **`42P01`** | `undefined_table` | Table does not exist in schema. | **FAIL FAST** (Rollback & Raise) |

### Direct Classification API

You can use PGShield's classification logic directly:

```python
from pgshield.retry import classify_error, is_retryable, ErrorCategory

try:
    await session.execute(...)
except Exception as exc:
    result = classify_error(exc)
    print(result.retryable)  # bool (True for 40001, 40P01)
    print(result.sqlstate)  # "40001"
    print(result.category)  # ErrorCategory.SERIALIZATION_FAILURE
    print(result.reason)  # "PostgreSQL SQLSTATE 40001 (serialization_failure) is retryable"
```

---

## RetryPolicy Configuration

The `RetryPolicy` class is a frozen, immutable configuration dataclass governing retry attempts, backoff timing, and target SQLSTATE codes.

```python
from pgshield.retry import RetryPolicy

policy = RetryPolicy(
    max_attempts=5,  # Maximum execution attempts (initial + 4 retries)
    base_delay=0.05,  # Initial backoff delay in seconds (50ms)
    max_delay=2.0,  # Maximum backoff ceiling cap in seconds (2s)
    backoff_factor=2.0,  # Exponential multiplier
    jitter=True,  # Enable randomized Full Jitter
    retryable_sqlstates={"40001", "40P01"},  # Target SQLSTATE codes
)
```

### Configuration Options & Validation Rules

| Attribute | Type | Default | Validation Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `max_attempts` | `int` | `5` | Must be an integer `>= 1` | Total execution attempts permitted before permanently raising the final failure. |
| `base_delay` | `float` | `0.05` | Must be `>= 0.0` | Initial delay base (in seconds) applied before the first retry attempt. |
| `max_delay` | `float` | `2.0` | Must be `>= base_delay` | Maximum delay cap (in seconds) for any backoff interval. |
| `backoff_factor`| `float` | `2.0` | Must be `>= 1.0` | Multiplier for exponential delay scaling (`base_delay * (factor ** attempt)`). |
| `jitter` | `bool` | `True` | Boolean | When `True`, applies randomized Full Jitter between `0.0` and the calculated ceiling. |
| `retryable_sqlstates`| `FrozenSet[str]` | `{"40001", "40P01"}` | Normalized to uppercase | Set of 5-character SQLSTATE codes that trigger retries. |

Instances of `RetryPolicy` are strictly immutable (`frozen=True, slots=True`). Any attempt to modify fields post-initialization raises `dataclasses.FrozenInstanceError`.

---

## Exponential Backoff with Full Jitter

When a database experiences high concurrency contention, multiple transactions often fail almost simultaneously. If those transactions immediately retry, or retry on identical fixed timers, they collide again—a phenomenon known as the **thundering herd** problem.

PGShield implements **Full Jitter** exponential backoff (as documented in AWS architecture research):

$$\text{ceiling} = \min(\text{max\_delay}, \text{base\_delay} \times (\text{backoff\_factor}^{\text{attempt}}))$$
$$\text{delay} = \text{random.uniform}(0.0, \text{ceiling}) \quad \text{(if jitter is enabled)}$$

### Without Jitter vs. With Jitter Progression

Suppose three concurrent transactions encounter a serialization conflict at $T = 0$:

```text
WITHOUT JITTER (backoff_factor=2.0, base_delay=0.1s):
All clients calculate identical fixed intervals and collide again:
Client A: ──── 100ms ────► Retry (Collides with B & C!)
Client B: ──── 100ms ────► Retry (Collides with A & C!)
Client C: ──── 100ms ────► Retry (Collides with A & B!)

WITH FULL JITTER (PGShield default):
Delays are uniformly distributed across [0.0, ceiling]:
Client A: ── 42ms ──► Retry (Succeeds)
Client B: ──── 87ms ────► Retry (Succeeds)
Client C: ────── 115ms ──────► Retry (Succeeds)
```

The backoff calculator is mathematically bounded: delay is guaranteed to be non-negative (`>= 0.0`), never exceeds `max_delay`, and guards against Python float exponent overflow on large attempt numbers.

---

## PostgreSQL Error Normalization

Python applications interact with PostgreSQL through various drivers and abstraction layers (`asyncpg`, `psycopg2`, `psycopg3`, `SQLAlchemy`). Each library represents errors differently:

```text
PostgreSQL Server
       │ (Emits error wire protocol with 5-char SQLSTATE)
       ▼
Database Driver (asyncpg.PostgresError / psycopg.Error)
       │ (Exposes .sqlstate or .pgcode)
       ▼
SQLAlchemy Layer (sqlalchemy.exc.DBAPIError / OperationalError)
       │ (Wraps driver exception inside .orig)
       ▼
PGShield Error Normalizer (pgshield.postgres.errors)
       │ (Inspects .sqlstate, .pgcode, .orig, __cause__, __context__)
       ▼
PostgresErrorDetails (Unified, driver-agnostic dataclass)
```

### `PostgresErrorDetails`

The `parse_postgres_error()` function introspects any exception without requiring direct imports of database drivers, returning a normalized `PostgresErrorDetails` dataclass:

```python
from pgshield.postgres import parse_postgres_error

details = parse_postgres_error(error)

print(details.sqlstate)  # e.g. "40001"
print(details.category)  # PostgresErrorCategory.SERIALIZATION_FAILURE
print(details.message)  # Primary error message string
print(details.detail)  # Auxiliary PostgreSQL detail string (if present)
print(details.hint)  # PostgreSQL hint string (if present)
print(details.table_name)  # Table name if relation/constraint error
print(details.column_name)  # Column name if applicable
print(details.constraint_name)  # Constraint name if applicable
print(details.driver_error_type)  # Name of underlying driver exception class
```

---

## Supported Integrations

PGShield provides dedicated, tested adapters for the two primary asynchronous database toolkits in Python: **SQLAlchemy** and **asyncpg**.

### SQLAlchemy (`AsyncSession`)

The `SQLAlchemySessionAdapter` bridges an active `sqlalchemy.ext.asyncio.AsyncSession` to PGShield's `TransactionAdapter` protocol.

* `begin()`: Calls `await session.begin()`. If the session is already in an active transaction (`session.in_transaction()`), raises `RuntimeError`.
* `commit()`: Calls `await session.commit()`.
* `rollback()`: Calls `await session.rollback()`. PGShield does not call `expunge_all()`; pending objects in the caller's session are left as-is after rollback.

#### Helper: `run_sqlalchemy_transaction`

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select, update
from pgshield import run_sqlalchemy_transaction, RetryPolicy

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost:5432/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def debit_account(session: AsyncSession, account_id: int, amount: float):
    # This callable is re-run from the top if a 40001/40P01 occurs
    async def _tx_operation(s: AsyncSession):
        stmt = select(Account).where(Account.id == account_id).with_for_update()
        account = (await s.execute(stmt)).scalar_one()

        if account.balance < amount:
            raise ValueError("Insufficient balance")

        account.balance -= amount
        return account.balance

    async with session_factory() as session:
        return await run_sqlalchemy_transaction(
            session=session,
            operation=_tx_operation,
            policy=RetryPolicy(max_attempts=3, base_delay=0.05),
            metadata={"transaction_id": f"debit-{account_id}"},
        )
```

### asyncpg (`asyncpg.Connection`)

The `AsyncpgConnectionAdapter` integrates directly with raw `asyncpg.Connection` instances.

* `begin()`: Creates and starts an `asyncpg.transaction.Transaction` (`conn.transaction()`). If the connection is already in an active transaction (`conn.is_in_transaction()`), raises `RuntimeError`.
* `commit()`: Commits the active transaction boundary.
* `rollback()`: Rolls back the active transaction boundary.

### Session and connection ownership (v0.1)

* **SQLAlchemy:** PGShield owns the transaction it starts. If your `AsyncSession` is already in a transaction, PGShield raises `RuntimeError`. Wrap your own boundary around a fresh session.
* **asyncpg:** PGShield owns the transaction it starts on the connection you pass. If the connection is already in a transaction, PGShield raises `RuntimeError`. The correct pool pattern is:
  ```python
  async with pool.acquire() as conn:
      await run_asyncpg_transaction(conn, operation)
  ```
  A pool-aware convenience helper is planned for v0.2.

#### Helper: `run_asyncpg_transaction`

```python
import asyncpg
from pgshield import run_asyncpg_transaction, RetryPolicy


async def transfer_credits(
    conn: asyncpg.Connection, sender_id: int, recipient_id: int, credits: int
):
    async def _tx_operation(c: asyncpg.Connection):
        # 1. Deduct sender
        sender_balance = await c.fetchval(
            "UPDATE users SET credits = credits - $1 WHERE id = $2 RETURNING credits",
            credits,
            sender_id,
        )
        if sender_balance < 0:
            raise ValueError("Sender has insufficient credits")

        # 2. Credit recipient
        await c.execute(
            "UPDATE users SET credits = credits + $1 WHERE id = $2", credits, recipient_id
        )
        return sender_balance

    return await run_asyncpg_transaction(
        connection=conn,
        operation=_tx_operation,
        policy=RetryPolicy(max_attempts=5, base_delay=0.02),
        metadata={"transaction_id": f"transfer-{sender_id}-to-{recipient_id}"},
    )
```

---

## Diagnostics Subsystem & QueryExecutor

When a concurrency failure occurs in production, understanding *why* it happened is challenging. PGShield includes a dedicated diagnostic subsystem to capture and analyze PostgreSQL engine state at the moment of failure.

### Application vs. Diagnostic Execution

It is essential to understand the distinction between application transactions and diagnostic queries:

```text
┌──────────────────────────────────────────────────────────┐
│ Application Execution                                   │
│  - Runs business SQL (SELECT, INSERT, UPDATE, DELETE)    │
│  - Executes inside an explicit transaction boundary     │
│  - Fails with 40001 / 40P01 -> Aborts and rolls back    │
└──────────────────────────────────────────────────────────┘
                            vs.
┌──────────────────────────────────────────────────────────┐
│ Diagnostic Execution                                     │
│  - Runs read-only administrative queries                 │
│  - Queries pg_stat_activity and pg_locks                 │
│  - MUST NOT run inside an aborted transaction block      │
└──────────────────────────────────────────────────────────┘
```

### The `QueryExecutor` Protocol

PGShield defines a generic, runtime-checkable protocol for running read-only diagnostic SQL:

```python
from typing import Any, Mapping, Optional, Protocol, Sequence


class QueryExecutor(Protocol):
    async def execute_query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Sequence[Mapping[str, Any]]:
        """Execute a SQL query and return rows as dictionaries or mapping objects."""
        ...
```

#### Concrete `QueryExecutor` Adapters

PGShield provides pre-packaged concrete `QueryExecutor` adapters for both asyncpg and SQLAlchemy:

* **`AsyncpgQueryExecutor`** (`pgshield.integrations.asyncpg` or `pgshield`):
  Wraps an `asyncpg.Connection` and returns plain Python dictionaries:
  ```python
  from pgshield import AsyncpgQueryExecutor

  executor = AsyncpgQueryExecutor(conn)
  rows = await executor.execute_query("SELECT 1 AS num")
  ```

* **`SQLAlchemyQueryExecutor`** (`pgshield.integrations.sqlalchemy` or `pgshield`):
  Wraps a SQLAlchemy `AsyncSession` and returns row mappings:
  ```python
  from pgshield import SQLAlchemyQueryExecutor

  executor = SQLAlchemyQueryExecutor(session)
  rows = await executor.execute_query("SELECT 1 AS num")
  ```

### Dedicated Diagnostic Connections

The recommended production pattern is to provide a dedicated connection (or small diagnostic connection pool) via `executor_override`:

```python
import asyncpg
from pgshield import AsyncpgQueryExecutor, DiagnosticCollector, DiagnosticMode

# Configure collector with dedicated diagnostic connection or pool
diag_conn = await asyncpg.connect("postgresql://...")
executor = AsyncpgQueryExecutor(diag_conn)

diagnostics = DiagnosticCollector(
    mode=DiagnosticMode.ON_FAILURE,
    executor=executor,
)
```

---

## Active Session Inspection (`pg_stat_activity`)

The `postgres/activity.py` module inspects PostgreSQL backend processes. It queries `pg_stat_activity` while excluding the current diagnostic backend session (`pid <> pg_backend_pid()`).

### `SessionActivity` Dataclass

Each active session is parsed into an immutable `SessionActivity` instance:

```python
@dataclass(frozen=True, slots=True)
class SessionActivity:
    pid: int
    database: Optional[str]
    username: Optional[str]
    application_name: Optional[str]
    client_addr: Optional[str]
    state: Optional[str]  # "active", "idle in transaction", "idle"
    query_start: Optional[datetime | str]
    xact_start: Optional[datetime | str]
    wait_event_type: Optional[str]  # "Lock", "IO", "LWLock", "Client"
    wait_event: Optional[str]  # e.g. "relation", "tuple", "ClientRead"
    query_snippet: Optional[str]  # Truncated query (only if explicitly enabled)
```

### Convenient Diagnostic Properties

* `is_waiting_on_lock`: Returns `True` if `wait_event_type == "Lock"`.
* `is_idle_in_transaction`: Returns `True` if `state == "idle in transaction"` (a primary culprit for holding row locks and causing lock queues).

### Query Snippet Security Toggle

To prevent sensitive user data or credentials from leaking into diagnostic outputs, `include_query_snippet` is `False` by default. When explicitly set to `True`, PGShield extracts only the first 120 characters via `LEFT(query, 120)`.

---

## Lock Contention Analysis (`pg_locks` & `pg_blocking_pids`)

The `postgres/locks.py` module parses PostgreSQL's active lock table, joins relation and schema names from `pg_class` and `pg_namespace`, and analyzes lock contention.

### Two Diagnostic Paths for Lock Conflicts

PGShield provides two complementary diagnostic channels for analyzing lock contention:

1. **Inferred (default, works everywhere):**
   `detect_blocking_chains(locks)` pairs ungranted lock requests (`granted=False`) against granted locks (`granted=True`) sharing the identical resource key (`lock_type, relation_name, schema_name, page, tuple, transaction_id`). Crucially, relation lock pairs are filtered through PostgreSQL's official 8-mode relation lock compatibility matrix (`_RELATION_LOCK_COMPATIBILITY`, Table 13.2 in PostgreSQL documentation), eliminating false positives between compatible concurrent locks (such as concurrent `AccessShareLock` reads or concurrent `RowExclusiveLock` writes on separate rows). The resulting `BlockingRelationship.authoritative` attribute is set to `False`.

2. **Authoritative:**
   `fetch_locks_with_authoritative(executor)` and `fetch_blocking_pids(executor, pid=...)` query PostgreSQL's built-in `pg_blocking_pids(pid)` function. This queries the server's authoritative kernel lock wait queues, returning `AuthoritativeBlocking(blocked_pid=..., blocking_pids=[...])` structures indicating precisely which backend PIDs are holding back a waiting transaction.

### Diagnostic Privileges

Reading `pg_locks` and `pg_blocking_pids` requires no privileges beyond the ability to see your own backend; reading all backends requires `pg_monitor` or superuser. PGShield's tests exercise the self-visible path (or the superuser path when running in compose).

### `LockInfo` Dataclass

```python
@dataclass(frozen=True, slots=True)
class LockInfo:
    pid: int
    lock_type: str  # "relation", "transactionid", "tuple", "virtualxid"
    mode: str  # "RowExclusiveLock", "ExclusiveLock", "ShareLock"
    granted: bool  # True if held; False if blocked waiting
    relation_name: Optional[str]  # e.g. "accounts"
    schema_name: Optional[str]  # e.g. "public"
    page: Optional[int]
    tuple: Optional[int]
    transaction_id: Optional[str]
    virtual_transaction: Optional[str]
```

### Blocking Chain Detection (`detect_blocking_chains`)

PGShield matches ungranted lock requests (`granted=False`) against granted locks (`granted=True`) matching the identical resource key (`lock_type, relation_name, schema_name, page, tuple, transaction_id`).

```python
from pgshield.postgres.locks import detect_blocking_chains

blocking_chains = detect_blocking_chains(locks)
for block in blocking_chains:
    print(f"Process {block.blocked_pid} is BLOCKED by Process {block.blocking_pid}")
    print(f"Waiting for {block.waiting_mode}; Holder has {block.holding_mode}")
    print(f"Resource: {block.resource_description} (authoritative={block.authoritative})")
```

### Authoritative Blocking Inspection (`fetch_locks_with_authoritative`)

```python
from pgshield.postgres.locks import fetch_locks_with_authoritative

locks, authoritative_blocking = await fetch_locks_with_authoritative(executor)
for auth in authoritative_blocking:
    print(f"Blocked PID {auth.blocked_pid} is waiting for PIDs: {auth.blocking_pids}")
```

### Crucial Limitation: SSI (`40001`) vs. Heavyweight Locks (`pg_locks`)

> [!WARNING]
> **Do not expect `pg_locks` to explain every `40001` serialization failure.**
>
> pg_locks cannot fully explain SSI serialization failures (SQLSTATE 40001); SIREAD locks are tracked in-memory and may not appear as ordinary lock waits.
>
> PostgreSQL's Serializable Snapshot Isolation (SSI) engine detects read/write anti-dependencies using **SIREAD predicate locks**. Predicate locks are in-memory tracking flags that do not block execution; transactions proceed normally until commit time, where PostgreSQL aborts any transaction participating in a cycle.
>
> Because predicate locks are not blocking locks, `pg_locks` will generally show **no waiting locks** during a pure SSI serialization failure. PGShield explicitly identifies this distinction in its diagnostic explanations ("No blocking relationship was found in pg_locks; this is expected for Serializable Snapshot Isolation, where serialization conflicts are tracked via SIREAD predicate locks and may not appear as ordinary lock waits.") and warnings.

---

## Diagnostic Reports & Modes

The `DiagnosticCollector` provides four operational modes controlling when diagnostic queries run:

| Mode | Enum Value | Behavior | Recommended Environment |
| :--- | :--- | :--- | :--- |
| `OFF` | `DiagnosticMode.OFF` | Zero queries executed; returns `None`. Zero runtime overhead. | High-throughput production (default) |
| `ON_FAILURE` | `DiagnosticMode.ON_FAILURE` | Queries executed only when a transaction fails. | Staging / Debugging production |
| `SAMPLED` | `DiagnosticMode.SAMPLED` | Executed probabilistically according to `sample_rate` (e.g. 5%). | Production telemetry profiling |
| `ALWAYS` | `DiagnosticMode.ALWAYS` | Executed on every attempt (success or failure). | Test suites & local benchmarking |

### `DiagnosticReport` Structure

```python
@dataclass(frozen=True, slots=True)
class DiagnosticReport:
    transaction_id: Optional[str]
    attempt: int
    sqlstate: Optional[str]
    error_category: PostgresErrorCategory
    error_message: str
    duration: float
    error_detail: Optional[str]
    error_hint: Optional[str]
    isolation_level: Optional[str]
    pid: Optional[int]
    active_sessions: List[SessionActivity]
    locks: List[LockInfo]
    blocking_chains: List[BlockingRelationship]
    explanation: str
    warnings: List[str]
    recommendations: List[str]
```

### Automated Recommendations Engine

PGShield inspects the error and lock state to generate targeted guidance:

* **For `40001` (Serialization Failure):** Recommends whole-transaction retries with jitter, keeping read-write sets minimal, and declaring read-only transactions as `READ ONLY`.
* **For `40P01` (Deadlock):** Details the exact blocking chain and recommends acquiring locks in consistent global order across application endpoints.
* **For `23505` (Unique Violation):** Flags the error as non-retryable deterministic business logic and advises against automated retries.

---

## Structured Logging & Credential Redaction

The `TransactionLogger` produces structured, machine-readable logs compatible with Datadog, ELK, Grafana Loki, and CloudWatch.

### Standard Logger Configuration

By default, logs are emitted under the logger hierarchy `pgshield.transaction`:

```python
import logging
from pgshield.instrumentation import TransactionLogger

# Attach standard Python logging handler
logging.basicConfig(level=logging.INFO)

# Initialize logger
tx_logger = TransactionLogger(enabled=True)
```

### Emitted Lifecycle Events

| Event Type | Log Level | Emitted When |
| :--- | :--- | :--- |
| `transaction_started` | `DEBUG` | A new attempt begins execution. |
| `transaction_committed` | `INFO` | An attempt completes and successfully commits. |
| `transaction_rolled_back` | `WARNING` | An exception occurs and rollback is issued. |
| `transaction_retry` | `WARNING` | A retryable error triggers backoff delay and subsequent replay. |
| `transaction_failed` | `ERROR` | A transaction fails permanently (non-retryable or attempts exhausted). |

### Automatic Credential and URL Redaction

PGShield includes recursive sanitization (`sanitize_value`) that executes automatically across all event metadata:

1. **Sensitive Key Redaction:** Any dictionary key matching passwords, tokens, API keys, credentials, or secrets is replaced with `"[REDACTED]"`.
2. **Database DSN Masking:** Connection URLs (e.g. `postgresql://user:secret123@host:5432/db`) are automatically parsed and masked (`postgresql://user:[REDACTED]@host:5432/db`).

Sample JSON log output (`event.to_dict()`):

```json
{
  "event": "transaction_retry",
  "attempt": 1,
  "duration_ms": 42.15,
  "transaction_id": "order-create-9842",
  "sqlstate": "40001",
  "error_category": "serialization_failure",
  "retry_delay_ms": 78.40,
  "error_message": "could not serialize access due to read/write dependencies",
  "metadata": {
    "user_id": 104,
    "api_key": "[REDACTED]"
  }
}
```

---

## Telemetry & Telemetry Metrics

PGShield provides a decoupled metrics protocol (`MetricsCollector`) and a thread-safe, in-memory implementation (`InMemoryMetricsCollector`) that requires zero third-party monitoring dependencies.

### The `MetricsCollector` Protocol

Any monitoring collector (StatsD, Prometheus client, OpenTelemetry SDK) can implement the protocol:

```python
class MetricsCollector(Protocol):
    def record_transaction_started(self, transaction_id: Optional[str] = None) -> None: ...
    def record_transaction_committed(
        self, duration: float, attempts: int, transaction_id: Optional[str] = None
    ) -> None: ...
    def record_transaction_failed(
        self,
        duration: float,
        attempts: int,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None: ...
    def record_transaction_retry(
        self,
        attempt: int,
        delay: float,
        sqlstate: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None: ...
    def record_serialization_failure(self) -> None: ...
    def record_deadlock(self) -> None: ...
```

### Using `InMemoryMetricsCollector`

```python
from pgshield.instrumentation import InMemoryMetricsCollector

metrics = InMemoryMetricsCollector()

# Pass to runner
runner = TransactionRunner(metrics=metrics)

# Inspect snapshot after traffic
snapshot = metrics.snapshot()
print(f"Total Transactions:      {snapshot.transactions_total}")
print(f"Committed:               {snapshot.transactions_committed_total}")
print(f"Failed:                  {snapshot.transactions_failed_total}")
print(f"Retried:                 {snapshot.transactions_retried_total}")
print(f"Serialization Conflicts: {snapshot.serialization_failures_total}")
print(f"Deadlocks Detected:      {snapshot.deadlocks_total}")
print(f"Average Duration:        {snapshot.avg_duration_seconds * 1000:.2f}ms")
print(f"Average Retry Delay:     {snapshot.avg_retry_delay_seconds * 1000:.2f}ms")
```

---

## Transaction Context & Attempt History

During execution, metadata about the active transaction is accessible via Python's standard `contextvars` mechanism.

### Accessing Context Inside Business Logic

You can inspect the live transaction context using `get_current_context()`:

```python
from pgshield.transaction import get_current_context


async def my_operation():
    ctx = get_current_context()
    if ctx is not None:
        print(f"Attempt: {ctx.attempt} of {ctx.max_attempts}")
        print(f"Is Retry: {ctx.is_retry}")
        print(f"Retry Count: {ctx.retry_count}")
        print(f"Total Elapsed Time: {ctx.total_elapsed_time:.3f}s")
        if ctx.last_error:
            print(f"Previous attempt failed with: {ctx.last_error}")
```

### `AttemptRecord`

Every attempt (successful or failed) appends an immutable `AttemptRecord` to `ctx.history`:

* `attempt_number`: 1-based attempt index.
* `start_time`: Monotonic start timestamp in seconds.
* `duration`: Execution time of the attempt in seconds.
* `error`: Exception raised, if any.
* `classification`: The `ClassificationResult` object.
* `backoff_delay`: Delay applied after this attempt (0.0 if not retried).
* `is_success`: `True` if no error occurred.
* `was_retried`: `True` if backoff delay was scheduled for retry.

---

## Declarative Decorators (`@transaction`, `@transactional`)

PGShield provides `@transaction` (and its alias `@transactional`) to wrap async functions declaratively.

```python
from pgshield import transaction


# Basic usage (defaults to RetryPolicy())
@transaction
async def get_user_profile(user_id: int): ...


# Customized policy overrides
@transaction(max_attempts=3, base_delay=0.1, max_delay=1.0, jitter=True)
async def update_user_balance(user_id: int, new_balance: float): ...
```

### Injecting Dependencies into Decorators

You can inject pre-configured components (such as adapters, loggers, or runners) directly into the decorator:

```python
from pgshield import transaction, TransactionRunner, RetryPolicy

custom_runner = TransactionRunner(
    policy=RetryPolicy(max_attempts=4, base_delay=0.02),
    logger=my_logger,
    metrics=my_metrics,
)


@transaction(runner=custom_runner)
async def execute_order(order_id: str): ...
```

The decorator preserves the original function's signature and docstrings via `functools.wraps`, and attaches the runner instance as `.__pgshield_runner__`.

---

## FastAPI Integration Example

In FastAPI applications, database sessions are typically managed via dependency injection. Here is a production pattern integrating PGShield, SQLAlchemy `AsyncSession`, and FastAPI routes:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select

from pgshield import (
    RetryPolicy,
    TransactionRunner,
    SQLAlchemySessionAdapter,
    InMemoryMetricsCollector,
    TransactionLogger,
    run_sqlalchemy_transaction,
)

# 1. Configure PostgreSQL Engine with SERIALIZABLE isolation
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/banking_db"
engine = create_async_engine(
    DATABASE_URL,
    isolation_level="SERIALIZABLE",  # Explicitly enable serializable snapshot isolation
    pool_size=20,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# 2. Shared PGShield Telemetry
metrics = InMemoryMetricsCollector()
logger = TransactionLogger(enabled=True)
retry_policy = RetryPolicy(max_attempts=3, base_delay=0.05, max_delay=0.5)

app = FastAPI(title="PGShield Banking API")


async def get_db_session() -> AsyncSession:
    async with session_factory() as session:
        yield session


class TransferRequest(BaseModel):
    account_from: int
    account_to: int
    amount: float


@app.post("/transfer")
async def transfer_money(
    req: TransferRequest,
    session: AsyncSession = Depends(get_db_session),
):
    # Encapsulate the full transfer logic in a transactional callable
    async def _do_transfer(s: AsyncSession):
        # 1. Read sender
        res_from = await s.execute(select(Account).where(Account.id == req.account_from))
        acc_from = res_from.scalar_one_or_none()
        if not acc_from or acc_from.balance < req.amount:
            raise ValueError("Insufficient funds or invalid sender account")

        # 2. Read recipient
        res_to = await s.execute(select(Account).where(Account.id == req.account_to))
        acc_to = res_to.scalar_one_or_none()
        if not acc_to:
            raise ValueError("Invalid recipient account")

        # 3. Update balances
        acc_from.balance -= req.amount
        acc_to.balance += req.amount
        return {"from_balance": acc_from.balance, "to_balance": acc_to.balance}

    try:
        result = await run_sqlalchemy_transaction(
            session=session,
            operation=_do_transfer,
            policy=retry_policy,
            metadata={"transaction_id": f"tx-{req.account_from}-{req.account_to}"},
        )
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
```

> [!IMPORTANT]
> **PostgreSQL Isolation Level Requirement:**
> To trigger and test serialization failures (`40001`), PostgreSQL must run in `SERIALIZABLE` (or `REPEATABLE READ`) mode. By default, PostgreSQL connections use `READ COMMITTED`, which uses statement-level snapshots and avoids serialization conflicts (though deadlocks `40P01` can still occur). Configure `isolation_level="SERIALIZABLE"` on your SQLAlchemy engine or asyncpg connection.

---

## Installation

PGShield is written for modern Python and has **zero required external dependencies** for its core library.

### Python Version Support
* **Python 3.13+** is required (as configured in `pyproject.toml`).

### Installing via Pip / Poetry

```bash
# Core package (zero external dependencies)
pip install pgshield

# Or using poetry
poetry add pgshield
```

### Optional / Development Dependencies

PGShield provides optional integration support for SQLAlchemy, asyncpg, and FastAPI:

```toml
[dependency-groups]
dev = [
    "pytest (>=9.1.1,<10.0.0)",
    "pytest-asyncio (>=1.4.0,<2.0.0)",
    "sqlalchemy (>=2.0.52,<3.0.0)",
    "asyncpg (>=0.31.0,<0.32.0)",
    "fastapi (>=0.141.1,<0.142.0)",
    "httpx (>=0.28.1,<0.29.0)"
]
```

---

## Quick Start

Here is a complete, runnable example using PGShield with `asyncpg` in under 60 seconds:

```python
import asyncio
import asyncpg
from pgshield import run_asyncpg_transaction, RetryPolicy


async def main():
    conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/postgres")

    # Setup temporary table
    await conn.execute("CREATE TEMP TABLE counters (id INT PRIMARY KEY, val INT);")
    await conn.execute("INSERT INTO counters VALUES (1, 0);")

    # Define transactional business logic
    async def increment_counter(c: asyncpg.Connection):
        current_val = await c.fetchval("SELECT val FROM counters WHERE id = 1")
        new_val = current_val + 1
        await c.execute("UPDATE counters SET val = $1 WHERE id = 1", new_val)
        return new_val

    # Execute with automatic retries on 40001 / 40P01
    updated_val = await run_asyncpg_transaction(
        connection=conn,
        operation=increment_counter,
        policy=RetryPolicy(max_attempts=3, base_delay=0.05),
    )

    print(f"Successfully updated counter to: {updated_val}")
    await conn.close()


asyncio.run(main())
```

---

## Complete Real-World Example: Bank Transfer

This realistic example illustrates how PGShield coordinates the complete retry lifecycle when handling a concurrency conflict during a balance transfer between two accounts:

```python
import asyncio
import asyncpg
from pgshield import (
    TransactionRunner,
    AsyncpgConnectionAdapter,
    RetryPolicy,
    TransactionLogger,
    InMemoryMetricsCollector,
)


async def execute_transfer(
    conn: asyncpg.Connection,
    sender: int,
    recipient: int,
    amount: float,
):
    # Setup telemetry and policy
    logger = TransactionLogger(enabled=True)
    metrics = InMemoryMetricsCollector()
    policy = RetryPolicy(max_attempts=4, base_delay=0.05, max_delay=1.0, jitter=True)

    adapter = AsyncpgConnectionAdapter(conn)
    runner = TransactionRunner(
        policy=policy,
        adapter=adapter,
        logger=logger,
        metrics=metrics,
    )

    async def transfer_operation():
        # 1. Check sender balance
        sender_bal = await conn.fetchval(
            "SELECT balance FROM bank_accounts WHERE account_id = $1", sender
        )
        if sender_bal is None or sender_bal < amount:
            raise ValueError("Insufficient balance")

        # 2. Debit sender
        await conn.execute(
            "UPDATE bank_accounts SET balance = balance - $1 WHERE account_id = $2",
            amount,
            sender,
        )

        # 3. Credit recipient
        await conn.execute(
            "UPDATE bank_accounts SET balance = balance + $1 WHERE account_id = $2",
            amount,
            recipient,
        )

        return {"sender_new_balance": sender_bal - amount}

    # Execute with full result metadata
    result = await runner.run_with_result(
        transfer_operation,
        metadata={"transaction_id": f"tx-transfer-{sender}->{recipient}"},
    )

    print(f"Result: {result.value}")
    print(f"Total Attempts: {result.total_attempts}")
    print(f"Was Retried: {result.retried}")
    print(f"Total Duration: {result.total_duration * 1000:.2f}ms")
```

### Behind the Scenes During Execution:

1. **Attempt 1 Begins:** `adapter.begin()` starts a new transaction.
2. **Conflict Occurs:** A concurrent transaction updates `bank_accounts`, raising `40001 (serialization_failure)`.
3. **Rollback:** PGShield immediately calls `adapter.rollback()`.
4. **Classification:** PGShield inspects the error and identifies `40001` as a retryable concurrency conflict.
5. **Backoff Delay:** PGShield calculates a jittered delay (e.g. 38.4ms) and awaits `asyncio.sleep()`.
6. **Attempt 2 Begins:** `adapter.begin()` starts a brand-new transaction.
7. **Re-execution:** `transfer_operation` reads the *new*, updated balance from PostgreSQL.
8. **Commit:** `adapter.commit()` succeeds.
9. **Metrics & Logging:** Success event emitted; `transactions_retried_total` and `serialization_failures_total` incremented.

---

## Safety & Correctness Considerations

When architecting systems around transaction retries, observe these critical software engineering principles:

### 1. Whole Callable Execution
The entire callable is executed again from line 1 on every attempt. Never assume that state accumulated in local Python variables during attempt 1 will be reset automatically—keep the callable self-contained.

### 2. External Side Effects Must Be Handled Carefully
**DO NOT make non-idempotent network calls or external mutations inside a retried callable:**

```python
# ❌ DANGEROUS: Credit card could be charged multiple times!
async def checkout():
    await stripe_client.charge(amount=100)  # External side effect!
    await db.save_order()  # If this triggers 40001, charge repeats!
```

To handle side effects safely:
* **Move external calls outside the transaction:** Execute database writes first, commit, and then call external services.
* **Use the Transactional Outbox Pattern:** Insert an outbox record into PostgreSQL within the transaction, commit, and let an asynchronous worker process the side effect.
* **Make external operations idempotent:** Use idempotency keys with external APIs.

### 3. Keep Transactions Short
Long-running transactions hold locks longer and broaden their read/write sets, exponentially increasing the probability of `40001` and `40P01` aborts. Do not perform heavy CPU computation, image processing, or third-party HTTP requests inside database transaction boundaries.

### 4. Bounded Retries
Never configure `max_attempts` to infinity. Unbounded retries in an overloaded database exacerbate server load and lock queues. Choose a realistic limit (3 to 5 attempts) and allow unresolvable conflicts to propagate to the application error handler.

### 5. Ambiguous Commit Outcomes
PGShield does NOT retry transactions whose `commit()` call raised an exception. When a network disconnection, socket timeout, or driver error occurs during commit, the server may have successfully committed the transaction before the connection was severed. Retrying such a transaction could execute non-idempotent operations multiple times and cause data corruption. Callers must treat commit failures as ambiguous and design application-level idempotency accordingly.

---

## Performance & Runtime Overhead

PGShield is designed for high-performance, asynchronous services:

1. **Zero Overhead for Successful Transactions:** When transactions succeed on the first attempt, overhead is limited to a few nanoseconds of Python method dispatch and timestamp recording.
2. **Zero Diagnostic Cost in Default Mode:** By default (`DiagnosticMode.OFF`), no queries to `pg_stat_activity` or `pg_locks` are executed.
3. **Lock Release Before Sleep:** Backoff sleep occurs strictly *after* `adapter.rollback()` has completed. A failing transaction will never hold database locks while waiting on its backoff timer.
4. **Sampling Support:** The `DiagnosticMode.SAMPLED` mode allows teams to capture diagnostic telemetry on a small percentage of transactions without degrading database performance.

---

## Project Structure

The PGShield repository structure is organized as follows:

```text
Main/
├── pyproject.toml                     # Poetry project config (Python >= 3.13)
├── poetry.lock                        # Pinned lockfile
├── src/
│   └── pgshield/
│       ├── __init__.py                # Top-level public API exports
│       ├── retry/                     # Retry subsystem
│       │   ├── policy.py              # RetryPolicy configuration dataclass
│       │   ├── backoff.py             # Full Jitter exponential backoff math
│       │   └── classifier.py          # SQLSTATE extraction & error classification
│       ├── transaction/               # Transaction execution layer
│       │   ├── runner.py              # TransactionRunner & TransactionAdapter protocols
│       │   ├── context.py             # TransactionContext & AttemptRecord (ContextVar)
│       │   └── decorator.py           # @transaction & @transactional decorators
│       ├── postgres/                  # PostgreSQL engine diagnostics
│       │   ├── errors.py              # PostgresErrorDetails & error normalization
│       │   ├── activity.py            # pg_stat_activity & QueryExecutor protocol
│       │   ├── locks.py               # pg_locks & detect_blocking_chains analysis
│       │   └── diagnostics.py         # DiagnosticReport, DiagnosticCollector & modes
│       ├── instrumentation/           # Telemetry & observability
│       │   ├── logging.py             # TransactionLogger & credential redaction
│       │   └── metrics.py             # MetricsCollector & InMemoryMetricsCollector
│       └── integrations/              # Database library adapters
│           ├── sqlalchemy.py          # SQLAlchemy AsyncSession adapter & runner
│           └── asyncpg.py             # asyncpg Connection adapter & runner
└── tests/
    ├── unit/                          # Isolated unit tests (14 modules)
    │   ├── test_policy.py             # RetryPolicy validation & immutability
    │   ├── test_backoff.py            # Backoff growth, caps, and jitter bounds
    │   ├── test_classifier.py         # SQLSTATE classification & custom extractors
    │   ├── test_transaction.py        # Lifecycle, rollbacks, commits, and replay
    │   ├── test_decorator.py          # @transaction and @transactional decorator tests
    │   ├── test_postgres_errors.py    # Driver unwrapping & error normalization
    │   ├── test_postgres_activity.py  # Session parsing & query construction
    │   ├── test_postgres_locks.py     # Lock analysis & blocking chain detection
    │   ├── test_postgres_diagnostics.py # Diagnostic reports & SSI explanations
    │   ├── test_logging.py            # Event logging & password/URL sanitization
    │   ├── test_metrics.py            # Metrics aggregation & snapshots
    │   ├── test_runner_instrumentation.py # Telemetry emission from TransactionRunner
    │   ├── test_sqlalchemy_integration.py # SQLAlchemy AsyncSession integration tests
    │   └── test_asyncpg_integration.py    # asyncpg Connection integration tests
    └── integration/                   # End-to-end integration tests
        ├── test_end_to_end.py         # Full end-to-end suite (FastAPI, 40001, 40P01)
        └── test_postgres_concurrency.py # Real PostgreSQL live database tests (optional)
```

---

## Testing

PGShield features an extensive test suite validating all lifecycle states, retry bounds, mathematical distributions, and integration adapters.

### Running Tests

Run the test suite using Poetry and pytest:

```bash
poetry run pytest
```

### Running integration tests against a real PostgreSQL

```bash
docker compose up -d --wait
export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
pytest -m integration
```

The healthcheck in `docker compose` uses `pg_isready` with `--wait` so tests don't race a half-started DB.

#### Continuous integration

CI runs lint (advisory in Phase 5), unit tests without database dependencies, integration tests against a `services: postgres` container, and a compose verification job that runs `docker compose up -d --wait`. If the compose verification job is red, the compose file is broken; if the `services:` job is red, the code is broken. This separation ensures container orchestration defects are diagnosed independently from database driver logic.

### Verified Test Results

```text
============================= test session starts =============================
platform win32 -- Python 3.13.0, pytest-9.1.1, pluggy-1.6.0
collected 110 items

tests\integration\test_end_to_end.py ........                            [  7%]
tests\integration\test_postgres_concurrency.py ss                        [  9%]
tests\unit\test_asyncpg_integration.py ....                              [ 12%]
tests\unit\test_backoff.py ........                                      [ 20%]
tests\unit\test_classifier.py ..................                         [ 36%]
tests\unit\test_decorator.py .....                                       [ 40%]
tests\unit\test_logging.py ..........                                    [ 50%]
tests\unit\test_metrics.py ..                                            [ 51%]
tests\unit\test_policy.py ........                                       [ 59%]
tests\unit\test_postgres_activity.py ....                                [ 62%]
tests\unit\test_postgres_diagnostics.py .....                            [ 67%]
tests\unit\test_postgres_errors.py .........                             [ 75%]
tests\unit\test_postgres_locks.py .......                                [ 81%]
tests\unit\test_runner_instrumentation.py .....                          [ 86%]
tests\unit\test_sqlalchemy_integration.py .....                          [ 90%]
tests\unit\test_transaction.py ..........                                [100%]

======================= 108 passed, 2 skipped in 3.02s ========================
```

*(Note: The 2 skipped tests in `test_postgres_concurrency.py` dynamically verify whether a live PostgreSQL server is reachable at `localhost:5432`; if not present, they skip gracefully without failing the build).*

---

## Public API Reference

All primary symbols are exported at the root `pgshield` package level:

```python
import pgshield
```

### Core Symbols Exported in `pgshield.__all__`

| Symbol | Category | Description |
| :--- | :--- | :--- |
| `RetryPolicy` | Retry | Immutable configuration for retries, delays, and SQLSTATEs. |
| `calculate_backoff` | Retry | Calculates exponential backoff with full jitter for an attempt. |
| `classify_error` | Retry | Inspects an exception and returns a structured `ClassificationResult`. |
| `is_retryable` | Retry | Convenience boolean check for error retryability. |
| `extract_sqlstate` | Retry | Extracts a 5-character SQLSTATE string across diverse exception types. |
| `ClassificationResult`| Retry | Dataclass storing retry decision, SQLSTATE, category, and reason. |
| `ErrorCategory` | Retry | Enum for categorized errors (`SERIALIZATION_FAILURE`, `DEADLOCK_DETECTED`, etc.). |
| `TransactionRunner` | Transaction | Central orchestrator executing whole-transaction retries. |
| `TransactionAdapter` | Transaction | Protocol for transaction boundaries (`begin`, `commit`, `rollback`). |
| `NullTransactionAdapter`| Transaction | No-op adapter for transactions managed internally. |
| `TransactionResult` | Transaction | Return container storing value, attempts, duration, and history. |
| `TransactionContext`| Transaction | Live execution metadata available via `get_current_context()`. |
| `AttemptRecord` | Transaction | Immutable historical record of a single transaction attempt. |
| `get_current_context`| Transaction | Retrieves active `TransactionContext` from `contextvars`. |
| `transaction` | Transaction | Decorator wrapping async functions with automatic retries. |
| `transactional` | Transaction | Semantic alias for `transaction`. |
| `PostgresErrorDetails`| PostgreSQL | Normalized dataclass with SQLSTATE, hint, detail, table, and column. |
| `PostgresErrorCategory`| PostgreSQL | Categorization of PostgreSQL errors for diagnostics. |
| `parse_postgres_error`| PostgreSQL | Normalizes driver and ORM exceptions into `PostgresErrorDetails`. |
| `SessionActivity` | PostgreSQL | Dataclass representing a session snapshot from `pg_stat_activity`. |
| `LockInfo` | PostgreSQL | Dataclass representing a lock snapshot from `pg_locks`. |
| `DiagnosticReport` | PostgreSQL | Structured diagnostic report with blocking analysis and recommendations. |
| `DiagnosticMode` | PostgreSQL | Enum controlling diagnostic execution (`OFF`, `ON_FAILURE`, `SAMPLED`, `ALWAYS`). |
| `DiagnosticCollector`| PostgreSQL | Engine for executing diagnostic queries and building reports. |
| `build_diagnostic_report`| PostgreSQL | Factory function assembling a `DiagnosticReport`. |
| `TransactionLogger` | Instrumentation | Structured event logger for transaction lifecycles. |
| `LoggedTransactionEvent`| Instrumentation | Dataclass representing an emitted logging event. |
| `TransactionEventType`| Instrumentation | Enum of lifecycle events (`started`, `committed`, `retry`, etc.). |
| `sanitize_value` | Instrumentation | Recursive redactor for sensitive keys and connection URLs. |
| `MetricsCollector` | Instrumentation | Protocol for recording transaction metrics. |
| `InMemoryMetricsCollector`| Instrumentation | Thread-safe, in-memory implementation of `MetricsCollector`. |
| `NullMetricsCollector`| Instrumentation | Zero-overhead, no-op metrics collector. |
| `MetricsSnapshot` | Instrumentation | Immutable snapshot of aggregated transaction telemetry. |
| `SQLAlchemySessionAdapter`| Integration | Adapts SQLAlchemy `AsyncSession` to `TransactionAdapter`. |
| `SQLAlchemyTransactionRunner`| Integration | Specialized `TransactionRunner` for SQLAlchemy. |
| `run_sqlalchemy_transaction`| Integration | Helper executing a callable with retries on an `AsyncSession`. |
| `extract_sqlalchemy_sqlstate`| Integration | Extracts SQLSTATE from SQLAlchemy `DBAPIError.orig`. |
| `AsyncpgConnectionAdapter`| Integration | Adapts `asyncpg.Connection` to `TransactionAdapter`. |
| `AsyncpgTransactionRunner`| Integration | Specialized `TransactionRunner` for asyncpg. |
| `run_asyncpg_transaction`| Integration | Helper executing a callable with retries on an `asyncpg.Connection`. |
| `extract_asyncpg_sqlstate`| Integration | Extracts SQLSTATE from `asyncpg.PostgresError`. |
| `AsyncpgQueryExecutor`| Integration | Concrete `QueryExecutor` adapter for `asyncpg.Connection`. |
| `SQLAlchemyQueryExecutor`| Integration | Concrete `QueryExecutor` adapter for SQLAlchemy `AsyncSession`. |
| `AuthoritativeBlocking`| PostgreSQL | Dataclass representing authoritative blocking PIDs from `pg_blocking_pids`. |
| `fetch_blocking_pids` | PostgreSQL | Direct inspection of `pg_blocking_pids` for a given backend PID. |
| `fetch_locks_with_authoritative` | PostgreSQL | Queries `pg_locks` and authoritative blocking PIDs simultaneously. |

---

## Known Limitations

To maintain engineering transparency, note the following architectural realities (see [docs/limitations.md](docs/limitations.md) for the complete 10-point specification):

1. **SSI Conflicts Do Not Appear in `pg_locks`:** PostgreSQL's SSI (`40001`) conflicts are tracked via SIREAD predicate locks in private kernel memory structures. They do not block queries and will generally not show up as waiting locks in `pg_locks`.
2. **Observational Diagnostics:** Diagnostic queries against `pg_stat_activity` and `pg_locks` represent a momentary snapshot after the transaction failed. If conflicting concurrent transactions have already committed or disconnected, their locks will no longer appear in the diagnostic output.
3. **Separate Connection for Diagnostics:** Applications should provide a dedicated connection or pool to `DiagnosticCollector` via concrete adapters (`AsyncpgQueryExecutor`, `SQLAlchemyQueryExecutor`). PGShield intentionally avoids running diagnostic SQL on unverified application connections that may be in aborted transaction states.
4. **Idempotency Requirement:** Callables passed to `TransactionRunner` must be safe to execute multiple times. If your function contains uncommitted local state mutations or external API calls, those actions will repeat on retry.
5. **No Synchronous Driver Support:** PGShield is written exclusively for asynchronous Python (`asyncio`). Synchronous DBAPI drivers (`psycopg2` sync, synchronous SQLAlchemy sessions) are not supported.

---

## Developer Experience

PGShield is engineered for a predictable developer experience and strict typing:

* **Strict Type Safety:** Type hints are strictly enforced across the entire library with `mypy` in strict mode. The package includes a `py.typed` marker (PEP 561) enabling downstream type checkers to validate PGShield invocations without untyped boundary errors.
* **Architectural Boundaries:** A comprehensive 10-point review of operational constraints, ambiguous commit behaviors, and external side effects is documented in [docs/limitations.md](docs/limitations.md).
* **Runnable Examples:** Self-contained example scripts demonstrating atomic balance transfers are available in the `examples/` directory:
  ```bash
  export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
  python examples/sqlalchemy_transfer.py
  python examples/asyncpg_transfer.py
  ```

---

## Design Philosophy

The architecture of PGShield is guided by four core principles:

1. **PostgreSQL handles concurrency control.** PostgreSQL's MVCC and lock managers are world-class. PGShield does not attempt to predict conflicts, intercept locks, or reinvent concurrency control outside the database.
2. **PGShield handles retry orchestration.** Recovering from transient conflicts requires careful rollback management, jittered backoff, and full callable replay. PGShield handles this complexity cleanly and reliably.
3. **The application owns business logic.** Transactions remain regular Python functions. PGShield wraps operations without intruding on domain models or business rules.
4. **Diagnostics explain what the database was doing.** When transactions fail, developers should not have to guess. PGShield inspects database engine internals and translates low-level lock contention into actionable engineering recommendations.

---

## License

PGShield is open-source software licensed under the **MIT License**. See `pyproject.toml` for author and package metadata.