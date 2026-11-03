# Known Limitations

This document outlines the architectural boundaries and operational constraints of PGShield.

1. **Ambiguous Commit Outcomes:** If a network interruption or connection drop occurs during `commit()`, PGShield does not retry the transaction because PostgreSQL may have already committed the changes. Callers must design idempotent operations or verify state externally when commit outcomes are ambiguous.
2. **External Side Effects:** Any callable passed to PGShield may execute multiple times during transient failure retries. Non-database side effects (such as sending emails, dispatching webhooks, or charging credit cards) should be deferred until after commit using an outbox pattern.
3. **SSI Conflicts in `pg_locks` (SQLSTATE 40001):** PostgreSQL tracks Serializable Snapshot Isolation (SSI) conflicts using in-memory SIREAD predicate locks that never block execution. As a result, serialization failures under `SERIALIZABLE` isolation will generally show no waiting locks in `pg_locks`.
4. **Inferred vs. Authoritative Lock Detection:** `detect_blocking_chains()` infers blocking candidates by evaluating `pg_locks` against PostgreSQL's relation lock compatibility matrix. For authoritative kernel wait-queue verification, use `fetch_locks_with_authoritative()` or `fetch_blocking_pids()`.
5. **No Exactly-Once Semantics:** PGShield guarantees at-least-once execution for retryable blocks up to `max_attempts`. It does not provide distributed two-phase commit or cross-system exactly-once guarantees.
6. **Default Retry Scope:** By default, only transient concurrency conflicts—SQLSTATE `40001` (serialization failure) and `40P01` (deadlock detected)—are retried. Deterministic errors such as unique constraint violations (`23505`) and foreign key errors (`23503`) are never retried.
7. **Session and Connection Boundary Ownership:** PGShield manages its own explicit transaction boundary and refuses SQLAlchemy sessions or asyncpg connections that already have an active transaction. Applications must pass fresh sessions or connections with no pre-existing transaction block.
8. **Diagnostic Query Text Privacy:** Diagnostic collection against `pg_stat_activity` omits SQL query text by default to avoid leaking sensitive data or credentials into logs. Query snippet capture must be explicitly enabled via `include_query_snippet=True`.
9. **No Synchronous Driver Support:** PGShield is engineered strictly for modern asynchronous Python (`asyncio`) runtimes. Synchronous drivers such as standard `psycopg2` or synchronous SQLAlchemy engines are not supported.
10. **Observational Diagnostic Lag:** Diagnostic snapshots captured from `pg_stat_activity` and `pg_locks` represent point-in-time database state after a failure occurs. Short-lived conflicting transactions that commit or rollback immediately may no longer appear in the lock table.

