"""asyncpg driver integration for PGShield.

This module adapts asyncpg connections and pools to PGShield's TransactionAdapter protocol,
enabling automatic whole-transaction retries for async Python applications using asyncpg directly.
"""

from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import asyncpg
from asyncpg.transaction import Transaction

from pgshield.retry.classifier import extract_sqlstate
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.runner import (
    TransactionAdapter,
    TransactionRunner,
)

T = TypeVar("T")


class AsyncpgConnectionAdapter(TransactionAdapter):
    """Adapts an asyncpg.Connection to PGShield's TransactionAdapter protocol.

    Manages transaction lifecycle by creating, starting, committing, and rolling back
    asyncpg.transaction.Transaction objects for each attempt.
    """

    def __init__(self, connection: asyncpg.Connection) -> None:
        """Initialize adapter with an active asyncpg Connection.

        Args:
            connection: An active asyncpg.Connection instance.
        """
        self._connection = connection
        self._current_transaction: Optional[Transaction] = None

    @property
    def connection(self) -> asyncpg.Connection:
        """Return the underlying asyncpg.Connection."""
        return self._connection

    async def begin(self) -> None:
        """Create and start a new transaction boundary on the connection.

        Refuses connections that are already inside an active transaction.
        """
        if self._connection.is_in_transaction():
            raise RuntimeError(
                "PGShield cannot begin a transaction on an asyncpg connection "
                "that is already in a transaction. Acquire a fresh connection from your pool "
                "for each retryable operation, or wrap your own boundary."
            )
        if self._current_transaction is not None:
            await self.rollback()
        self._current_transaction = self._connection.transaction()
        await self._current_transaction.start()

    async def commit(self) -> None:
        """Commit current transaction boundary (PGShield only owns what it started)."""
        if self._current_transaction is not None:
            await self._current_transaction.commit()
            self._current_transaction = None

    async def rollback(self) -> None:
        """Roll back current transaction boundary (PGShield only owns what it started)."""
        if self._current_transaction is not None:
            try:
                await self._current_transaction.rollback()
            except Exception:
                pass
            finally:
                self._current_transaction = None


class AsyncpgQueryExecutor:
    """Concrete QueryExecutor adapter for asyncpg connections.

    Executes read-only diagnostic queries using an active asyncpg.Connection
    and returns results as a sequence of plain Python dictionaries.
    """

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def execute_query(
        self, sql: str, params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None
    ) -> Sequence[Mapping[str, Any]]:
        """Execute a read query and return rows as a list of plain dicts."""
        if isinstance(params, Mapping):
            args: Sequence[Any] = tuple(params.values())
        else:
            args = params or []
        records = await self._connection.fetch(sql, *args)
        return [dict(record) for record in records]


def extract_asyncpg_sqlstate(error: BaseException) -> Optional[str]:
    """Extract a 5-character PostgreSQL SQLSTATE from an asyncpg exception.

    Args:
        error: The exception to inspect.

    Returns:
        5-character SQLSTATE code if found, otherwise None.
    """
    if isinstance(error, asyncpg.PostgresError):
        code = getattr(error, "sqlstate", None)
        if isinstance(code, str) and len(code) == 5:
            return code.upper()

    return extract_sqlstate(error)


class AsyncpgTransactionRunner(TransactionRunner):
    """Convenience TransactionRunner specialized for asyncpg.Connection operations."""

    def __init__(
        self,
        connection: Optional[asyncpg.Connection] = None,
        policy: Optional[RetryPolicy] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize with an optional asyncpg Connection and RetryPolicy.

        Args:
            connection: Optional asyncpg.Connection to use as default adapter.
            policy: Optional RetryPolicy configuration.
            **kwargs: Additional parameters forwarded to TransactionRunner.
        """
        adapter = AsyncpgConnectionAdapter(connection) if connection is not None else None
        super().__init__(policy=policy, adapter=adapter, **kwargs)


async def run_asyncpg_transaction(
    connection: asyncpg.Connection,
    operation: Callable[[asyncpg.Connection], Awaitable[T]],
    *,
    policy: Optional[RetryPolicy] = None,
    runner: Optional[TransactionRunner] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> T:
    """Execute an async operation with automatic whole-transaction retries on an asyncpg Connection.

    Guarantees that `operation(connection)` is re-executed from the very beginning if a
    retryable PostgreSQL error (40001, 40P01) is encountered.

    Args:
        connection: Active asyncpg.Connection.
        operation: Async callable receiving the connection and performing database statements.
        policy: Optional RetryPolicy.
        runner: Optional preconfigured TransactionRunner.
        metadata: Optional metadata attached to TransactionContext.

    Returns:
        The return value of the operation upon successful commit.

    Example:
        >>> user = await run_asyncpg_transaction(
        ...     conn,
        ...     lambda c: c.fetchrow("SELECT * FROM users WHERE id = $1", user_id),
        ... )
    """
    adapter = AsyncpgConnectionAdapter(connection)
    active_runner = runner or TransactionRunner(policy=policy)

    async def _wrapped_op() -> T:
        return await operation(connection)

    return await active_runner.run(_wrapped_op, adapter=adapter, metadata=metadata)
