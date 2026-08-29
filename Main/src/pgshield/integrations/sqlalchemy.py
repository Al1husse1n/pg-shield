"""SQLAlchemy AsyncSession integration for PGShield.

This module adapts SQLAlchemy's AsyncSession to PGShield's TransactionAdapter protocol,
enabling automatic whole-transaction retries for FastAPI and async SQLAlchemy applications.
"""

from __future__ import annotations

from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Dict,
    Optional,
    TypeVar,
    Union,
)

from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgshield.postgres.errors import parse_postgres_error
from pgshield.retry.classifier import extract_sqlstate
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.runner import (
    TransactionAdapter,
    TransactionResult,
    TransactionRunner,
)

T = TypeVar("T")
AsyncSessionFactory = Callable[[], Union[AsyncSession, AsyncContextManager[AsyncSession]]]


class SQLAlchemySessionAdapter(TransactionAdapter):
    """Adapts an existing SQLAlchemy AsyncSession to PGShield's TransactionAdapter.

    Handles explicit transaction boundaries on the provided AsyncSession.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize adapter with an AsyncSession.

        Args:
            session: An active SQLAlchemy AsyncSession instance.
        """
        self._session = session

    @property
    def session(self) -> AsyncSession:
        """Return the underlying AsyncSession."""
        return self._session

    async def begin(self) -> None:
        """Begin a new database transaction if not already inside an active transaction."""
        if not self._session.in_transaction():
            await self._session.begin()

    async def commit(self) -> None:
        """Commit the current database transaction."""
        await self._session.commit()

    async def rollback(self) -> None:
        """Roll back the current database transaction and reset session state."""
        try:
            await self._session.rollback()
        except Exception:
            # Prevent rollback failure from masking primary database exceptions
            pass


def extract_sqlalchemy_sqlstate(error: BaseException) -> Optional[str]:
    """Extract a 5-character PostgreSQL SQLSTATE from a SQLAlchemy exception.

    Unwraps SQLAlchemy DBAPIError / SQLAlchemyError to access the driver's native error code.

    Args:
        error: The exception to inspect.

    Returns:
        5-character SQLSTATE code if found, otherwise None.
    """
    if isinstance(error, DBAPIError):
        if error.orig is not None and isinstance(error.orig, BaseException):
            extracted = extract_sqlstate(error.orig)
            if extracted:
                return extracted

    return extract_sqlstate(error)


class SQLAlchemyTransactionRunner(TransactionRunner):
    """Convenience TransactionRunner specialized for SQLAlchemy AsyncSession operations."""

    def __init__(
        self,
        session: Optional[AsyncSession] = None,
        policy: Optional[RetryPolicy] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize with an optional AsyncSession and RetryPolicy.

        Args:
            session: Optional AsyncSession to use as default adapter.
            policy: Optional RetryPolicy configuration.
            **kwargs: Additional parameters forwarded to TransactionRunner.
        """
        adapter = SQLAlchemySessionAdapter(session) if session is not None else None
        super().__init__(policy=policy, adapter=adapter, **kwargs)


async def run_sqlalchemy_transaction(
    session: AsyncSession,
    operation: Callable[[AsyncSession], Awaitable[T]],
    *,
    policy: Optional[RetryPolicy] = None,
    runner: Optional[TransactionRunner] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> T:
    """Execute an async operation with automatic whole-transaction retries on an AsyncSession.

    Guarantees that `operation(session)` is re-executed from the very beginning if a
    retryable PostgreSQL error (40001, 40P01) is encountered.

    Args:
        session: Active SQLAlchemy AsyncSession.
        operation: Async callable receiving the AsyncSession and performing database operations.
        policy: Optional RetryPolicy.
        runner: Optional preconfigured TransactionRunner.
        metadata: Optional metadata attached to TransactionContext.

    Returns:
        The return value of the operation upon successful commit.

    Example:
        >>> async with async_session() as session:
        ...     user = await run_sqlalchemy_transaction(
        ...         session,
        ...         lambda s: s.get(User, user_id),
        ...     )
    """
    adapter = SQLAlchemySessionAdapter(session)
    active_runner = runner or TransactionRunner(policy=policy)

    async def _wrapped_op() -> T:
        return await operation(session)

    return await active_runner.run(_wrapped_op, adapter=adapter, metadata=metadata)
