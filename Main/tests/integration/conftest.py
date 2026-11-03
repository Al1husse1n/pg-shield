"""Pytest fixtures for PGShield real PostgreSQL integration tests."""

import asyncio
import os
from typing import AsyncGenerator
import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


@pytest.fixture
def pg_dsn() -> str:
    """Provide the PostgreSQL test DSN from environment or skip if unavailable."""
    if "PGSHIELD_TEST_DSN" not in os.environ:
        pytest.skip("PGSHIELD_TEST_DSN not set; skipping integration tests")

    dsn = os.environ["PGSHIELD_TEST_DSN"]

    # Verify reachability
    async def _test_reachability() -> None:
        conn = await asyncpg.connect(dsn)
        await conn.close()

    try:
        asyncio.run(asyncio.wait_for(_test_reachability(), timeout=2.0))
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable at {dsn}: {exc}")

    return dsn


@pytest_asyncio.fixture
async def asyncpg_conn(pg_dsn: str) -> AsyncGenerator[asyncpg.Connection, None]:
    """Yield a fresh asyncpg connection against real PostgreSQL and close it."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def sqlalchemy_session(pg_dsn: str) -> AsyncGenerator[AsyncSession, None]:
    """Yield a fresh SQLAlchemy AsyncSession against real PostgreSQL and close it."""
    sa_url = pg_dsn.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url)
    async with AsyncSession(engine) as session:
        try:
            yield session
        finally:
            await session.close()
    await engine.dispose()
