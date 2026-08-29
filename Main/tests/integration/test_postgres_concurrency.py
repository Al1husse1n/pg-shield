"""Integration tests against real PostgreSQL instance for PGShield.

These tests execute against a real PostgreSQL database when available.
If no PostgreSQL database is reachable at the configured URL, they skip gracefully.
"""

import asyncio
import os
import pytest

from pgshield.integrations.asyncpg import run_asyncpg_transaction
from pgshield.integrations.sqlalchemy import (
    SQLAlchemySessionAdapter,
    run_sqlalchemy_transaction,
)
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.runner import TransactionRunner

TEST_DB_URL = os.environ.get(
    "PGSHIELD_TEST_DB_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)


async def is_postgres_reachable() -> bool:
    """Check if the test PostgreSQL instance is reachable."""
    try:
        import asyncpg

        conn = await asyncio.wait_for(asyncpg.connect(TEST_DB_URL), timeout=1.0)
        await conn.close()
        return True
    except Exception:
        return False


# Determine at test collection time if real postgres is reachable
def check_postgres():
    try:
        return asyncio.run(is_postgres_reachable())
    except Exception:
        return False


POSTGRES_AVAILABLE = check_postgres()
pytestmark = pytest.mark.skipif(
    not POSTGRES_AVAILABLE,
    reason=f"PostgreSQL not reachable at {TEST_DB_URL}",
)


@pytest.mark.asyncio
async def test_real_postgres_asyncpg_successful_transaction():
    """Verify transaction commit and query execution on real PostgreSQL using asyncpg."""
    import asyncpg

    conn = await asyncpg.connect(TEST_DB_URL)
    try:
        await conn.execute("CREATE TEMP TABLE test_pgshield_items (id SERIAL PRIMARY KEY, val TEXT);")

        async def insert_item(c: asyncpg.Connection) -> int:
            row = await c.fetchrow(
                "INSERT INTO test_pgshield_items (val) VALUES ($1) RETURNING id;",
                "hello_pgshield",
            )
            return row["id"]

        item_id = await run_asyncpg_transaction(conn, insert_item)
        assert item_id is not None

        # Verify committed
        count = await conn.fetchval("SELECT count(*) FROM test_pgshield_items;")
        assert count == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_real_postgres_sqlalchemy_rollback_on_error():
    """Verify transaction rollback on non-retryable error using real PostgreSQL + SQLAlchemy."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy import text

    # Transform standard URL to asyncpg URL format if needed
    sa_url = TEST_DB_URL.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url)

    try:
        async with engine.connect() as conn:
            await conn.execute(text("CREATE TEMP TABLE test_sa_items (id INT PRIMARY KEY);"))
            await conn.commit()

        async with AsyncSession(engine) as session:
            # First insert
            async def first_insert(s: AsyncSession):
                await s.execute(text("INSERT INTO test_sa_items (id) VALUES (1);"))

            await run_sqlalchemy_transaction(session, first_insert)

            # Duplicate insert (unique violation 23505) -> should rollback and propagate
            async def duplicate_insert(s: AsyncSession):
                await s.execute(text("INSERT INTO test_sa_items (id) VALUES (1);"))

            with pytest.raises(Exception) as exc_info:
                await run_sqlalchemy_transaction(session, duplicate_insert)

            # Verify it failed and rolled back
            assert "23505" in str(exc_info.value) or "unique" in str(exc_info.value).lower()

        # Check only 1 item exists in table
        async with AsyncSession(engine) as session:
            res = await session.execute(text("SELECT count(*) FROM test_sa_items;"))
            count = res.scalar()
            assert count == 1
    finally:
        await engine.dispose()
