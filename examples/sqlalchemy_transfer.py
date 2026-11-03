"""PGShield SQLAlchemy Money Transfer Example.

Demonstrates executing a multi-statement financial transfer inside an atomic,
retry-safe transaction boundary using PGShield's run_sqlalchemy_transaction helper.

To run this example:
    export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
    python examples/sqlalchemy_transfer.py
"""

import asyncio
import os
import sys

from pgshield import run_sqlalchemy_transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


async def main() -> None:
    dsn = os.environ.get("PGSHIELD_TEST_DSN")
    if not dsn:
        print(
            "Error: PGSHIELD_TEST_DSN environment variable is not set.", file=sys.stderr
        )
        print(
            "Example: export PGSHIELD_TEST_DSN="
            "'postgresql://postgres:postgres@localhost:5432/postgres'",
            file=sys.stderr,
        )
        sys.exit(1)

    sa_url = dsn.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url)

    async with engine.connect() as setup_conn:
        await setup_conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS demo_sa_accounts (id INT PRIMARY KEY, balance INT);"
            )
        )
        await setup_conn.execute(text("TRUNCATE TABLE demo_sa_accounts;"))
        await setup_conn.execute(
            text(
                "INSERT INTO demo_sa_accounts (id, balance) VALUES (1, 1000), (2, 500);"
            )
        )
        await setup_conn.commit()

    async with AsyncSession(engine) as session:

        async def transfer(s: AsyncSession) -> None:
            amount = 250
            await s.execute(
                text(
                    "UPDATE demo_sa_accounts SET balance = balance - :amt WHERE id = 1"
                ),
                {"amt": amount},
            )
            await s.execute(
                text(
                    "UPDATE demo_sa_accounts SET balance = balance + :amt WHERE id = 2"
                ),
                {"amt": amount},
            )

        await run_sqlalchemy_transaction(session, transfer)

    async with engine.connect() as check_conn:
        res = await check_conn.execute(
            text("SELECT id, balance FROM demo_sa_accounts ORDER BY id;")
        )
        rows = res.fetchall()
        print(
            f"Transfer successful! Account 1: ${rows[0][1]}, Account 2: ${rows[1][1]}"
        )
        await check_conn.execute(text("DROP TABLE demo_sa_accounts;"))
        await check_conn.commit()

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
