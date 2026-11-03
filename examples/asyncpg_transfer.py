"""PGShield asyncpg Money Transfer Example.

Demonstrates executing a multi-statement financial transfer inside an atomic,
retry-safe transaction boundary using PGShield's run_asyncpg_transaction helper.

To run this example:
    export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
    python examples/asyncpg_transfer.py
"""

import asyncio
import os
import sys

import asyncpg
from pgshield import run_asyncpg_transaction


async def main() -> None:
    dsn = os.environ.get("PGSHIELD_TEST_DSN")
    if not dsn:
        print("Error: PGSHIELD_TEST_DSN environment variable is not set.", file=sys.stderr)
        print(
            "Example: export PGSHIELD_TEST_DSN="
            "'postgresql://postgres:postgres@localhost:5432/postgres'",
            file=sys.stderr,
        )
        sys.exit(1)

    conn = await asyncpg.connect(dsn)
    try:
        # 1. Prepare table and sample balances
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS demo_accounts (id INT PRIMARY KEY, balance INT);"
        )
        await conn.execute("TRUNCATE TABLE demo_accounts;")
        await conn.execute("INSERT INTO demo_accounts (id, balance) VALUES (1, 1000), (2, 500);")

        # 2. Define business logic for transfer
        async def transfer(c: asyncpg.Connection) -> None:
            amount = 200
            await c.execute(
                "UPDATE demo_accounts SET balance = balance - $1 WHERE id = $2;",
                amount,
                1,
            )
            await c.execute(
                "UPDATE demo_accounts SET balance = balance + $1 WHERE id = $2;",
                amount,
                2,
            )

        # 3. Execute with whole-transaction automatic retries
        await run_asyncpg_transaction(conn, transfer)

        # 4. Verify committed balances
        b1 = await conn.fetchval("SELECT balance FROM demo_accounts WHERE id = 1;")
        b2 = await conn.fetchval("SELECT balance FROM demo_accounts WHERE id = 2;")
        print(f"Transfer successful! Account 1: ${b1}, Account 2: ${b2}")

        await conn.execute("DROP TABLE demo_accounts;")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
