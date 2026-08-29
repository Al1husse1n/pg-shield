"""Database driver and ORM integrations for PGShield.

Provides:
- sqlalchemy.py: SQLAlchemy AsyncSession integration (SQLAlchemySessionAdapter, SQLAlchemyTransactionRunner, run_sqlalchemy_transaction)
- asyncpg.py: asyncpg driver integration (AsyncpgConnectionAdapter, AsyncpgTransactionRunner, run_asyncpg_transaction)
"""

from pgshield.integrations.asyncpg import (
    AsyncpgConnectionAdapter,
    AsyncpgTransactionRunner,
    extract_asyncpg_sqlstate,
    run_asyncpg_transaction,
)
from pgshield.integrations.sqlalchemy import (
    SQLAlchemySessionAdapter,
    SQLAlchemyTransactionRunner,
    extract_sqlalchemy_sqlstate,
    run_sqlalchemy_transaction,
)

__all__ = [
    # sqlalchemy
    "SQLAlchemySessionAdapter",
    "SQLAlchemyTransactionRunner",
    "extract_sqlalchemy_sqlstate",
    "run_sqlalchemy_transaction",
    # asyncpg
    "AsyncpgConnectionAdapter",
    "AsyncpgTransactionRunner",
    "extract_asyncpg_sqlstate",
    "run_asyncpg_transaction",
]
