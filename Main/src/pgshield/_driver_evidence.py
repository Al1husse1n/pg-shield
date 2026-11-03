"""Internal helper for validating PostgreSQL driver error evidence.

Ensures that exceptions carrying SQLSTATE-like attributes are verified to
originate from genuine PostgreSQL drivers or supported DBAPI/SQLAlchemy wrappers,
preventing misclassification of arbitrary application exceptions.
"""

from __future__ import annotations

from typing import Final, Optional

# Recognized PostgreSQL driver modules and ORM wrappers
_DRIVER_MODULE_KEYWORDS: Final[tuple[str, ...]] = (
    "asyncpg",
    "psycopg",
    "psycopg2",
    "sqlalchemy",
)

# Standard PostgreSQL driver / PEP 249 DBAPI / SQLAlchemy exception class names
_DRIVER_CLASS_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "PostgresError",
        "DBAPIError",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
        "SQLAlchemyError",
    }
)


def _matches_driver_evidence(target: BaseException) -> bool:
    """Check whether a single exception has evidence of originating from a PostgreSQL driver."""
    cls = type(target)
    mod = (getattr(cls, "__module__", "") or "").lower()
    name = cls.__name__

    # 1. Direct PostgreSQL driver modules (asyncpg, psycopg, psycopg2):
    # All exceptions from these dedicated drivers originate from PostgreSQL.
    if any(driver in mod for driver in ("asyncpg", "psycopg", "psycopg2")):
        return True

    # 2. Database abstraction (e.g. SQLAlchemy):
    # Requires BOTH a driver-ish module name and a recognized DBAPI/SQLAlchemy exception class name.
    # A class-name-only match is not safe in isolation (e.g. sqlite3.DatabaseError or custom user
    # exceptions named DatabaseError in application code), so BOTH the class name and a driver-ish
    # module are required.
    if any(driver in mod for driver in _DRIVER_MODULE_KEYWORDS) and name in _DRIVER_CLASS_ALLOWLIST:
        return True

    return False


def _has_postgres_driver_evidence(
    error: BaseException, _visited: Optional[set[int]] = None
) -> bool:
    """Verify evidence that an exception originates from a PostgreSQL driver.

    Rules:
    1. Direct PostgreSQL driver: The exception class's module contains 'asyncpg', 'psycopg',
       or 'psycopg2'.
    2. Driver/DBAPI class allowlist: The exception class's module contains a driver keyword
       ('asyncpg', 'psycopg', 'psycopg2', 'sqlalchemy') AND the exception class name is in
       _DRIVER_CLASS_ALLOWLIST. A class-name-only match is not safe in isolation (e.g.
       sqlite3.DatabaseError or custom application exceptions named DatabaseError), so BOTH
       the class name and a driver-ish module are required.
    3. Wrapped exceptions: The exception has an `.orig` or `.__cause__` attribute pointing to
       an underlying exception that satisfies rule 1 or rule 2.
    """
    if _matches_driver_evidence(error):
        return True

    if _visited is None:
        _visited = set()
    err_id = id(error)
    if err_id in _visited:
        return False
    _visited.add(err_id)

    orig = getattr(error, "orig", None)
    if isinstance(orig, BaseException) and _has_postgres_driver_evidence(orig, _visited):
        return True

    cause = getattr(error, "__cause__", None)
    if isinstance(cause, BaseException) and _has_postgres_driver_evidence(cause, _visited):
        return True

    return False

