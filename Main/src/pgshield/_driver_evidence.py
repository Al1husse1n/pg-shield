from __future__ import annotations

from typing import Final

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
    cls = type(target)
    mod = (getattr(cls, "__module__", "") or "").lower()
    name = cls.__name__

    if any(driver in mod for driver in ("asyncpg", "psycopg", "psycopg2")):
        return True

    if any(driver in mod for driver in _DRIVER_MODULE_KEYWORDS) and name in _DRIVER_CLASS_ALLOWLIST:
        return True

    return False


def _has_postgres_driver_evidence(error: BaseException) -> bool:
    seen: set[int] = set()
    stack: list[BaseException] = [error]

    while stack:
        err = stack.pop()

        if id(err) in seen:
            continue
        seen.add(id(err))

        if _matches_driver_evidence(err):
            return True

        for attr in ("orig", "__cause__"):
            nxt = getattr(err, attr, None)
            if isinstance(nxt, BaseException):
                stack.append(nxt)

    return False
