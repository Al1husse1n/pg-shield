"""PostgreSQL pg_stat_activity session inspection for PGShield diagnostics.

This module provides focused, security-conscious utilities to inspect active PostgreSQL
backend processes and their wait states during concurrency diagnostic workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime               
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
)

# Standard SQL query for inspecting pg_stat_activity without sensitive query leaks
ACTIVITY_QUERY_BASE = """
SELECT
    pid,
    datname AS database,
    usename AS username,
    application_name,
    client_addr::text AS client_addr,
    state,
    query_start,
    xact_start,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()           #Excludes the current session that is running this query
"""

# Query including a truncated/safe query snippet when explicitly enabled
ACTIVITY_QUERY_WITH_SNIPPET = """
SELECT
    pid,
    datname AS database,
    usename AS username,
    application_name,
    client_addr::text AS client_addr,
    state,
    query_start,
    xact_start,
    wait_event_type,
    wait_event,
    LEFT(query, 120) AS query_snippet           #contains first 120 characters of the associated query (alternative/optional for security purposes)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
"""


@runtime_checkable          #useful to pass isinstance check for protocol, but not necessarily needed here
class QueryExecutor(Protocol):
    """Protocol for executing read-only diagnostic queries against PostgreSQL."""

    async def execute_query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Sequence[Mapping[str, Any]]:       #str key and any value
        """Execute a SQL query and return rows as dictionaries or mapping objects."""
        ...


@dataclass(frozen=True, slots=True)
class SessionActivity:
    """Snapshot of a PostgreSQL backend session from pg_stat_activity.

    Attributes:
        pid: Process ID of the backend server.
        database: Database name the backend is connected to.    
        username: Name of the authenticated database user.
        application_name: Client application name.
        client_addr: Client IP address (if TCP connection).
        state: Connection state (e.g. "active", "idle in transaction", "idle").
        query_start: Timestamp when the active query started.
        xact_start: Timestamp when the active transaction started.
        wait_event_type: Type of wait event (e.g. "Lock", "IO", "LWLock", "Client").
        wait_event: Name of the specific wait event.
        query_snippet: Truncated query snippet (only present if explicitly requested).
    """

    pid: int
    database: Optional[str] = None
    username: Optional[str] = None
    application_name: Optional[str] = None
    client_addr: Optional[str] = None
    state: Optional[str] = None
    query_start: Optional[Union[datetime, str]] = None
    xact_start: Optional[Union[datetime, str]] = None
    wait_event_type: Optional[str] = None
    wait_event: Optional[str] = None
    query_snippet: Optional[str] = None

    @property
    def is_waiting_on_lock(self) -> bool:
        """Return True if this backend process is actively waiting for a database lock."""
        return self.wait_event_type == "Lock"

    @property
    def is_idle_in_transaction(self) -> bool:
        """Return True if this process is holding open a transaction while idle."""
        return self.state == "idle in transaction"


def parse_activity_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    include_query_snippet: bool = False,
) -> List[SessionActivity]:
    """Parse raw database result rows into structured SessionActivity instances.

    Args:
        rows: Iterable of mapping objects (e.g. asyncpg Record, dict, or SQLAlchemy RowMapping).
        include_query_snippet: Whether to include query snippet if present in rows.

    Returns:
        List of parsed SessionActivity objects.
    """
    activities: List[SessionActivity] = []

    for row in rows:
        pid = row.get("pid")
        if pid is None:
            continue

        query_snippet = None
        if include_query_snippet and "query_snippet" in row:
            raw_snippet = row["query_snippet"]
            if raw_snippet is not None:
                query_snippet = str(raw_snippet).strip()

        activities.append(
            SessionActivity(
                pid=int(pid),
                database=row.get("database"),
                username=row.get("username"),
                application_name=row.get("application_name"),
                client_addr=row.get("client_addr"),
                state=row.get("state"),
                query_start=row.get("query_start"),
                xact_start=row.get("xact_start"),
                wait_event_type=row.get("wait_event_type"),
                wait_event=row.get("wait_event"),
                query_snippet=query_snippet,
            )
        )

    return activities


def build_activity_query(
    *,
    include_query_snippet: bool = False,
    filter_database: Optional[str] = None,
    filter_pids: Optional[Sequence[int]] = None,
) -> str:
    """Construct a parameterized or filtered SQL query for pg_stat_activity.

    Args:
        include_query_snippet: Whether to include truncated query text.
        filter_database: Optional database name filter.
        filter_pids: Optional list of specific PIDs to filter by.

    Returns:
        SQL query string.
    """
    base = ACTIVITY_QUERY_WITH_SNIPPET if include_query_snippet else ACTIVITY_QUERY_BASE
    clauses: List[str] = []

    if filter_database is not None:
        safe_db = filter_database.replace("'", "''")        #escaping
        clauses.append(f"datname = '{safe_db}'")

    if filter_pids:
        pids_str = ", ".join(str(int(p)) for p in filter_pids)
        clauses.append(f"pid IN ({pids_str})")

    if clauses:
        return f"{base} AND {' AND '.join(clauses)}"

    return base


async def fetch_activity(
    executor: QueryExecutor,
    *,
    include_query_snippet: bool = False,
    filter_database: Optional[str] = None,
    filter_pids: Optional[Sequence[int]] = None,
) -> List[SessionActivity]:
    """Execute a query against pg_stat_activity using the provided QueryExecutor.

    Args:
        executor: QueryExecutor implementation to run the diagnostic query.
        include_query_snippet: Whether to include query snippet.
        filter_database: Optional database filter.
        filter_pids: Optional PID filter.

    Returns:
        List of SessionActivity records.
    """
    sql = build_activity_query(
        include_query_snippet=include_query_snippet,
        filter_database=filter_database,
        filter_pids=filter_pids,
    )
    rows = await executor.execute_query(sql)                #the protocol must be defined using an adapter in integration, since asyncpg and sqlalchemy doesnt have execute_query in them
    return parse_activity_rows(rows, include_query_snippet=include_query_snippet)
