"""PostgreSQL pg_locks inspection and blocking analysis for PGShield.

This module provides focused utilities to inspect database locks, identify lock contention,
and analyze blocking/deadlock relationships without heavy query overhead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from pgshield.postgres.activity import QueryExecutor

# Standard diagnostic SQL query inspecting pg_locks with relation names
LOCKS_QUERY_BASE = """
SELECT
    l.pid,
    l.locktype AS lock_type,
    l.mode,
    l.granted,
    COALESCE(c.relname, '') AS relation_name,
    COALESCE(n.nspname, '') AS schema_name,
    l.page,
    l.tuple,
    l.transactionid::text AS transaction_id,
    l.virtualtransaction AS virtual_transaction
FROM pg_locks l
LEFT JOIN pg_class c ON l.relation = c.oid
LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE l.pid <> pg_backend_pid()
ORDER BY l.granted ASC, l.pid ASC
"""


@dataclass(frozen=True, slots=True)
class LockInfo:
    """Snapshot of a single PostgreSQL lock acquisition or lock request from pg_locks.

    Attributes:
        pid: Process ID of the backend server holding or requesting the lock.
        lock_type: Type of lock target (e.g. "relation", "transactionid", "tuple", "virtualxid").
        mode: Lock mode held or desired (e.g. "RowExclusiveLock", "ExclusiveLock", "ShareLock").
        granted: True if the lock is currently held, False if the process is blocked waiting.
        relation_name: Name of the table or index relation, if applicable.
        schema_name: Schema containing the relation, if applicable.
        page: Relation block number, if page or tuple lock.
        tuple: Tuple number within page, if tuple lock.
        transaction_id: Transaction ID string if transactionid lock.
        virtual_transaction: Virtual transaction ID string if virtualxid lock.
    """

    pid: int
    lock_type: str
    mode: str
    granted: bool
    relation_name: Optional[str] = None
    schema_name: Optional[str] = None
    page: Optional[int] = None
    tuple: Optional[int] = None
    transaction_id: Optional[str] = None
    virtual_transaction: Optional[str] = None

    @property
    def is_waiting(self) -> bool:
        """Return True if the backend is waiting to acquire this lock."""
        return not self.granted

    @property
    def qualified_relation(self) -> Optional[str]:
        """Return schema-qualified relation name if available."""
        if self.schema_name and self.relation_name:
            return f"{self.schema_name}.{self.relation_name}"
        return self.relation_name or None

    @property
    def target_key(self) -> Tuple[Any, ...]:
        """Return a hashable composite key uniquely identifying the locked resource."""
        return (
            self.lock_type,
            self.relation_name,
            self.schema_name,
            self.page,
            self.tuple,
            self.transaction_id,
            self.virtual_transaction,
        )


@dataclass(frozen=True, slots=True)
class BlockingRelationship:
    """Represents a blocking conflict where one backend is waiting for another backend.

    Attributes:
        blocked_pid: PID of the waiting/blocked process.
        blocking_pid: PID of the process holding the conflicting lock.
        lock_type: The type of lock being contested.
        waiting_mode: The lock mode requested by the blocked PID.
        holding_mode: The lock mode held by the blocking PID.
        resource_description: Human-readable description of the contested resource.
    """

    blocked_pid: int
    blocking_pid: int
    lock_type: str
    waiting_mode: str
    holding_mode: str
    resource_description: str


def parse_lock_rows(rows: Iterable[Mapping[str, Any]]) -> List[LockInfo]:
    """Parse raw database result rows into structured LockInfo instances.

    Args:
        rows: Iterable of mapping objects (e.g. asyncpg Record, dict, or SQLAlchemy RowMapping).

    Returns:
        List of parsed LockInfo instances.
    """
    locks: List[LockInfo] = []

    for row in rows:
        pid = row.get("pid")
        if pid is None:
            continue

        rel_name = row.get("relation_name")
        if rel_name == "":
            rel_name = None

        schema_name = row.get("schema_name")
        if schema_name == "":
            schema_name = None

        granted = bool(row.get("granted", False))
        page = row.get("page")
        tuple_num = row.get("tuple")

        locks.append(
            LockInfo(
                pid=int(pid),
                lock_type=str(row.get("lock_type", "unknown")),
                mode=str(row.get("mode", "unknown")),
                granted=granted,
                relation_name=rel_name,
                schema_name=schema_name,
                page=int(page) if page is not None else None,
                tuple=int(tuple_num) if tuple_num is not None else None,
                transaction_id=str(row["transaction_id"]) if row.get("transaction_id") else None,
                virtual_transaction=str(row["virtual_transaction"])
                if row.get("virtual_transaction")
                else None,
            )
        )

    return locks


def detect_blocking_chains(locks: Sequence[LockInfo]) -> List[BlockingRelationship]:
    """Analyze a collection of LockInfo objects to discover blocking relationships.

    Matches waiting lock requests (`granted=False`) against held locks (`granted=True`)
    on the exact same database target resource.

    Args:
        locks: Sequence of LockInfo records from pg_locks.

    Returns:
        List of detected BlockingRelationship records.
    """
    # Group granted locks by target resource key
    granted_by_target: Dict[Tuple[Any, ...], List[LockInfo]] = {}
    waiting_locks: List[LockInfo] = []

    for lock in locks:
        if lock.granted:
            granted_by_target.setdefault(lock.target_key, []).append(lock)
        else:
            waiting_locks.append(lock)

    blocking_relationships: List[BlockingRelationship] = []
    seen: Set[Tuple[int, int, Tuple[Any, ...]]] = set()

    for waiting in waiting_locks:
        holders = granted_by_target.get(waiting.target_key, [])
        for holder in holders:
            if holder.pid == waiting.pid:
                continue

            dedup_key = (waiting.pid, holder.pid, waiting.target_key)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            # Build human-readable resource description
            resource_parts = [f"lock_type={waiting.lock_type}"]
            if waiting.qualified_relation:
                resource_parts.append(f"relation={waiting.qualified_relation}")
            if waiting.page is not None:
                resource_parts.append(f"page={waiting.page}")
            if waiting.tuple is not None:
                resource_parts.append(f"tuple={waiting.tuple}")
            if waiting.transaction_id:
                resource_parts.append(f"xid={waiting.transaction_id}")

            resource_desc = " ".join(resource_parts)

            blocking_relationships.append(
                BlockingRelationship(
                    blocked_pid=waiting.pid,
                    blocking_pid=holder.pid,
                    lock_type=waiting.lock_type,
                    waiting_mode=waiting.mode,
                    holding_mode=holder.mode,
                    resource_description=resource_desc,
                )
            )

    return blocking_relationships


def build_locks_query(*, filter_pids: Optional[Sequence[int]] = None) -> str:
    """Construct a SQL query for pg_locks, optionally filtered by PIDs."""
    if not filter_pids:
        return LOCKS_QUERY_BASE

    pids_str = ", ".join(str(int(p)) for p in filter_pids)
    return f"{LOCKS_QUERY_BASE} AND l.pid IN ({pids_str})"


async def fetch_locks(
    executor: QueryExecutor,
    *,
    filter_pids: Optional[Sequence[int]] = None,
) -> List[LockInfo]:
    """Execute a query against pg_locks using the provided QueryExecutor.

    Args:
        executor: QueryExecutor implementation.
        filter_pids: Optional PID filter.

    Returns:
        List of parsed LockInfo records.
    """
    sql = build_locks_query(filter_pids=filter_pids)
    rows = await executor.execute_query(sql)
    return parse_lock_rows(rows)
