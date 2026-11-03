"""Transaction management and execution layer for PGShield.

Provides:
- TransactionRunner: Central execution engine for whole-transaction retries (runner.py)
- TransactionAdapter & NullTransactionAdapter: Protocols for transaction boundaries (runner.py)
- TransactionResult: Result metadata container (runner.py)
- TransactionContext & AttemptRecord: Execution context and attempt history (context.py)
- get_current_context(): Active context accessor (context.py)
- @transaction & @transactional: Declarative retry decorators (decorator.py)
"""

from pgshield.transaction.context import (
    AttemptRecord,
    TransactionContext,
    get_current_context,
)
from pgshield.transaction.decorator import (
    transaction,
    transactional,
)
from pgshield.transaction.runner import (
    NullTransactionAdapter,
    TransactionAdapter,
    TransactionResult,
    TransactionRunner,
)

__all__ = [
    "TransactionRunner",
    "TransactionAdapter",
    "NullTransactionAdapter",
    "TransactionResult",
    "TransactionContext",
    "AttemptRecord",
    "get_current_context",
    "transaction",
    "transactional",
]
