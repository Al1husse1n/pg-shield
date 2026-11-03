"""Test configuration and fixtures for PGShield."""

from typing import Callable, Optional

import pytest

from pgshield.transaction.runner import TransactionAdapter


class FakeAdapter(TransactionAdapter):
    """TransactionAdapter test double with configurable failure injection."""

    def __init__(
        self,
        *,
        fail_on_begin: bool = False,
        fail_on_commit: bool = False,
        fail_on_rollback: bool = False,
        error_factory: Optional[Callable[[str], Exception]] = None,
    ) -> None:
        self.fail_on_begin = fail_on_begin
        self.fail_on_commit = fail_on_commit
        self.fail_on_rollback = fail_on_rollback
        self.error_factory = error_factory or (
            lambda stage: RuntimeError(f"Injected failure during {stage}")
        )
        self.begins: int = 0
        self.commits: int = 0
        self.rollbacks: int = 0

    async def begin(self) -> None:
        self.begins += 1
        if self.fail_on_begin:
            raise self.error_factory("begin")

    async def commit(self) -> None:
        self.commits += 1
        if self.fail_on_commit:
            raise self.error_factory("commit")

    async def rollback(self) -> None:
        self.rollbacks += 1
        if self.fail_on_rollback:
            raise self.error_factory("rollback")


@pytest.fixture
def fake_adapter() -> FakeAdapter:
    """Provide a fresh FakeAdapter instance for testing."""
    return FakeAdapter()
