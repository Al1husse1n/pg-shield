"""Decorator for declarative whole-transaction execution in PGShield.

This module provides the `@transaction` decorator which wraps async functions and
delegates execution directly to `TransactionRunner`.
"""

from __future__ import annotations

import functools
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    ParamSpec,
    Set,
    TypeVar,
    overload,
)

from pgshield.instrumentation.logging import TransactionLogger
from pgshield.instrumentation.metrics import MetricsCollector
from pgshield.postgres.diagnostics import DiagnosticCollector
from pgshield.retry.backoff import RandomFloatGenerator
from pgshield.retry.policy import RetryPolicy
from pgshield.transaction.runner import (
    SleepCallable,
    TransactionAdapter,
    TransactionRunner,
)

P = ParamSpec("P")
R = TypeVar("R")


@overload
def transaction(
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
    ...


@overload
def transaction(
    *,
    policy: Optional[RetryPolicy] = None,
    adapter: Optional[TransactionAdapter] = None,
    runner: Optional[TransactionRunner] = None,
    logger: Optional[TransactionLogger] = None,
    metrics: Optional[MetricsCollector] = None,
    diagnostics: Optional[DiagnosticCollector] = None,
    max_attempts: Optional[int] = None,
    base_delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    backoff_factor: Optional[float] = None,
    jitter: Optional[bool] = None,
    retryable_sqlstates: Optional[Set[str]] = None,
    sleep_func: Optional[SleepCallable] = None,
    random_func: Optional[RandomFloatGenerator] = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    ...


def transaction(
    fn: Optional[Callable[P, Awaitable[R]]] = None,
    *,
    policy: Optional[RetryPolicy] = None,
    adapter: Optional[TransactionAdapter] = None,
    runner: Optional[TransactionRunner] = None,
    logger: Optional[TransactionLogger] = None,
    metrics: Optional[MetricsCollector] = None,
    diagnostics: Optional[DiagnosticCollector] = None,
    max_attempts: Optional[int] = None,
    base_delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    backoff_factor: Optional[float] = None,
    jitter: Optional[bool] = None,
    retryable_sqlstates: Optional[Set[str]] = None,
    sleep_func: Optional[SleepCallable] = None,
    random_func: Optional[RandomFloatGenerator] = None,
) -> Any:
    """Decorator to execute an async function with automatic whole-transaction retries.

    Delegates completely to TransactionRunner, guaranteeing that the wrapped function
    is re-executed from the very beginning upon encountering a retryable PostgreSQL error.

    Can be used with or without arguments:
        @transaction
        async def transfer_funds(account_from, account_to, amount):
            ...

        @transaction(max_attempts=3, base_delay=0.1)
        async def create_order(user_id, item_id):
            ...

    Args:
        fn: The async function to decorate (when used without parentheses).
        policy: Optional custom RetryPolicy.
        adapter: Optional TransactionAdapter to manage begin/commit/rollback boundaries.
        runner: Optional pre-configured TransactionRunner instance.
        logger: Optional TransactionLogger.
        metrics: Optional MetricsCollector.
        diagnostics: Optional DiagnosticCollector.
        max_attempts: Shorthand override for policy max_attempts.
        base_delay: Shorthand override for policy base_delay.
        max_delay: Shorthand override for policy max_delay.
        backoff_factor: Shorthand override for policy backoff_factor.
        jitter: Shorthand override for policy jitter.
        retryable_sqlstates: Shorthand override for policy retryable_sqlstates.
        sleep_func: Async sleep callable for backoff delays (useful for testing).
        random_func: Injectable random generator for backoff jitter.

    Returns:
        The wrapped async function.
    """

    def decorator(target_fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        effective_runner: TransactionRunner
        if runner is not None:
            effective_runner = runner
        else:
            effective_policy: RetryPolicy
            if policy is not None:
                effective_policy = policy
            elif any(
                x is not None
                for x in (
                    max_attempts,
                    base_delay,
                    max_delay,
                    backoff_factor,
                    jitter,
                    retryable_sqlstates,
                )
            ):
                policy_kwargs: dict[str, Any] = {}
                if max_attempts is not None:
                    policy_kwargs["max_attempts"] = max_attempts
                if base_delay is not None:
                    policy_kwargs["base_delay"] = base_delay
                if max_delay is not None:
                    policy_kwargs["max_delay"] = max_delay
                if backoff_factor is not None:
                    policy_kwargs["backoff_factor"] = backoff_factor
                if jitter is not None:
                    policy_kwargs["jitter"] = jitter
                if retryable_sqlstates is not None:
                    policy_kwargs["retryable_sqlstates"] = retryable_sqlstates
                effective_policy = RetryPolicy(**policy_kwargs)
            else:
                effective_policy = RetryPolicy()

            effective_runner = TransactionRunner(
                policy=effective_policy,
                adapter=adapter,
                logger=logger,
                metrics=metrics,
                diagnostics=diagnostics,
                sleep_func=sleep_func,
                random_func=random_func,
            )

        @functools.wraps(target_fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await effective_runner.run(target_fn, *args, **kwargs)

        setattr(wrapper, "__pgshield_runner__", effective_runner)
        return wrapper

    if fn is not None:
        return decorator(fn)

    return decorator


transactional = transaction
