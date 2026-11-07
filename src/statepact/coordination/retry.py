"""Retry coordination with exponential backoff, jitter and replanning hooks.

Only errors whose ``retryable`` attribute is true are retried by default. A
:class:`~statepact.core.exceptions.ConflictError` is safe to retry because it
guarantees the transaction did not commit; an unknown exception is not.

``replan`` is called after a conflict and before the next attempt. Agents use it
to update their plan (for example, re-validate a tool result or choose a different
product) before a fresh snapshot is taken.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from statepact.core.exceptions import ConflictError, RetryExhaustedError, StatePactError

RetryCallback = Callable[[BaseException, int, float], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Policy for retrying whole transactions, never in-flight writes."""

    max_attempts: int = 5
    base_delay: float = 0.005
    max_delay: float = 1.0
    multiplier: float = 2.0
    jitter: float = 0.2
    retry_on: tuple[type[BaseException], ...] = (ConflictError,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter must be in [0, 1]")
        if self.base_delay > self.max_delay:
            raise ValueError("base_delay must be <= max_delay")

    def should_retry(self, error: BaseException, attempt: int) -> bool:
        if attempt >= self.max_attempts:
            return False
        if isinstance(error, StatePactError) and not error.retryable:
            return False
        return isinstance(error, self.retry_on)

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Delay before attempt ``attempt + 1`` (attempt is 1-based)."""

        raw = min(self.max_delay, self.base_delay * (self.multiplier ** (attempt - 1)))
        if self.jitter <= 0 or raw <= 0:
            return raw
        generator = rng or random
        low = raw * (1.0 - self.jitter)
        return low + (raw - low) * generator.random()


@dataclass(frozen=True, slots=True)
class RetryOutcome[T]:
    value: T
    attempts: int
    total_delay: float
    errors: tuple[BaseException, ...] = ()

    @property
    def retried(self) -> bool:
        return self.attempts > 1


async def run_with_retry[T](
    operation: Callable[[int], Awaitable[T]],
    policy: RetryPolicy | None = None,
    *,
    on_retry: RetryCallback | None = None,
    replan: Callable[[BaseException, int], Awaitable[None] | None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: random.Random | None = None,
) -> RetryOutcome[T]:
    """Execute ``operation(attempt)`` until it succeeds or the policy gives up."""

    effective = policy or RetryPolicy()
    errors: list[BaseException] = []
    total_delay = 0.0
    attempt = 1
    while True:
        try:
            value = await operation(attempt)
        except Exception as error:
            errors.append(error)
            if not effective.should_retry(error, attempt):
                if attempt == 1:
                    raise
                raise RetryExhaustedError(attempt, error) from error
            delay = effective.delay_for(attempt, rng=rng)
            total_delay += delay
            if replan is not None:
                await _maybe_await(replan(error, attempt))
            if on_retry is not None:
                await _maybe_await(on_retry(error, attempt, delay))
            await sleep(delay)
            attempt += 1
            continue
        return RetryOutcome(
            value=value,
            attempts=attempt,
            total_delay=total_delay,
            errors=tuple(errors),
        )


async def _maybe_await(value: Awaitable[None] | None) -> None:
    if value is not None:
        await value


def is_retryable(error: BaseException) -> bool:
    if isinstance(error, StatePactError):
        return error.retryable
    return False


def describe_retries(errors: Sequence[BaseException]) -> str:
    if not errors:
        return "no retries"
    return ", ".join(type(error).__name__ for error in errors)
