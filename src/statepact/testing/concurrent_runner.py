"""Deterministic concurrent execution helpers.

Used by the test suite, the benchmark runner and the playground. The barrier
option releases every coroutine at the same event-loop tick, which is what makes
race-condition tests reproducible instead of timing-dependent.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from statepact.core.exceptions import StatePactError


@dataclass(frozen=True, slots=True)
class Outcome[R]:
    index: int
    ok: bool
    value: R | None = None
    error: BaseException | None = None
    latency_ms: float = 0.0

    def unwrap(self) -> R:
        if self.error is not None:
            raise self.error
        assert self.value is not None
        return self.value


async def run_concurrently[T](
    count: int,
    worker: Callable[[int], Awaitable[T]],
    *,
    max_concurrency: int | None = None,
    synchronize_start: bool = True,
    stagger: float = 0.0,
) -> list[Outcome[T]]:
    """Run ``worker(i)`` for ``i`` in ``range(count)`` and collect outcomes.

    Exceptions are captured per task instead of cancelling peers, because
    conflict tests care about how many transactions *succeeded*.
    """

    if count < 0:
        raise ValueError("count must be >= 0")
    if max_concurrency is not None and max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
    if stagger < 0:
        raise ValueError("stagger must be >= 0")

    barrier = asyncio.Barrier(count) if synchronize_start and count > 1 else None
    semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None
    outcomes: list[Outcome[T] | None] = [None] * count

    async def runner(index: int) -> None:
        started = time.perf_counter()
        try:
            if barrier is not None:
                await barrier.wait()
            elif stagger:
                await asyncio.sleep(stagger * index)
            if semaphore is not None:
                async with semaphore:
                    value = await worker(index)
            else:
                value = await worker(index)
            elapsed = (time.perf_counter() - started) * 1000.0
            outcomes[index] = Outcome(index=index, ok=True, value=value, latency_ms=elapsed)
        except Exception as exc:
            elapsed = (time.perf_counter() - started) * 1000.0
            outcomes[index] = Outcome(index=index, ok=False, error=exc, latency_ms=elapsed)
        except asyncio.CancelledError:
            raise

    await asyncio.gather(*(runner(i) for i in range(count)))
    return [outcome for outcome in outcomes if outcome is not None]


def partition[T](outcomes: list[Outcome[T]]) -> tuple[list[Outcome[T]], list[Outcome[T]]]:
    """Split outcomes into (successes, failures)."""

    return (
        [outcome for outcome in outcomes if outcome.ok],
        [outcome for outcome in outcomes if not outcome.ok],
    )


def classify_errors[T](outcomes: list[Outcome[T]]) -> dict[str, int]:
    """Count failures by exception class name."""

    counts: dict[str, int] = {}
    for outcome in outcomes:
        if outcome.error is None:
            continue
        name = type(outcome.error).__name__
        counts[name] = counts.get(name, 0) + 1
    return counts


def invariant_violations[T](outcomes: list[Outcome[T]]) -> int:
    """Count failures caused by explicitly raised invariant assertions."""

    return sum(
        1
        for outcome in outcomes
        if isinstance(outcome.error, StatePactError) and not outcome.error.retryable
    )
