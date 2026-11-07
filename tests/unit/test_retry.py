from __future__ import annotations

import random

import pytest

from agentcommit import ConflictError, RetryExhaustedError, RetryPolicy, TransactionManager
from agentcommit.coordination.retry import run_with_retry


class FakeSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


async def test_retry_succeeds_after_conflicts() -> None:
    attempts: list[int] = []
    sleep = FakeSleep()

    async def operation(number: int) -> str:
        attempts.append(number)
        if number < 3:
            raise ConflictError(message="conflict")
        return "done"

    outcome = await run_with_retry(
        operation,
        RetryPolicy(max_attempts=5, base_delay=0.01, jitter=0.0),
        sleep=sleep,
    )
    assert outcome.value == "done"
    assert outcome.attempts == 3
    assert attempts == [1, 2, 3]
    assert sleep.delays == [0.01, 0.02]


async def test_retry_exhaustion_raises_with_cause() -> None:
    async def operation(number: int) -> None:
        raise ConflictError(message="always conflicting")

    with pytest.raises(RetryExhaustedError) as info:
        await run_with_retry(
            operation,
            RetryPolicy(max_attempts=3, base_delay=0.0),
            sleep=FakeSleep(),
        )
    assert info.value.attempts == 3
    assert isinstance(info.value.last_error, ConflictError)


async def test_non_retryable_errors_are_not_retried() -> None:
    calls = 0

    async def operation(number: int) -> None:
        nonlocal calls
        calls += 1
        raise ValueError("bug in agent code")

    with pytest.raises(ValueError):
        await run_with_retry(operation, RetryPolicy(max_attempts=5), sleep=FakeSleep())
    assert calls == 1


def test_backoff_is_capped_and_monotonic() -> None:
    policy = RetryPolicy(
        base_delay=0.01, max_delay=0.05, multiplier=2.0, jitter=0.0, max_attempts=10
    )
    delays = [policy.delay_for(attempt) for attempt in range(1, 8)]
    assert delays == [0.01, 0.02, 0.04, 0.05, 0.05, 0.05, 0.05]


def test_jitter_stays_within_bounds() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=1.0, jitter=0.5, max_attempts=3)
    rng = random.Random(1234)
    for _ in range(100):
        delay = policy.delay_for(1, rng=rng)
        assert 0.5 <= delay <= 1.0


def test_invalid_policies_are_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(jitter=1.5)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=2.0, max_delay=1.0)


async def test_manager_retry_replans_and_re_reads(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    observed: list[int] = []
    replanned: list[int] = []

    async def work(session) -> int:
        value = await session.read("k")
        observed.append(value)
        if len(observed) == 1:
            # Simulate a concurrent winner between our read and commit.
            other = manager.session(agent_id="other", resources=[resource])
            async with other:
                await other.write("k", 2)
                await other.commit()
        await session.write("k", value + 10)
        return value + 10

    async def replan(error: BaseException, attempt: int) -> None:
        replanned.append(attempt)

    result = await manager.run(
        "agent",
        [resource],
        work,
        replan=replan,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0),
        sleep=FakeSleep(),
    )
    assert result == 12
    assert observed == [1, 2]
    assert replanned == [1]
    assert await resource.read("k") == 12
