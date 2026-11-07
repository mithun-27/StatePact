"""Failure injection: the commit point must never leave partial state."""

from __future__ import annotations

import asyncio

import pytest

from agentcommit import ConflictError, TransactionManager
from agentcommit.storage.memory import MemoryBackend
from agentcommit.testing import run_concurrently


class InjectedFaultError(RuntimeError):
    def __init__(self, point: str) -> None:
        super().__init__(f"injected fault at {point}")
        self.point = point


class FaultPlan:
    def __init__(self, fail_at: set[str] | None = None) -> None:
        self.fail_at = set(fail_at or ())
        self.points: list[str] = []
        self.triggered = 0

    def __call__(self, point: str) -> None:
        self.points.append(point)
        if point in self.fail_at:
            self.triggered += 1
            raise InjectedFaultError(point)


def build(plan: FaultPlan) -> tuple[MemoryBackend, TransactionManager]:
    backend = MemoryBackend(fault_injector=plan)
    return backend, TransactionManager(backend)


@pytest.mark.parametrize("point", ["before_validate", "after_validate", "before_apply"])
async def test_fault_before_apply_leaves_state_and_version_untouched(point: str) -> None:
    plan = FaultPlan({point})
    backend, manager = build(plan)
    resource = manager.resource("r", {"k": 1})
    with pytest.raises(InjectedFaultError):
        async with manager.session(agent_id="a", resources=[resource]) as session:
            await session.write("k", 2)
            await session.commit()
    assert plan.triggered == 1
    assert await resource.read("k") == 1
    assert backend.sequence == 1
    assert manager.transactions()[-1].status.value in {"failed", "aborted"}


async def test_commit_recovers_after_fault_is_cleared() -> None:
    plan = FaultPlan({"after_validate"})
    _backend, manager = build(plan)
    resource = manager.resource("r", {"k": 1})
    with pytest.raises(InjectedFaultError):
        async with manager.session(agent_id="a", resources=[resource]) as session:
            await session.write("k", 2)
            await session.commit()
    plan.fail_at.clear()
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        await session.commit()
    assert await resource.read("k") == 2


async def test_random_faults_never_corrupt_counter() -> None:
    """Every successful commit applies exactly one increment; no partial writes."""

    class EveryNth(FaultPlan):
        def __init__(self, n: int) -> None:
            super().__init__(set())
            self.n = n
            self.calls = 0

        def __call__(self, point: str) -> None:
            if point != "before_apply":
                return
            self.calls += 1
            if self.calls % self.n == 0:
                self.triggered += 1
                raise InjectedFaultError(point)

    plan = EveryNth(3)
    _backend, manager = build(plan)
    resource = manager.resource("r", {"k": 0})
    successes: list[int] = []

    async def worker(index: int) -> None:
        while True:
            try:
                async with manager.session(agent_id=f"a{index}", resources=[resource]) as session:
                    value = await session.read("k")
                    await session.write("k", value + 1)
                    await session.commit()
                successes.append(index)
                return
            except ConflictError:
                await asyncio.sleep(0)
            except InjectedFaultError:
                await asyncio.sleep(0)

    await run_concurrently(50, worker, synchronize_start=True)
    assert plan.triggered >= 1
    assert await resource.read("k") == len(successes)


async def test_locks_are_released_when_backend_commit_fails() -> None:
    plan = FaultPlan({"before_apply"})
    backend, manager = build(plan)
    resource = manager.resource("r", {"k": 1})
    with pytest.raises(InjectedFaultError):
        async with manager.session(
            agent_id="a", resources=[resource], isolation="pessimistic"
        ) as session:
            await session.write("k", 2)
            await session.commit()
    assert backend.lock_snapshot()["locks"] == {}
    assert backend.lock_stats.to_dict()["acquisitions"] >= 1

    plan.fail_at.clear()
    async with manager.session(
        agent_id="b", resources=[resource], isolation="pessimistic", lock_timeout=0.2
    ) as session:
        await session.write("k", 3)
        await session.commit()
    assert await resource.read("k") == 3


async def test_idempotency_replay_survives_a_retry_after_transport_failure() -> None:
    plan = FaultPlan(set())
    _backend, manager = build(plan)
    resource = manager.resource("r", {"k": 5})
    async with manager.session(agent_id="a", resources=[resource], idempotency_key="op-1") as s:
        stock = await s.read("k")
        await s.write("k", stock - 1)
        await s.commit()
    # Replaying the same intent after an ambiguous failure must not decrement twice.
    async with manager.session(agent_id="a", resources=[resource], idempotency_key="op-1") as s:
        stock = await s.read("k")
        await s.write("k", stock - 1)
        result = await s.commit()
    assert result.replayed is True
    assert await resource.read("k") == 4
