from __future__ import annotations

import asyncio

import pytest

from statepact import (
    ConflictError,
    DeadlockError,
    LockTimeoutError,
    TransactionManager,
)
from statepact.testing import run_concurrently


async def _reserve(manager: TransactionManager, inventory, agent: str) -> bool:
    try:
        async with manager.session(agent_id=agent, resources=[inventory]) as session:
            stock = await session.read("sku")
            if stock <= 0:
                return False
            await asyncio.sleep(0)
            await session.write("sku", stock - 1)
            await session.commit()
            return True
    except Exception:
        return False


async def _reserve_with_retry(manager: TransactionManager, inventory, agent: str) -> bool:
    from statepact import RetryPolicy

    async def work(session) -> bool:
        stock = await session.read("sku")
        if stock <= 0:
            return False
        await asyncio.sleep(0)
        await session.write("sku", stock - 1)
        return True

    try:
        return await manager.run(
            agent,
            [inventory],
            work,
            retry_policy=RetryPolicy(max_attempts=30, base_delay=0.0, jitter=0.0),
        )
    except Exception:
        return False


async def test_single_unit_is_reserved_exactly_once(manager: TransactionManager) -> None:
    inventory = manager.resource("inventory", {"sku": 1})
    outcomes = await run_concurrently(200, lambda i: _reserve(manager, inventory, f"agent-{i}"))
    successes = [outcome for outcome in outcomes if outcome.ok and outcome.value]
    assert len(successes) == 1
    assert await inventory.read("sku") == 0


@pytest.mark.parametrize("stock", [1, 5, 25])
async def test_no_overselling_under_contention(manager: TransactionManager, stock: int) -> None:
    inventory = manager.resource("inventory", {"sku": stock})
    outcomes = await run_concurrently(
        100, lambda i: _reserve_with_retry(manager, inventory, f"agent-{i}")
    )
    reserved = sum(1 for outcome in outcomes if outcome.ok and outcome.value)
    assert reserved == stock
    assert await inventory.read("sku") == 0


async def test_1000_agents_do_not_oversell(manager: TransactionManager) -> None:
    inventory = manager.resource("inventory", {"sku": 7})
    outcomes = await run_concurrently(
        1000, lambda i: _reserve_with_retry(manager, inventory, f"agent-{i}")
    )
    reserved = sum(1 for outcome in outcomes if outcome.ok and outcome.value)
    assert reserved == 7
    assert await inventory.read("sku") == 0


async def test_concurrent_disjoint_writes_all_commit(manager: TransactionManager) -> None:
    resource = manager.resource("counters", {f"k{i}": 0 for i in range(50)})

    async def worker(index: int) -> bool:
        async with manager.session(agent_id=f"a{index}", resources=[resource]) as session:
            await session.write(f"k{index}", index + 1)
            await session.commit()
        return True

    outcomes = await run_concurrently(50, worker)
    assert all(outcome.ok for outcome in outcomes)
    state = await resource.state()
    assert state == {f"k{i}": i + 1 for i in range(50)}


async def test_two_phase_locking_serialises_writers(manager: TransactionManager) -> None:
    from statepact import RetryPolicy

    resource = manager.resource("r", {"k": 0})

    async def increment(index: int) -> None:
        async def work(session) -> None:
            value = await session.read("k")
            await asyncio.sleep(0)
            await session.write("k", value + 1)

        await manager.run(
            f"a{index}",
            [resource],
            work,
            isolation="pessimistic",
            retry_policy=RetryPolicy(max_attempts=100, base_delay=0.001, jitter=0.5),
        )

    await run_concurrently(20, increment)
    assert await resource.read("k") == 20


async def test_deadlock_is_detected_and_one_victim_aborts(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"a": 0, "b": 0})
    both_ready = asyncio.Barrier(2)
    errors: list[BaseException] = []

    async def transaction_one() -> None:
        try:
            async with manager.session(
                agent_id="t1", resources=[resource], isolation="pessimistic"
            ) as session:
                await session.write("a", 1)
                await both_ready.wait()
                await asyncio.sleep(0.01)
                await session.write("b", 1)
                await session.commit()
        except BaseException as exc:
            errors.append(exc)

    async def transaction_two() -> None:
        try:
            async with manager.session(
                agent_id="t2", resources=[resource], isolation="pessimistic"
            ) as session:
                await session.write("b", 2)
                await both_ready.wait()
                await asyncio.sleep(0.01)
                await session.write("a", 2)
                await session.commit()
        except BaseException as exc:
            errors.append(exc)

    await asyncio.gather(transaction_one(), transaction_two())
    assert len(errors) == 1
    assert isinstance(errors[0], DeadlockError)
    # The survivor committed both writes atomically.
    state = await resource.state()
    assert set(state.values()) in ({1}, {2})
    assert manager.backend.lock_stats.deadlocks == 1
    assert manager.backend.lock_stats.to_dict()["waits"] >= 1


async def test_lock_timeout_is_reported(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    holder = manager.session(agent_id="holder", resources=[resource], isolation="pessimistic")
    waiter = manager.session(
        agent_id="waiter",
        resources=[resource],
        isolation="pessimistic",
        lock_timeout=0.05,
    )
    async with holder:
        await holder.write("k", 2)
        async with waiter:
            with pytest.raises(LockTimeoutError):
                await waiter.write("k", 3)
    # The holder released on exit, and the timed-out waiter holds nothing.
    async with manager.session(
        agent_id="third", resources=[resource], isolation="pessimistic", lock_timeout=0.2
    ) as third:
        await third.write("k", 4)
        await third.commit()
    assert await resource.read("k") == 4


async def test_locks_released_when_user_code_raises(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    with pytest.raises(RuntimeError):
        async with manager.session(
            agent_id="crasher", resources=[resource], isolation="pessimistic"
        ) as session:
            await session.write("k", 2)
            raise RuntimeError("tool failed")
    assert manager.backend.lock_stats.to_dict()["acquisitions"] >= 1
    assert manager.backend.lock_snapshot()["locks"] == {}
    async with manager.session(
        agent_id="next", resources=[resource], isolation="pessimistic", lock_timeout=0.2
    ) as session:
        await session.write("k", 3)
        await session.commit()
    assert await resource.read("k") == 3


async def test_pessimistic_conflict_still_aborts_safely(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    optimistic = manager.session(agent_id="o", resources=[resource])
    pessimistic = manager.session(agent_id="p", resources=[resource], isolation="pessimistic")
    async with optimistic, pessimistic:
        await optimistic.read("k")
        await pessimistic.read("k")
        await optimistic.write("k", 2)
        await optimistic.commit()
        await pessimistic.write("k", 3)
        with pytest.raises(ConflictError):
            await pessimistic.commit()
    assert await resource.read("k") == 2
    assert manager.backend.lock_snapshot()["locks"] == {}
