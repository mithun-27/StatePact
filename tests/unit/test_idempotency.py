from __future__ import annotations

import pytest

from agentcommit import (
    EffectFailedError,
    EffectInProgressError,
    EffectState,
    IdempotencyMismatchError,
    IdempotentExecutor,
    MemoryEffectStore,
    TransactionManager,
)


async def test_idempotent_commit_replays_without_double_apply(manager: TransactionManager) -> None:
    resource = manager.resource("inventory", {"sku": 5})
    async with manager.session(
        agent_id="a", resources=[resource], idempotency_key="order-42"
    ) as session:
        stock = await session.read("sku")
        await session.write("sku", stock - 1)
        first = await session.commit()
    assert first.replayed is False
    assert await resource.read("sku") == 4

    async with manager.session(
        agent_id="a", resources=[resource], idempotency_key="order-42"
    ) as session:
        stock = await session.read("sku")
        await session.write("sku", stock - 1)
        second = await session.commit()
    assert second.replayed is True
    assert second.commit_version == first.commit_version
    assert await resource.read("sku") == 4


async def test_idempotency_key_reuse_with_different_payload_fails(
    manager: TransactionManager,
) -> None:
    resource = manager.resource("r", {"k": 1, "other": 1})
    async with manager.session(agent_id="a", resources=[resource], idempotency_key="k1") as s:
        await s.write("k", 2)
        await s.commit()
    # Same key, different *operation* (a different key/op footprint) is a bug.
    with pytest.raises(IdempotencyMismatchError):
        async with manager.session(agent_id="a", resources=[resource], idempotency_key="k1") as s:
            await s.write("other", 3)
            await s.commit()
    assert await resource.read("k") == 2
    assert await resource.read("other") == 1


async def test_idempotent_replay_accepts_a_different_derived_value(
    manager: TransactionManager,
) -> None:
    """A retried read-modify-write must replay, not be rejected as a mismatch.

    The agent re-reads, so it computes a different new value. The logical
    operation is the same, and the first commit already applied it.
    """

    resource = manager.resource("r", {"k": 5})
    async with manager.session(agent_id="a", resources=[resource], idempotency_key="op-9") as s:
        await s.write("k", await s.read("k") - 1)
        first = await s.commit()
    async with manager.session(agent_id="a", resources=[resource], idempotency_key="op-9") as s:
        await s.write("k", await s.read("k") - 1)
        second = await s.commit()
    assert second.replayed is True
    assert second.commit_version == first.commit_version
    assert await resource.read("k") == 4


async def test_explicit_idempotency_fingerprint_binds_the_payload(
    manager: TransactionManager,
) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(
        agent_id="a",
        resources=[resource],
        idempotency_key="strict-1",
        idempotency_fingerprint="transfer:100",
    ) as s:
        await s.write("k", 2)
        await s.commit()
    with pytest.raises(IdempotencyMismatchError):
        async with manager.session(
            agent_id="a",
            resources=[resource],
            idempotency_key="strict-1",
            idempotency_fingerprint="transfer:200",
        ) as s:
            await s.write("k", 3)
            await s.commit()


async def test_effect_executes_once_and_replays_result() -> None:
    store = MemoryEffectStore()
    executor = IdempotentExecutor(store)
    calls = 0

    async def charge() -> str:
        nonlocal calls
        calls += 1
        return "charged"

    assert await executor.run("charge-1", charge) == "charged"
    assert await executor.run("charge-1", charge) == "charged"
    assert calls == 1
    record = await store.get("charge-1")
    assert record is not None
    assert record.state is EffectState.SUCCEEDED


async def test_failed_effect_is_recorded_and_not_retried_by_default() -> None:
    store = MemoryEffectStore()
    executor = IdempotentExecutor(store)

    async def boom() -> None:
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError):
        await executor.run("effect-1", boom)
    record = await store.get("effect-1")
    assert record is not None and record.state is EffectState.FAILED
    with pytest.raises(EffectFailedError):
        await executor.run("effect-1", boom)

    calls = 0

    async def recover() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    assert await executor.run("effect-1", recover, retry_failed=True) == "ok"
    assert calls == 1


async def test_executing_effect_is_not_started_twice() -> None:
    store = MemoryEffectStore()
    executor = IdempotentExecutor(store)
    from datetime import UTC, datetime

    from agentcommit.core.idempotency import EffectRecord

    now = datetime.now(UTC)
    await store.create(EffectRecord(key="busy", state=EffectState.EXECUTING, updated_at=now))
    with pytest.raises(EffectInProgressError):
        await executor.run("busy", lambda: _noop())


async def _noop() -> None:
    return None


async def test_two_phase_reserve_then_execute() -> None:
    store = MemoryEffectStore()
    executor = IdempotentExecutor(store)
    assert await executor.reserve("2pc-1") is True
    assert await executor.reserve("2pc-1") is False
    calls = 0

    async def action() -> int:
        nonlocal calls
        calls += 1
        return 7

    assert await executor.execute_reserved("2pc-1", action) == 7
    assert await executor.execute_reserved("2pc-1", action) == 7
    assert calls == 1
