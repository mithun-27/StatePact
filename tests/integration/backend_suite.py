"""Shared behavioural suite for durable backends.

The suite is backend-agnostic: each integration module supplies a connected
backend instance and calls these helpers. Keeping the assertions in one place
prevents PostgreSQL and Redis from drifting from the in-memory contract.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from statepact import (
    ConflictError,
    LeaseLostError,
    LockTimeoutError,
    RetryPolicy,
    TransactionManager,
)
from statepact.core.interfaces import StorageBackend


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


async def exercise_resource_lifecycle(backend: StorageBackend) -> None:
    name = _name("lifecycle")
    await backend.ensure_resource(name, {"k": 1}, {"owner": "test"})
    await backend.ensure_resource(name, {"k": 999}, {"extra": True})  # never overwrites
    manager = TransactionManager(backend)
    resource = manager.resource(name)

    assert await resource.read("k") == 1
    async with manager.session(agent_id="a", resources=[resource]) as session:
        assert await session.read("k") == 1
        await session.write("k", 2)
        result = await session.commit()
    assert result.committed is True
    assert result.commit_version is not None

    assert await resource.read("k") == 2
    snapshot = await backend.snapshot(name)
    try:
        assert snapshot.version_of("k") == result.commit_version
    finally:
        snapshot.release()

    history = await backend.history(name, "k", limit=10)
    assert [entry.value for entry in history][:2] == [2, 1]
    assert name in await backend.list_resources()
    assert await backend.health_check() is True


async def exercise_optimistic_conflict(backend: StorageBackend) -> None:
    name = _name("conflict")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"k": 1})
    s1 = manager.session(agent_id="s1", resources=[resource])
    s2 = manager.session(agent_id="s2", resources=[resource])
    async with s1, s2:
        assert await s1.read("k") == 1
        assert await s2.read("k") == 1
        await s1.write("k", 2)
        await s2.write("k", 3)
        await s1.commit()
        with pytest.raises(ConflictError):
            await s2.commit()
    assert await resource.read("k") == 2


async def exercise_idempotent_replay(backend: StorageBackend) -> None:
    name = _name("idem")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"k": 10})
    async with manager.session(
        agent_id="a", resources=[resource], idempotency_key="op-1"
    ) as session:
        await session.write("k", await session.read("k") - 1)
        first = await session.commit()
    async with manager.session(
        agent_id="a", resources=[resource], idempotency_key="op-1"
    ) as session:
        await session.write("k", await session.read("k") - 1)
        second = await session.commit()
    assert first.replayed is False
    assert second.replayed is True
    assert second.commit_version == first.commit_version
    assert await resource.read("k") == 9


async def exercise_lease_fencing(backend: StorageBackend) -> None:
    name = _name("lease")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"k": 0})
    await resource.ensure(manager)

    old = await manager.lease(name, "old-owner", ttl_seconds=0.5)
    assert await backend.release_lease(old) is True
    new = await manager.lease(name, "new-owner", ttl_seconds=30)
    assert new.token > old.token
    assert await backend.current_fence(name) == new.token

    with pytest.raises(LeaseLostError):
        async with manager.session(
            agent_id="old-owner", resources=[resource], fence=old.token
        ) as s:
            await s.write("k", 1)
            await s.commit()

    async with manager.session(agent_id="new-owner", resources=[resource], fence=new.token) as s:
        await s.write("k", 2)
        await s.commit()
    assert await resource.read("k") == 2


async def exercise_pessimistic_lock_timeout(backend: StorageBackend) -> None:
    name = _name("locks")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"k": 0})
    holder = manager.session(agent_id="holder", resources=[resource], isolation="pessimistic")
    waiter = manager.session(
        agent_id="waiter",
        resources=[resource],
        isolation="pessimistic",
        lock_timeout=0.2,
    )
    async with holder:
        await holder.write("k", 1)
        async with waiter:
            with pytest.raises(LockTimeoutError):
                await waiter.read("k")
        await holder.commit()
    assert await resource.read("k") == 1


async def exercise_serializable_write_skew(backend: StorageBackend) -> None:
    name = _name("skew")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"alice": True, "bob": True})
    s1 = manager.session(agent_id="a1", resources=[resource], isolation="serializable")
    s2 = manager.session(agent_id="a2", resources=[resource], isolation="serializable")
    async with s1, s2:
        assert await s1.read("alice")
        assert await s2.read("bob")
        if await s1.read("bob"):
            await s1.write("alice", False)
        if await s2.read("alice"):
            await s2.write("bob", False)
        await s1.commit()
        with pytest.raises(ConflictError):
            await s2.commit()
    assert await resource.read("alice") is False
    assert await resource.read("bob") is True


async def exercise_concurrent_increment(backend: StorageBackend, agents: int = 20) -> None:
    name = _name("counter")
    manager = TransactionManager(backend)
    resource = manager.resource(name, {"k": 0})

    async def increment() -> int:
        async def work(session: Any) -> int:
            value = await session.read("k")
            await session.write("k", value + 1)
            return value + 1

        return await manager.run(
            "agent",
            [resource],
            work,
            retry_policy=RetryPolicy(max_attempts=200, base_delay=0.0, jitter=0.0),
        )

    await asyncio.gather(*(increment() for _ in range(agents)))
    assert await resource.read("k") == agents
