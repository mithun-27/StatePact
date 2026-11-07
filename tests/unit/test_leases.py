from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from statepact import (
    LeaseLostError,
    LeaseManager,
    LeaseNotAvailableError,
    TransactionManager,
)
from statepact.coordination.fencing import FenceGuard


async def test_lease_acquire_renew_release(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    await resource.ensure(manager)
    backend = manager.backend
    lease = await backend.acquire_lease("r", "agent-1", 30.0)
    assert lease.token == 1
    assert lease.owner == "agent-1"
    renewed = await backend.renew_lease(lease, 60.0)
    assert renewed.token == lease.token
    assert renewed.expires_at >= lease.expires_at
    assert await backend.current_fence("r") == 1
    assert await backend.release_lease(renewed) is True
    assert await backend.release_lease(renewed) is False


async def test_only_one_owner_at_a_time(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    await resource.ensure(manager)
    backend = manager.backend
    lease = await backend.acquire_lease("r", "agent-1", 30.0)
    with pytest.raises(LeaseNotAvailableError):
        await backend.acquire_lease("r", "agent-2", 30.0)
    await backend.release_lease(lease)
    second = await backend.acquire_lease("r", "agent-2", 30.0)
    assert second.token == 2


async def test_expired_lease_cannot_be_renewed(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    await resource.ensure(manager)
    backend = manager.backend
    lease = await backend.acquire_lease("r", "agent-1", 0.01)
    await asyncio.sleep(0.02)
    with pytest.raises(LeaseLostError):
        await backend.renew_lease(lease)


async def test_stale_fence_cannot_write(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    await resource.ensure(manager)
    backend = manager.backend
    old = await backend.acquire_lease("r", "old-owner", 0.01)
    await asyncio.sleep(0.02)
    await backend.acquire_lease("r", "new-owner", 30.0)  # token 2
    with pytest.raises(LeaseLostError):
        async with manager.session(
            agent_id="old-owner", resources=[resource], fence=old.token
        ) as s:
            await s.write("k", 99)
            await s.commit()
    assert await resource.read("k") == 1
    # The current owner can write with its own token.
    current = await backend.current_fence("r")
    async with manager.session(agent_id="new-owner", resources=[resource], fence=current) as s:
        await s.write("k", 2)
        await s.commit()
    assert await resource.read("k") == 2


async def test_lease_manager_hold_and_guard(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    await resource.ensure(manager)
    leases = LeaseManager(manager.backend, ttl_seconds=5.0)
    async with leases.hold("r", "agent-1") as lease:
        await leases.guard(lease)
        assert lease.token >= 1
    lease_after = await manager.backend.current_fence("r")
    assert lease_after >= 1


async def test_fence_guard_rejects_regression() -> None:
    guard = FenceGuard()
    guard.observe("r", 5)
    guard.validate("r", 5)
    guard.validate("r", 6)
    guard.observe("r", 6)
    with pytest.raises(LeaseLostError):
        guard.validate("r", 5)
    assert guard.current("r") == 6
    assert guard.snapshot() == {"r": 6}


def test_lease_expiry_helper() -> None:
    from statepact.core.interfaces import Lease

    now = datetime.now(UTC)
    lease = Lease("r", "owner", 1, now + timedelta(seconds=5), 5.0)
    assert not lease.is_expired(now)
    assert lease.is_expired(now + timedelta(seconds=6))
