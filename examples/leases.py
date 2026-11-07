"""Leases plus fencing tokens: the old owner cannot write after losing the lease.

Run with::

    python examples/leases.py
"""

from __future__ import annotations

import asyncio

from statepact import LeaseLostError, TransactionManager


async def main() -> None:
    manager = TransactionManager()
    job = manager.resource("job", {"attempt": 0})
    await job.ensure(manager)

    old = await manager.lease("job", owner="worker-a", ttl_seconds=30)
    await manager.backend.release_lease(old)

    new = await manager.lease("job", owner="worker-b", ttl_seconds=30)
    print(f"old token={old.token} new token={new.token}")

    try:
        async with manager.session(
            agent_id="worker-a", resources=[job], fence=old.token
        ) as session:
            await session.write("attempt", 1)
            await session.commit()
    except LeaseLostError as exc:
        print("stale owner rejected:", exc)

    async with manager.session(
        agent_id="worker-b", resources=[job], fence=new.token
    ) as session:
        await session.write("attempt", 2)
        await session.commit()

    print("attempt:", await job.read("attempt"))
    assert await job.read("attempt") == 2
    await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
