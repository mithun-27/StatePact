"""Replay an ambiguous commit without applying it twice.

The first transaction commits, but imagine the process dies before it sees the
reply. The retry uses the same idempotency key, so the backend returns the
original commit result instead of decrementing again.

Run with::

    python examples/idempotency.py
"""

from __future__ import annotations

import asyncio

from statepact import TransactionManager


async def main() -> None:
    manager = TransactionManager()
    wallet = manager.resource("wallet", {"balance": 100})
    await wallet.ensure(manager)

    async def charge_once() -> str:
        async with manager.session(
            agent_id="billing-agent",
            resources=[wallet],
            idempotency_key="invoice-42",
        ) as session:
            balance = await session.read("balance")
            await session.write("balance", balance - 30)
            result = await session.commit()
            return "replayed" if result.replayed else "applied"

    print("first:", await charge_once())
    print("retry:", await charge_once())
    print("balance:", await wallet.read("balance"))
    assert await wallet.read("balance") == 70
    await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
