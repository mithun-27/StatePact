"""Reserve inventory from many agents without overselling.

Run with::

    python examples/inventory.py
"""

from __future__ import annotations

import asyncio

from statepact import RetryPolicy, TransactionManager


async def main() -> None:
    manager = TransactionManager()  # in-memory backend
    inventory = manager.resource("inventory", {"sku-1": 10})
    await inventory.ensure(manager)

    accepted = 0

    async def agent(index: int) -> None:
        nonlocal accepted

        async def reserve(session) -> bool:
            stock = await session.read("sku-1")
            await asyncio.sleep(0)  # an agent "thinking" between read and write
            if stock <= 0:
                return False
            await session.write("sku-1", stock - 1)
            return True

        ok = await manager.run(
            f"agent-{index}",
            [inventory],
            reserve,
            retry_policy=RetryPolicy(max_attempts=200),
        )
        accepted += int(ok)

    await asyncio.gather(*(agent(index) for index in range(50)))
    print(f"accepted={accepted} remaining={await inventory.read('sku-1')}")
    assert accepted == 10
    assert await inventory.read("sku-1") == 0
    await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
