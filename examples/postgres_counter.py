"""Run a transactional counter against PostgreSQL.

Start the bundled services with ``docker compose up -d postgres`` and export::

    STATEPACT_DATABASE_URL=postgresql://statepact:statepact@localhost:5432/statepact

Run with::

    pip install "statepact[postgres]"
    python examples/postgres_counter.py
"""

from __future__ import annotations

import asyncio
import os
import uuid

from statepact import RetryPolicy, TransactionManager
from statepact.storage.postgres import PostgresBackend


async def main() -> None:
    dsn = os.environ.get(
        "STATEPACT_DATABASE_URL",
        "postgresql://statepact:statepact@localhost:5432/statepact",
    )
    backend = await PostgresBackend.connect(dsn)
    manager = TransactionManager(backend)
    # A fresh name keeps the example repeatable without deleting existing data.
    counter = manager.resource(f"counter-{uuid.uuid4().hex[:8]}", {"value": 0})
    await counter.ensure(manager)

    async def increment(agent: int) -> None:
        async def work(session) -> None:
            value = await session.read("value")
            await session.write("value", value + 1)

        await manager.run(
            f"agent-{agent}",
            [counter],
            work,
            retry_policy=RetryPolicy(max_attempts=50, base_delay=0.0, jitter=0.0),
        )

    await asyncio.gather(*(increment(index) for index in range(20)))
    print("counter:", await counter.read("value"))
    assert await counter.read("value") == 20
    await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
