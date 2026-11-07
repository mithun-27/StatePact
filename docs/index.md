# AgentCommit

Transaction management for autonomous AI agents.

AgentCommit gives a multi-agent system one thing most frameworks leave out: a
correct boundary around **shared mutable state**. Agents read a value, reason
about it, and write it back. Without conflict detection, concurrent agents lose
updates, oversell inventory, double-apply side effects and violate invariants.

```python
async with manager.session(agent_id="planner", resources=[inventory]) as session:
    stock = await session.read("sku-1")
    if stock <= 0:
        return "OUT_OF_STOCK"
    await session.write("sku-1", stock - 1)
    await session.commit()
```

The session stages reads and writes. At commit, the backend validates the read
set and write bases against the latest committed versions and applies all writes
atomically, or rejects the transaction with a classified conflict. A retry
re-runs the whole body on a fresh snapshot.

## What you get

| Capability | Where |
|---|---|
| Snapshot reads and atomic commits | `TransactionManager`, `AgentSession` |
| `optimistic`, `snapshot`, `serializable`, `pessimistic`, `none` isolation | [Isolation](isolation.md) |
| Conflict retries with backoff, jitter and replanning | [Retries](retries.md) |
| Idempotent commit replay | [Idempotency](idempotency.md) |
| Leases with monotonic fencing tokens | [Leases](leases.md) |
| In-memory, PostgreSQL and Redis backends | [Backends](backends.md) |
| LangGraph, CrewAI and AutoGen adapters | [Integrations](integrations.md) |
| Events, transaction log, metrics, OpenTelemetry bridge | [Operations](operations.md) |

## Install

```bash
git clone https://github.com/mithun-27/AgentCommit
cd AgentCommit
python -m pip install ".[postgres,redis]"
```

Python 3.12+ is required. The core dependency is Pydantic 2; backend drivers are
optional extras. A 0.1.0 PyPI release is pending, so install from source for now.

## Five-minute tour

```python
import asyncio
from agentcommit import RetryPolicy, TransactionManager

async def main() -> None:
    manager = TransactionManager()                       # in-memory backend
    counter = manager.resource("counter", {"value": 0})
    await counter.ensure(manager)

    async def increment(session) -> None:
        value = await session.read("value")
        await asyncio.sleep(0)                           # concurrent agents interleave
        await session.write("value", value + 1)

    async def agent(index: int) -> None:
        await manager.run(
            f"agent-{index}",
            [counter],
            increment,
            retry_policy=RetryPolicy(max_attempts=200),
        )

    await asyncio.gather(*(agent(i) for i in range(50)))
    print(await counter.read("value"))                    # 50
    await manager.close()

asyncio.run(main())
```

Next: [Quickstart](quickstart.md) → [Concepts](concepts.md) →
[Isolation](isolation.md).

## Design principles

1. **Explicit over implicit.** There is no auto-commit. Exiting a session without
   `commit()` aborts. A failed commit is never assumed applied.
2. **Classified failures.** Every exception says whether the transaction
   committed and whether retrying is safe.
3. **The backend applies state.** Sessions build a commit plan; backends validate
   and apply atomically. The same behavioural contract runs against memory,
   PostgreSQL and Redis.
4. **Honest limits.** Predicate reads, cross-database transactions and arbitrary
   external side effects are not magic. They are documented in
   [Limitations](limitations.md).

## Status

0.1.0 is an alpha. The engine, integration tests against real PostgreSQL/Redis,
property tests and benchmarks are real and reproducible. The public API may change
before 1.0.
