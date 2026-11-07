# Quickstart

## Requirements

* Python 3.12 or newer.
* Pydantic 2 (installed automatically).
* PostgreSQL 16+ (`asyncpg`) or Redis 7+ (`redis`) only if you use those backends.

## Install

PyPI publication of 0.1.0 is pending. Install from source:

```bash
git clone https://github.com/mithun-27/AgentCommit
cd AgentCommit
python -m pip install ".[postgres,redis]"
```

For development:

```bash
uv sync --extra dev
```

## 1. In-memory: one transaction

```python
import asyncio
from agentcommit import TransactionManager


async def main() -> None:
    manager = TransactionManager()
    users = manager.resource("users", {"u1": {"credits": 5}})
    await users.ensure(manager)

    async with manager.session(agent_id="billing", resources=[users]) as session:
        user = await session.read("u1")
        user["credits"] -= 1
        await session.write("u1", user)
        result = await session.commit()

    print(result.commit_version, await users.read("u1"))
    await manager.close()


asyncio.run(main())
```

`session.commit()` is explicit. Leaving the `async with` block without it aborts
the transaction, releasing snapshots and any pessimistic locks.

## 2. Retry a conflicting transaction

```python
from agentcommit import RetryPolicy

stock = await manager.run(
    "agent-1",
    [inventory],
    reserve_one_unit,          # async def reserve_one_unit(session): ...
    isolation="optimistic",
    retry_policy=RetryPolicy(max_attempts=100, base_delay=0.002, jitter=0.3),
)
```

`run` opens a fresh session per attempt. Only retryable conflicts are retried by
default. Use `run_outcome` when you need attempt counts, delays and the collected
errors:

```python
outcome = await manager.run_outcome("agent-1", [inventory], reserve_one_unit)
print(outcome.value, outcome.attempts, outcome.errors)
```

To update an agent's plan between attempts, pass `replan`:

```python
async def replan(error, attempt):
    await agent.observe_conflict(error, attempt)

await manager.run("agent-1", [inventory], work, replan=replan)
```

## 3. Make a commit idempotent

```python
async with manager.session(
    agent_id="billing",
    resources=[wallet],
    idempotency_key="invoice-42",
) as session:
    balance = await session.read("balance")
    await session.write("balance", balance - 30)
    result = await session.commit()
    print(result.replayed)     # False first time, True on replay
```

The default fingerprint hashes the agent id and the write footprint, not the
values. A retried read-modify-write therefore replays instead of raising a
mismatch. Pass `idempotency_fingerprint="..."` to bind the full payload
explicitly.

## 4. Use PostgreSQL

```bash
docker compose up -d postgres
export AGENTCOMMIT_DATABASE_URL=postgresql://agentcommit:agentcommit@localhost:5432/agentcommit
```

```python
import os

from agentcommit import TransactionManager
from agentcommit.storage.postgres import PostgresBackend

dsn = os.environ["AGENTCOMMIT_DATABASE_URL"]
backend = await PostgresBackend.connect(dsn)
manager = TransactionManager(backend)
```

The schema is created automatically. Multiple processes can share the database;
commits are validated under one advisory lock.

## 5. Use Redis

```bash
docker compose up -d redis
```

```python
from agentcommit.storage.redis import RedisBackend

backend = await RedisBackend.connect("redis://localhost:6379/0", prefix="myapp")
manager = TransactionManager(backend)
```

Each commit is one Lua script, so validation and application are atomic. All keys
are namespaced under `prefix`.

## 6. Protect a leased job

```python
lease = await manager.lease("job", owner="worker-a", ttl_seconds=30)

async with manager.session(agent_id="worker-a", resources=[job], fence=lease.token):
    ...
    await session.commit()

await backend.renew_lease(lease)       # heartbeat before the TTL expires
await backend.release_lease(lease)
```

A stale token is rejected with `LeaseLostError`; re-acquire the lease before
retrying the write.

## 7. Run the playground

```bash
uv sync --extra demo
uv run python demo/app.py
# open http://localhost:7860
```

The playground runs the real engine in-process and compares isolation modes on
inventory, counter and write-skew scenarios.

## Next steps

* [Concepts](concepts.md) — sessions, resources, versions, commit plans.
* [Isolation](isolation.md) — choose the right mode and understand its anomalies.
* [Backends](backends.md) — deployment trade-offs and schemas.
* [Limitations](limitations.md) — read this before production.
