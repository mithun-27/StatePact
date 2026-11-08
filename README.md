# StatePact

### Transaction management for autonomous AI agents

**Concurrency control for shared agent state — with optimistic validation, MVCC, strict two-phase locking, serializability checks, retries, idempotency, and lease fencing.**

[![CI](https://github.com/mithun-27/StatePact/actions/workflows/ci.yml/badge.svg)](https://github.com/mithun-27/StatePact/actions/workflows/ci.yml)
![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)
[![Live Playground](https://img.shields.io/badge/Live%20Playground-Try%20it-6D5EFC)](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground)
[![Apache 2.0 License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
![Alpha](https://img.shields.io/badge/Status-Alpha-orange.svg)

[**Live Concurrency Playground ↗**](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground) · [Documentation](docs/index.md) · [Report an issue](https://github.com/mithun-27/StatePact/issues)

---

> **The problem:** Multiple autonomous agents can read the same state, make decisions independently, and overwrite one another's updates. StatePact brings transaction-style concurrency control to that shared state.

## Table of contents

- [Why StatePact?](#why-statepact)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Isolation modes](#isolation-modes)
- [Failure and retry semantics](#failure-and-retry-semantics)
- [Idempotency](#idempotency)
- [Leases and fencing tokens](#leases-and-fencing-tokens)
- [Storage backends](#storage-backends)
- [Agent framework integrations](#agent-framework-integrations)
- [Benchmarks](#benchmarks)
- [API overview](#api-overview)
- [Documentation](#documentation)
- [Development](#development)
- [Project status and limitations](#project-status-and-limitations)
- [What StatePact is not](#what-statepact-is-not)
- [License](#license)

## Why StatePact?

Imagine three AI agents managing the last item in an online store:

1. Agent A reads `stock = 1`.
2. Agent B reads `stock = 1`.
3. Both decide the item is available.
4. Both write `stock = 0`.

Two reservations have now claimed one item. This is a classic **race condition**, and it becomes increasingly important as agents execute tools and modify shared state concurrently.

Framework checkpointers persist messages and workflow state, but they do not automatically provide conflict detection across concurrent tool calls. **StatePact adds that missing coordination layer.**

### What you get

| Capability | What it does |
| --- | --- |
| **Isolation modes** | Choose `optimistic`, `snapshot`, `serializable`, `pessimistic`, or explicitly unsafe `none` |
| **Conflict-aware retries** | Re-run the entire transaction against a fresh snapshot after a retryable conflict |
| **Idempotent commits** | Replay an operation without applying its writes twice |
| **Leases and fencing** | Prevent an expired worker from writing after a newer lease owner takes over |
| **Multiple backends** | Use process-local memory, PostgreSQL, or Redis |
| **Framework integrations** | Connect LangGraph, CrewAI, AutoGen, or a custom agent loop |

StatePact is framework-independent. Framework adapters are optional wrappers around the same transaction engine.

## Quick start

### Installation

**Python 3.12+** is required. Version `0.1.0` is prepared and tested, but is not yet published on PyPI while Trusted Publishing is being configured. Install from source for now:

```bash
git clone https://github.com/mithun-27/StatePact.git
cd StatePact

# Core package — in-memory backend
python -m pip install .

# Optional backends and extras
python -m pip install ".[postgres]"
python -m pip install ".[redis]"
python -m pip install ".[demo]"
```

The core dependency is Pydantic 2. PostgreSQL and Redis support are optional extras. For development:

```bash
uv sync --extra dev
```

### Your first transaction

```python
import asyncio

from statepact import RetryPolicy, TransactionManager


async def main() -> None:
    manager = TransactionManager()

    inventory = manager.resource("inventory", {"sku-1": 10})
    await inventory.ensure(manager)

    async def reserve(session) -> bool:
        stock = await session.read("sku-1")

        # Simulate work between reading and writing.
        await asyncio.sleep(0)

        if stock <= 0:
            return False

        await session.write("sku-1", stock - 1)
        return True

    reserved = await manager.run(
        "agent-1",
        [inventory],
        reserve,
        retry_policy=RetryPolicy(max_attempts=100),
    )

    print("Reserved:", reserved)
    print("Remaining stock:", await inventory.read("sku-1"))

    await manager.close()


asyncio.run(main())
```

`manager.run()` opens a fresh session for each attempt and retries only retryable conflicts.

For explicit transaction control:

```python
async with manager.session(
    agent_id="planner",
    resources=[inventory],
) as session:
    stock = await session.read("sku-1")
    await session.write("sku-1", stock - 1)

    result = await session.commit()

# Exiting a session without committing aborts the transaction.
```

More runnable examples are available in [`examples/`](examples/): inventory, idempotency, leases, and a PostgreSQL counter.

## How it works

A session stages reads and writes. It does **not** apply state directly. At commit time, StatePact builds a `CommitPlan`; the selected backend validates the plan and applies it atomically, or rejects it with a classified conflict.

```text
 AgentSession
      │
      │ staged reads / writes
      ▼
 Transaction
      │
      ▼
   CommitPlan
      │
      ├── OCC validation
      ├── 2PL lock manager
      ├── serializability audit
      └── retry / idempotency
      │
      ▼
 StorageBackend
      ├── MemoryBackend
      ├── PostgresBackend
      └── RedisBackend

 Supporting components:
   • event bus / transaction log / metrics
   • lease + fencing token
```

This separation allows the same transaction semantics to be tested against three different storage engines.

## Isolation modes

StatePact offers five isolation modes:

| Mode | Read behavior | Commit validation | Guarantees for tracked keys | Best suited for |
| --- | --- | --- | --- | --- |
| `none` | Latest | None | No guarantees; lost updates are expected | Baselines and demos only |
| `snapshot` | MVCC snapshot | Write base versions (first-committer-wins) | Prevents lost updates; write skew is possible | High throughput when skew is acceptable |
| `optimistic` | MVCC snapshot | Every read and write base version | Serializable for the tracked key set | General default |
| `serializable` | MVCC snapshot | Optimistic validation plus serialization-graph audit | Same guarantee as `optimistic`, with cycle diagnostics | Audits and higher assurance |
| `pessimistic` | Locked reads | Strict 2PL, deadlock detection, and timeout | Serializable for locked keys | Hot keys and predictable contention |

> [!IMPORTANT]
> Reads are tracked at **key granularity**. Predicate and range reads (for example, `SELECT ... WHERE`) are not tracked. A transaction that depends on "no rows matched" is therefore not protected from a concurrent insert.

Read-only commits skip validation and do not consume a version.

The included write-skew benchmark uses the classic two-on-call-doctors scenario:

- `serializable` rejects one transaction in **20/20 trials**.
- `snapshot` commits both transactions and violates the invariant in **20/20 trials**.

## Failure and retry semantics

Every transaction error should answer three questions:

- Did the transaction commit?
- Is retrying safe?
- Which resource or key failed?

A `ConflictError` always means **the transaction did not commit**.

| Exception | Retryable? | Meaning |
| --- | :---: | --- |
| `ReadWriteConflict` | Yes | A key read by this transaction changed before commit |
| `WriteWriteConflict` | Yes | Another transaction wrote the key first |
| `StaleSnapshotError` | Yes | The snapshot is no longer valid |
| `DeadlockError` | Yes | A wait-for cycle was broken by aborting this transaction |
| `LockTimeoutError` | Yes | Lock acquisition timed out |
| `LeaseLostError` | **No** | The fencing token is stale; re-acquire the lease |
| `SerializationError` | Yes | A serialization cycle was detected |
| `RetryExhaustedError` | No | The retry policy gave up |
| `IdempotencyMismatchError` | No | An idempotency key was reused with a different payload |
| `StorageError` | No | Backend or transport failure; commit state may be unknown |

Retryable conflicts are safe to retry because the transaction did not commit. `LeaseLostError` requires acquiring a new lease first.

Unknown commit outcomes, such as transport failures, are recorded as `FAILED` and are not automatically retried. Use an `idempotency_key` to make resubmission safe.

## Idempotency

Pass an `idempotency_key` to a session or to `manager.run()`. Replaying the same key returns the original `CommitResult` with `replayed=True`; the writes are not applied again.

The default fingerprint is **value-insensitive**. It hashes the agent ID and write footprint (`resource`, `key`, `op`). This is useful for read-modify-write retries: the newly computed value may differ, while the logical operation remains the same.

For strict payload binding, provide `idempotency_fingerprint` explicitly.

### External side effects

External effects use a separate journal through `IdempotentExecutor` and `MemoryEffectStore`, with these transitions:

```text
PENDING → EXECUTING → SUCCEEDED | FAILED
```

The journal separates reservation from execution. It provides at-most-once **completion**, not a distributed transaction. For details, see [Idempotency and side effects](docs/idempotency.md).

## Leases and fencing tokens

A lease by itself is not enough. A worker can pause, lose its lease, and wake up after another worker has taken ownership.

StatePact attaches a fencing token to writes made under a lease. The backend rejects any token older than the highest token it has observed for that resource.

```python
lease = await manager.lease(
    "job",
    owner="worker-a",
    ttl_seconds=30,
)

async with manager.session(
    agent_id="worker-a",
    resources=[job],
    fence=lease.token,
) as session:
    ...
```

Tokens are monotonic high-water marks per resource. They are never reused, even after release or expiry.

See [Leases and fencing](docs/leases.md).

## Storage backends

| Feature | `MemoryBackend` | `PostgresBackend` | `RedisBackend` |
| --- | --- | --- | --- |
| Scope | One process | Multi-process | Multi-process |
| Durable | No | Yes — tables | Yes — key prefix |
| Commit atomicity | `asyncio` lock | DB transaction + advisory lock | Lua script |
| MVCC history | In-memory, garbage-collected | `statepact_history` | Capped Redis lists |
| Pessimistic locks | Wait-for graph | Polling + timeout | Lua + TTL |
| Leases / fencing | In-memory | Durable | Durable |
| Idempotency | In-memory | Durable | TTL-based |

### PostgreSQL

PostgreSQL creates its schema on `connect()`. The commit path serializes validation and application behind one `pg_advisory_xact_lock`, making cross-process commits linearizable at the cost of throughput.

```python
from statepact import TransactionManager
from statepact.storage.postgres import PostgresBackend

backend = await PostgresBackend.connect(
    "postgresql://user:pass@host/db"
)
manager = TransactionManager(backend)
```

Install the optional dependency with:

```bash
python -m pip install ".[postgres]"
```

### Redis

Redis executes each commit as a single Lua script. Install its optional dependency with:

```bash
python -m pip install ".[redis]"
```

The memory backend is the semantic reference used by the test suite. It is **not** safe for coordination across processes.

See [Backend details](docs/backends.md) for schemas, key layouts, and trade-offs.

## Agent framework integrations

Framework adapters live in `statepact.integrations`. They do not import their optional framework dependencies at module load.

### LangGraph example

```python
from statepact.integrations.langgraph import LangGraphCoordinator

coordinator = LangGraphCoordinator(
    manager,
    agent_id="researcher",
    resources=[inventory],
)


@coordinator.node
async def reserve(state, session):
    stock = await session.read("sku-1")

    if stock == 0:
        return {"status": "sold_out"}

    await session.write("sku-1", stock - 1)
    return {"status": "reserved"}


graph.add_node("reserve", reserve)
```

### Available adapters

| Integration | Adapter | Purpose |
| --- | --- | --- |
| **LangGraph** | `LangGraphCoordinator` | Wrap graph nodes and merge returned patches after commit |
| **CrewAI** | `transaction_tool(...)` / `TransactionTool` | Synchronous tool for an atomic read-modify-write |
| **AutoGen** | `AutoGenTransactionMiddleware` | Transactional message handler with optional idempotency prefix |
| **Custom agents** | `run_transaction`, `read_modify_write`, `transactional` | Reusable helpers for any framework |

See [Integrations](docs/integrations.md).

## Benchmarks

*Measured on 2026-11-07 using the benchmark runner in this repository. Raw artifact: [run-20261107T051143Z.json](benchmarks/results/run-20261107T051143Z.json) (`latest.json` is identical).*

**Test environment**

- Windows 11 `10.0.26300`
- Python 3.12.13, AMD64, 12-logical-CPU VM
- PostgreSQL 16 and Redis 7 in Docker Desktop on the same host
- 40 agents, 10 operations each (400 commits), and 20 write-skew trials
- `asyncio.sleep(0)` inserted between read and write to guarantee interleaving

### Throughput and retry amplification

Throughput is measured in committed operations per second. The value in parentheses is average attempts per operation.

| Scenario | Memory | PostgreSQL | Redis |
| --- | ---: | ---: | ---: |
| Independent keys | 1,534 (1.00) | 53.7 (1.00) | 229.4 (1.00) |
| Single hot key, zero backoff | 344 (10.75) | 7.7 (13.56) | 18.8 (20.59) |
| Single hot key, jittered backoff | 241 (3.17) | 7.8 (12.90) | 26.8 (14.15) |
| Read-only commits | 6,619 | 133.4 | 427.4 |
| Lease + commit (sequential) | 3,013 | 18.3 | 88.9 |

### Correctness under the same load

| Check | Memory | PostgreSQL | Redis |
| --- | ---: | ---: | ---: |
| `none`: counter after 400 increments | 10 ❌ | 24 ❌ | 10 ❌ |
| `optimistic`: counter after 400 increments | 400 ✅ | 400 ✅ | 400 ✅ |
| `snapshot`: write-skew violations | 20/20 ❌ | 20/20 ❌ | 20/20 ❌ |
| `serializable`: write-skew violations | 0/20 ✅ | 0/20 ✅ | 0/20 ✅ |

### Interpreting the results

- **Correctness is the key result.** With `none`, 376–390 of 400 updates are lost. With `optimistic`, all 400 increments are preserved.
- **Hot-key throughput is a worst-case workload.** Forty agents contend on one key with immediate retries. Each retry is a full commit attempt, so serialized backends such as PostgreSQL pay a round-trip cost per conflict.
- **For production hot keys,** consider sharding hot resources or using `pessimistic` locking with jitter.
- **PostgreSQL is correctness-first in this implementation.** A global advisory lock per commit and several SQL round trips limit throughput. It is not a throughput-optimized commit engine.
- **These are not comparisons against other libraries.** They document this engine's behavior on one machine.

Re-run the benchmark on your machine:

```bash
docker compose up -d postgres redis

STATEPACT_TEST_POSTGRES_URL=postgresql://statepact:statepact@localhost:5432/statepact \
STATEPACT_TEST_REDIS_URL=redis://localhost:6379/0 \
python benchmarks/run.py \
  --backends memory,postgres,redis \
  --agents 40 \
  --ops 10 \
  --trials 20
```

The runner is in-process by design: it measures the transaction engine rather than an HTTP server, so Locust is not used.

See [Benchmark methodology](docs/benchmarks.md).

## API overview

| Component | Main responsibilities |
| --- | --- |
| `TransactionManager` | `resource`, `session`, `run`, `run_outcome`, `lease`, `health_check`, `close`; exposes `transactions` and `stats` |
| `AgentSession` | `read`, `get`, `get_entry`, `version`, `write`, `delete`, `commit`, `abort`; exposes `state`, `read_set`, `write_set`, `commit_result` |
| `SharedResource` | Diagnostic reads: `read`, `get`, `version`, `versions`, `history`, `state`, `snapshot` |
| `RetryPolicy` | `max_attempts`, `base_delay`, `max_delay`, `multiplier`, `jitter`, `retry_on` |
| Observability | `EventBus`, `TransactionLog`, `Metrics`, and optional OpenTelemetry bridge |

Full reference: [API documentation](docs/api.md).

## Documentation

Start with the [documentation index](docs/index.md).

| Guide | Description |
| --- | --- |
| [Quickstart](docs/quickstart.md) | Get a transaction running |
| [Concepts](docs/concepts.md) | Core StatePact concepts |
| [Isolation and anomalies](docs/isolation.md) | Isolation modes and concurrency anomalies |
| [Backends](docs/backends.md) | Storage schemas and backend trade-offs |
| [Leases and fencing](docs/leases.md) | Lease ownership and stale-worker protection |
| [Idempotency and side effects](docs/idempotency.md) | Safe replays and external effects |
| [Retries](docs/retries.md) | Retry behavior and policies |
| [Integrations](docs/integrations.md) | Agent framework adapters |
| [Benchmarks](docs/benchmarks.md) | Methodology and results |
| [Operations](docs/operations.md) | Operational guidance |
| [Limitations](docs/limitations.md) | Known limitations |

## Development

Set up the development environment:

```bash
uv sync --extra dev
```

Run the unit tests and static checks:

```bash
uv run pytest -q
uv run ruff check src tests demo benchmarks
uv run mypy src
```

The test suite currently contains **121 tests**. Fourteen integration tests are skipped unless service URLs are configured.

Run PostgreSQL and Redis integration tests:

```bash
docker compose up -d postgres redis

STATEPACT_TEST_POSTGRES_URL=postgresql://statepact:statepact@localhost:5432/statepact \
STATEPACT_TEST_REDIS_URL=redis://localhost:6379/0 \
uv run pytest tests/integration -q
```

The integration suite runs the same behavioral contract against PostgreSQL and Redis, including conflicts, idempotent replay, leases and fencing, lock timeouts, write skew, and a 20-agent concurrent increment.

## Project status and limitations

**Current release: `0.1.0` — Alpha**

The transaction semantics, tests, and benchmarks are implemented. The API may change before `1.0`.

Known limitations include:

- Predicate and range reads are not tracked.
- The PostgreSQL backend's global commit lock limits throughput.
- The serializability audit is process-local.
- External side effects require cooperation from the external system for idempotency.

See [Known limitations](docs/limitations.md) before using StatePact in production.

## What StatePact is not

- **Not a workflow or checkpoint engine.** Use tools such as LangGraph or Temporal for workflow orchestration. StatePact coordinates shared mutable state, not message history.
- **Not a vector store or a database.** PostgreSQL and Redis store coordination state; your domain data can live elsewhere.
- **Not exactly-once delivery to arbitrary external APIs.** The effect journal provides at-most-once completion when the external action is idempotent or accepts an idempotency key.
- **Not a multi-database distributed transaction protocol.**

## License

StatePact is released under the [Apache-2.0 License](LICENSE).

---

**StatePact — safer shared state for concurrent AI agents.**

[Explore the repository](https://github.com/mithun-27/StatePact) · [Try the live playground](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground)
