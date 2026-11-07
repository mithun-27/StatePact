# StatePact

**Transaction management for autonomous AI agents.** Optimistic concurrency, MVCC,
strict 2PL, serializability checking, retries, idempotent commits and
lease/fencing tokens for state that multiple agents read and write.

[![CI](https://github.com/mithun-27/StatePact/actions/workflows/ci.yml/badge.svg)](https://github.com/mithun-27/StatePact/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![Playground](https://img.shields.io/badge/playground-live-6d5efc.svg)](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

```
pip install statepact   # 0.1.0 release is pending; see Install below
```

---

## The problem

An agent reads shared state, thinks, then writes it back:

```python
stock = await session.read("sku-1")   # 1
await think()                         # three other agents also read 1
await session.write("sku-1", stock - 1)
```

Without a transaction boundary, two reservations both write `0`: one unit is sold
twice, or a counter loses updates, or two agents both decide they are the one on
call. Framework checkpointers persist *messages*, not **conflict detection across
concurrent tool calls**. StatePact adds the missing concurrency layer:

* **Isolation you choose** â€” `optimistic`, `snapshot`, `serializable`,
  `pessimistic`, or `none` as an explicitly unsafe baseline.
* **Correct retries** â€” a conflict re-runs the whole transaction on a fresh
  snapshot; an unknown failure is never silently retried.
* **Idempotent commits** â€” the same operation replayed returns the original
  result instead of applying twice.
* **Leases with fencing tokens** â€” a paused worker that wakes up after its lease
  expired cannot overwrite the new owner.
* **Backends you can deploy** â€” process-local memory, PostgreSQL, or Redis.

StatePact is framework-independent. LangGraph, CrewAI and AutoGen adapters are
thin optional wrappers; the same engine works with a custom agent loop.

Try the [live concurrency playground](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground).
It is a browser simulation of the same validation rules (free static hosting),
not the Python engine; the runnable Python demo is in [`demo/`](demo/).

## What it is not

* Not a workflow/checkpoint engine (use LangGraph, Temporal, â€¦). It coordinates
  **shared mutable state**, not message history.
* Not a vector store or a database. PostgreSQL/Redis store the *coordination*
  state; your domain data can live anywhere.
* Not exactly-once delivery to arbitrary external APIs. The effect journal gives
  at-most-once **completion** when the external action is idempotent or accepts an
  idempotency key. See [docs/idempotency.md](docs/idempotency.md).
* Not a multi-database distributed transaction protocol.

## Install

0.1.0 is prepared and tested but not yet on PyPI (Trusted Publishing is being
configured). Install from source in the meantime:

```bash
git clone https://github.com/mithun-27/StatePact
cd StatePact
python -m pip install .            # core, in-memory backend only
python -m pip install ".[postgres]" # + asyncpg
python -m pip install ".[redis]"    # + redis
python -m pip install ".[demo]"     # + Gradio playground
```

Requires Python 3.12+. The core dependency is Pydantic 2. PostgreSQL and Redis are
optional extras. `uv sync --extra dev` sets up the development environment.

## Quickstart

```python
import asyncio
from statepact import RetryPolicy, TransactionManager


async def main() -> None:
    manager = TransactionManager()                 # in-memory backend
    inventory = manager.resource("inventory", {"sku-1": 10})
    await inventory.ensure(manager)

    async def reserve(session) -> bool:
        stock = await session.read("sku-1")
        await asyncio.sleep(0)                     # agent thinking between read and write
        if stock <= 0:
            return False
        await session.write("sku-1", stock - 1)
        return True

    ok = await manager.run(
        "agent-1",
        [inventory],
        reserve,
        retry_policy=RetryPolicy(max_attempts=100),
    )
    print(ok, await inventory.read("sku-1"))
    await manager.close()


asyncio.run(main())
```

`manager.run` opens a fresh session per attempt and retries only retryable
conflicts. For explicit control, open a session yourself:

```python
async with manager.session(agent_id="planner", resources=[inventory]) as session:
    stock = await session.read("sku-1")
    await session.write("sku-1", stock - 1)
    result = await session.commit()     # explicit: exiting without commit aborts
```

Runnable examples: [`examples/`](examples/) â€” inventory, idempotency, leases,
PostgreSQL counter.

## Isolation modes

| Mode | Reads | Commit validation | Guarantees for tracked keys | Use when |
|---|---|---|---|---|
| `none` | latest | none | **none** â€” lost updates are expected | baseline/demo only |
| `snapshot` | MVCC snapshot | write base versions (first-committer-wins) | no lost updates; **write skew possible** | high throughput, skew tolerated |
| `optimistic` | MVCC snapshot | every read + write base version | serializable for the tracked key set | default |
| `serializable` | MVCC snapshot | `optimistic` + serialization-graph audit | same as `optimistic`, with cycle diagnostics | audit/high assurance |
| `pessimistic` | locked reads | strict 2PL, deadlock detection/timeout | serializable for locked keys | hot keys, predictable contention |

Reads are tracked at **key granularity**. Predicate/range reads (`SELECT â€¦ WHERE`)
are not tracked, so a transaction that depends on "no rows matched" is not
protected from a concurrent insert. Read-only commits skip validation and do not
consume a version.

The benchmark demonstrates the difference on the classic write-skew anomaly
(two on-call doctors): `serializable` rejects one transaction in 20/20 trials;
`snapshot` commits both and violates the invariant in 20/20 trials.

## Failure semantics

Every error answers three questions: did it commit, is retry safe, which
resource/key failed. `ConflictError` always means **not committed**.

| Exception | Retryable | Meaning |
|---|---|---|
| `ReadWriteConflict` | yes | a key this transaction read changed before commit |
| `WriteWriteConflict` | yes | another transaction wrote a key first |
| `StaleSnapshotError` | yes | the snapshot is no longer valid |
| `DeadlockError` | yes | wait-for cycle broken by aborting this transaction |
| `LockTimeoutError` | yes | lock acquisition timed out |
| `LeaseLostError` | **no** | the fencing token is stale; re-acquire the lease |
| `SerializationError` | yes | a serialization cycle was detected |
| `RetryExhaustedError` | no | the retry policy gave up |
| `IdempotencyMismatchError` | no | a key was reused with a different payload |
| `StorageError` | no | backend/transport failure; commit state may be unknown |

Retryable exceptions can be retried safely because the transaction did not commit. `LeaseLostError` is not retryable; the caller must re-acquire a lease before retrying. Unknown commit outcomes (e.g., transport failures) are logged as `FAILED` and are not retried; use `idempotency_key` to make the operation safe to resubmit.

## Idempotency

Pass `idempotency_key` to a session (or to `manager.run`). Replaying the key
returns the original `CommitResult` with `replayed=True`; writes are not applied
again.

The default fingerprint is **value-insensitive**: it hashes the agent id and the
write footprint (`resource`, `key`, `op`). A retried read-modify-write computes a
different new value but is still the same operation, so it correctly replays
instead of raising a mismatch. If you need strict payload binding, pass
`idempotency_fingerprint` explicitly.

External side effects use a separate journal (`IdempotentExecutor`,
`MemoryEffectStore`) with `PENDING â†’ EXECUTING â†’ SUCCEEDED|FAILED` transitions and
reserve/execute phases. This is at-most-once **completion**, not a distributed
transaction.

## Leases and fencing

A lease alone is unsafe: an owner can pause, lose the lease, and wake up later.
Every write under a lease therefore carries the lease token, and the backend
rejects any token older than the highest token it has observed:

```python
lease = await manager.lease("job", owner="worker-a", ttl_seconds=30)
async with manager.session(agent_id="worker-a", resources=[job], fence=lease.token):
    ...
```

Tokens are monotonic high-water marks per resource and are never reused, even
after release or expiry. See [docs/leases.md](docs/leases.md).

## Backends

| | `MemoryBackend` | `PostgresBackend` | `RedisBackend` |
|---|---|---|---|
| Scope | one process | multi-process | multi-process |
| Durable | no | yes (tables) | yes (prefix) |
| Commit atomicity | asyncio lock | DB transaction + advisory lock | Lua script |
| MVCC history | in-memory, GC'd | `statepact_history` | capped Redis lists |
| Pessimistic locks | wait-for graph | polling + timeout | Lua + TTL |
| Leases/fencing | in-memory | durable | durable |
| Idempotency | in-memory | durable | TTL'd |
| Extra | â€” | `pip install .[postgres]` | `pip install .[redis]` |

PostgreSQL creates its schema on `connect()`. The commit path serializes
validation and application behind one `pg_advisory_xact_lock`, which makes
cross-process commits linearizable at the cost of throughput. Redis executes each
commit as one Lua script. The memory backend is the semantic reference and is
used by the test suite; it is not safe across processes.

```python
from statepact.storage.postgres import PostgresBackend

backend = await PostgresBackend.connect("postgresql://user:pass@host/db")
manager = TransactionManager(backend)
```

See [docs/backends.md](docs/backends.md) for schemas, key layout and trade-offs.

## Integrations

Framework adapters live in `statepact.integrations` and do not import the
framework at module load:

```python
from statepact.integrations.langgraph import LangGraphCoordinator

coordinator = LangGraphCoordinator(manager, agent_id="researcher", resources=[inventory])

@coordinator.node
async def reserve(state, session):
    stock = await session.read("sku-1")
    if stock == 0:
        return {"status": "sold_out"}
    await session.write("sku-1", stock - 1)
    return {"status": "reserved"}

graph.add_node("reserve", reserve)
```

* `statepact.integrations.langgraph` â€” wrap graph nodes; merges the returned
  patch into state after commit.
* `statepact.integrations.crewai` â€” `transaction_tool(...)` /
  `TransactionTool`, a synchronous CrewAI tool over one atomic read-modify-write.
* `statepact.integrations.autogen` â€” `AutoGenTransactionMiddleware`, a
  transactional message handler with optional idempotency prefix.
* `statepact.integrations.base` â€” `run_transaction`, `read_modify_write`,
  `transactional` for any other framework.

See [docs/integrations.md](docs/integrations.md).

## Benchmarks

Measured on **2026-11-07** with the runner in this repository. Raw artifact:
[`benchmarks/results/run-20261107T051143Z.json`](benchmarks/results/run-20261107T051143Z.json)
(`latest.json` is identical).

Environment: Windows 11 `10.0.26300`, Python 3.12.13, AMD64 12-logical-CPU
VM, PostgreSQL 16 and Redis 7 in Docker Desktop on the same host. 40 agents,
10 operations each (400 commits), 20 write-skew trials. The workload injects
`asyncio.sleep(0)` between read and write so interleaving is guaranteed.

**Throughput (committed operations/second) and retry amplification**

| Scenario | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| Independent keys | 1,534 (1.00 attempt/op) | 53.7 (1.00) | 229.4 (1.00) |
| Single hot key, zero backoff | 344 (10.75) | 7.7 (13.56) | 18.8 (20.59) |
| Single hot key, jittered backoff | 241 (3.17) | 7.8 (12.90) | 26.8 (14.15) |
| Read-only commits | 6,619 | 133.4 | 427.4 |
| Lease + commit (sequential) | 3,013 | 18.3 | 88.9 |

**Correctness under the same load**

| Check | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| `none`: counter after 400 increments | 10 âŒ | 24 âŒ | 10 âŒ |
| `optimistic`: counter after 400 increments | 400 âœ… | 400 âœ… | 400 âœ… |
| `snapshot`: write-skew violations | 20/20 âŒ | 20/20 âŒ | 20/20 âŒ |
| `serializable`: write-skew violations | 0/20 âœ… | 0/20 âœ… | 0/20 âœ… |

How to read these numbers:

* The **correctness rows are the point**. `none` loses 376â€“390 of 400 updates;
  transactions prevent that, and the invariant holds at every commit.
* Hot-key throughput is a **worst case**: 40 agents hammer one key with immediate
  retries. Every retry is a full commit attempt, so backends that serialize
  commits (PostgreSQL) pay a round-trip tax per conflict. Shard hot resources or
  use `pessimistic` locking with jitter for production hot keys.
* The PostgreSQL backend is correctness-first: one advisory lock per commit,
  several SQL round trips, and Docker port forwarding on Windows. It is not a
  throughput-optimized commit engine.
* These are **not** comparisons with other libraries. They document this engine on
  one machine. Re-run on yours:

```bash
docker compose up -d postgres redis
STATEPACT_TEST_POSTGRES_URL=postgresql://statepact:statepact@localhost:5432/statepact \
STATEPACT_TEST_REDIS_URL=redis://localhost:6379/0 \
python benchmarks/run.py --backends memory,postgres,redis --agents 40 --ops 10 --trials 20
```

The runner is in-process by design: it measures the engine, not an HTTP server, so
Locust is not used. See [docs/benchmarks.md](docs/benchmarks.md) for methodology.

## Architecture

```
AgentSession â”€â”€ staged reads/writes â”€â”€â–¶ Transaction â”€â”€ CommitPlan â”€â”€â–¶ StorageBackend
      â”‚                                      â”‚                         â”‚
      â”‚                                      â”œâ”€ OCC validation         â”œâ”€ MemoryBackend
      â”‚                                      â”œâ”€ 2PL lock manager       â”œâ”€ PostgresBackend
      â”‚                                      â”œâ”€ serializability audit  â””â”€ RedisBackend
      â”‚                                      â””â”€ retry / idempotency
      â”œâ”€ event bus / transaction log / metrics
      â””â”€ lease + fencing token
```

The session never applies state. It builds a `CommitPlan`; the backend validates
and applies it atomically, or rejects it with a classified conflict. That split
is what makes the same semantics testable against three very different storage
engines.

## API at a glance

* `TransactionManager(backend=None)` â€” `resource`, `session`, `run`,
  `run_outcome`, `lease`, `health_check`, `close`; `transactions`, `stats`.
* `AgentSession` â€” `read`, `get`, `get_entry`, `version`, `write`, `delete`,
  `commit`, `abort`; `state`, `read_set`, `write_set`, `commit_result`.
* `SharedResource` â€” non-transactional diagnostics: `read`, `get`, `version`,
  `versions`, `history`, `state`, `snapshot`.
* `RetryPolicy(max_attempts, base_delay, max_delay, multiplier, jitter, retry_on)`.
* Observability: `EventBus`, `TransactionLog`, `Metrics`; optional OpenTelemetry
  bridge in `statepact.observability.otel`.

Full reference: [docs/api.md](docs/api.md).

## Documentation

Start at [docs/index.md](docs/index.md), or jump to:

* [Quickstart](docs/quickstart.md)
* [Concepts](docs/concepts.md)
* [Isolation and anomalies](docs/isolation.md)
* [Backends](docs/backends.md)
* [Leases and fencing](docs/leases.md)
* [Idempotency and side effects](docs/idempotency.md)
* [Retries](docs/retries.md)
* [Integrations](docs/integrations.md)
* [Benchmarks](docs/benchmarks.md)
* [Operations](docs/operations.md)
* [Limitations](docs/limitations.md)

## Development

```bash
uv sync --extra dev
uv run pytest -q                 # 121 tests; 14 integration tests skip without service URLs
uv run ruff check src tests demo benchmarks
uv run mypy src
docker compose up -d postgres redis
STATEPACT_TEST_POSTGRES_URL=postgresql://statepact:statepact@localhost:5432/statepact \
STATEPACT_TEST_REDIS_URL=redis://localhost:6379/0 \
uv run pytest tests/integration -q
```

The integration suite runs the **same** behavioural contract against PostgreSQL
and Redis (`tests/integration/backend_suite.py`), including conflicts, idempotent
replay, leases/fencing, lock timeouts, write skew and a 20-agent concurrent
increment.

## Status

**0.1.0 â€” alpha.** The semantics, tests and benchmarks are real; the API may still
change before 1.0. Known limitations are listed in
[docs/limitations.md](docs/limitations.md) â€” notably: no predicate/range tracking,
the PostgreSQL backend is throughput-limited by its global commit lock, the
serializability audit is process-local, and external side effects require
cooperation for idempotency.

## License

Apache-2.0. See [LICENSE](LICENSE).
