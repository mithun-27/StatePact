# Backends

StatePact ships three implementations of the same `StorageBackend` contract.
The test suite runs one behavioural suite against all three, so conflicts,
idempotent replay, leases, fencing, locking, history and concurrency are checked
identically.

| | `MemoryBackend` | `PostgresBackend` | `RedisBackend` |
|---|---|---|---|
| Scope | one process | many processes | many processes |
| Durable | no | yes, SQL tables | depends on Redis persistence |
| Commit atomicity | one `asyncio.Lock` | DB transaction + advisory lock | one Lua script |
| Cross-process commits | no | yes | yes |
| MVCC history | in-memory, GC'd | `statepact_history` | capped Redis lists |
| Pessimistic locks | wait-for graph | polling + timeout | Lua + TTL |
| Leases / fencing | in-memory | durable | durable |
| Idempotency | in-memory | durable | TTL (30 days default) |
| Driver | none | `asyncpg` | `redis` |
| Extra | — | `statepact[postgres]` | `statepact[redis]` |

## MemoryBackend

```python
from statepact import TransactionManager
from statepact.storage.memory import MemoryBackend

backend = MemoryBackend()
manager = TransactionManager(backend)
```

* Commit validation and application run under one `asyncio.Lock`.
* Snapshots are MVCC: each key keeps a history, and a snapshot sees the newest
  entry at or before its `taken_at` sequence.
* Version history is garbage-collected once no live snapshot needs it. The
  playground and benchmarks release snapshots at commit/abort automatically.
* Pessimistic locking uses the in-process `LockManager` with a wait-for graph.
* State is lost when the process exits. It is not safe across processes or OS
  threads; use PostgreSQL or Redis for that.

Useful constructor options:

| Option | Default | Meaning |
|---|---|---|
| `lock_timeout` | `5.0` | default pessimistic lock wait |
| `serialization_window` | `512` | committed footprints retained for the audit |
| `retain_versions` | `None` | force history retention per key (tests) |
| `gc` | `True` | enable snapshot-driven history GC |

`MemoryBackend` is the semantic reference. It is the right backend for unit
tests, notebooks, the demo, and any single-process agent runtime.

## PostgresBackend

```python
import os
from statepact import TransactionManager
from statepact.storage.postgres import PostgresBackend

backend = await PostgresBackend.connect(os.environ["STATEPACT_DATABASE_URL"])
manager = TransactionManager(backend)
```

`connect()` opens an `asyncpg` pool, verifies the connection and creates the
schema if it does not exist. The default pool is 1–10 connections.

### Schema

| Table | Purpose |
|---|---|
| `statepact_meta` | single row with the global commit sequence |
| `statepact_resources` | resource registry, metadata, last commit time |
| `statepact_values` | current value per `(resource, key)` |
| `statepact_history` | every committed value with its version |
| `statepact_idempotency` | key → request fingerprint, version, timestamp |
| `statepact_locks` | pessimistic lock rows with optional expiry |
| `statepact_leases` | owner, monotonic token, expiry, TTL |

The user needs `CREATE TABLE` permission on first connect (or the schema must be
pre-created by an administrator). The tables are ordinary SQL; back them up with
your normal database tooling.

### Commit path

1. `BEGIN`, then `pg_advisory_xact_lock` on a constant key.
2. Check fencing tokens.
3. Check the idempotency record.
4. Read latest versions for the read and write sets.
5. Validate according to the isolation mode.
6. Serializability audit for `serializable` (process-local graph).
7. Increment `statepact_meta.seq`, write values and history, update metadata.
8. Store the idempotency record, `COMMIT`.

The advisory lock makes cross-process commits linearizable. It also serialises
all commits in the schema, which is why this backend is **correctness-first, not
throughput-optimised**. If you need more commit throughput, shard resources
across databases/schemas or choose Redis.

Read-only commits do not advance the sequence. Pessimistic locks are rows with a
timeout: acquisition is polled every 50 ms, and a lock cycle surfaces as
`LockTimeoutError` rather than a database deadlock. Leases take a
`pg_advisory_xact_lock(hashtext(...))` and `SELECT ... FOR UPDATE` so token
allocation is atomic per resource.

### Operations

```python
await manager.health_check()      # SELECT 1
await backend.clear()             # truncate StatePact tables, reset seq
await manager.close()             # closes the pool
```

`clear()` is a test/playground helper. It truncates the StatePact tables in the
schema configured at ``connect()`` (defaults to the public schema if not set) and
resets the sequence; it does not touch other tables.

### Configuration

```python
backend = await PostgresBackend.connect(
    dsn,
    min_size=2,
    max_size=20,
    lock_timeout=5.0,
    serialization_window=512,
)
```

Tune `max_size` to the database's connection budget and your number of worker
processes. Every commit needs one pooled connection for the duration of the
advisory lock.

## RedisBackend

```python
from statepact import TransactionManager
from statepact.storage.redis import RedisBackend

backend = await RedisBackend.connect(
    "redis://localhost:6379/0",
    prefix="myapp",
    history_limit=512,
    idempotency_ttl_seconds=30 * 24 * 3600,
    lock_ttl_seconds=60.0,
)
manager = TransactionManager(backend)
```

All keys are namespaced under `prefix`:

| Key | Type | Contents |
|---|---|---|
| `{p}:seq` | string | global commit sequence |
| `{p}:res` | set | registered resource names |
| `{p}:meta:{resource}` | hash | version, last commit, metadata |
| `{p}:data:{resource}` | hash | key → `{"v": value, "ver": n, "d": bool}` |
| `{p}:hist:{resource}:{key}` | list | newest-first history, capped |
| `{p}:idem:{key}` | string | request hash, version, timestamp (TTL) |
| `{p}:fence:{resource}` | string | highest fencing token ever issued |
| `{p}:lease:{resource}` | hash | owner, token, expiry, TTL |
| `{p}:lock:{resource}:{key}` | set | `transaction_id|mode` lock holders |

A single Lua script validates fencing, idempotency, reads, writes and applies all
writes. Redis executes a script atomically, so the commit point is well defined.
Pessimistic locks are TTL'd sets: a crashed process cannot block a key forever,
but a lock can expire while a slow client still believes it holds it. Keep
`lock_ttl_seconds` comfortably above your longest critical section, or handle
`LockTimeoutError` by retrying.

History is capped at `history_limit` entries per key (default 512). Idempotency
records expire after `idempotency_ttl_seconds`; a replay after that window is
treated as a new commit. Choose a TTL that covers your retry/replay horizon.

Redis durability is a **server configuration choice**. The bundled
`docker-compose.yml` enables AOF (`--appendonly yes`), but a managed Redis with no
persistence can lose recently committed state on restart. StatePact does not
sync to disk itself.

`serializable` uses full read validation on Redis. The in-memory
serialization-graph audit is process-local and is not executed there; full
backward validation is the portable guarantee.

## Choosing a backend

| Need | Backend |
|---|---|
| Unit tests, notebook, single process | memory |
| Durable fleet of agents with SQL operations | PostgreSQL |
| Low-latency shared coordination, TTL semantics | Redis |
| Multiple app instances + accidental commit volume | PostgreSQL (sharded) or Redis |

Do not put domain data only in StatePact's coordination tables and call it a
database. The values are JSON and the API is intentionally small. Treat the
backend as the coordination layer and keep your domain store where it belongs.

## Measured throughput

From the [benchmark run](benchmarks.md) on one Windows host with Docker Desktop:

| Scenario | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| Independent keys (40 agents × 10) | 1,664 ops/s | 59.7 ops/s | 219.8 ops/s |
| Single hot key, no backoff | 348 ops/s | 8.3 ops/s | 19.5 ops/s |
| Read-only commits | 6,378 ops/s | 130.6 ops/s | 415.3 ops/s |

These numbers are not a product comparison. They show the shape of the
trade-offs: PostgreSQL pays for cross-process linearizability with a global
commit lock and round trips; Redis pays one Lua round trip; memory pays almost
nothing.
