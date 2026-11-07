# Limitations

AgentCommit is a transaction coordination layer, not a database, workflow engine
or distributed transaction protocol. Its semantics are deliberately narrow so the
same contract can be implemented correctly by three very different backends.

## Key granularity only

AgentCommit tracks individual keys inside a named resource. Predicate reads,
range scans and "no rows matched" conditions are not tracked.

```python
# Tracked: this read is validated at commit.
stock = await session.read("sku-1")

# Not tracked: a concurrent insert can change the query result
# without invalidating this transaction.
rows = await my_database.fetch("SELECT * FROM orders WHERE status = 'open'")
```

**Workaround:** write a summary key that represents the predicate (for example a
`open_order_count` key, updated in the same transaction), or enforce the
invariant with a database constraint and read the constrained row through the
session.

## Reads outside a session are not protected

`SharedResource.read()` and `.get()` are diagnostic, non-transactional reads.
They observe the latest committed value and take no part in validation. Only
`AgentSession` reads are tracked.

## No bulk or range writes

You cannot stage "delete every key matching a pattern" or a SQL-style range
update. Each key must be read and written explicitly. Bulk operations belong in
your domain database, coordinated with your own versioning.

## Idempotency is opt-in and has a retention window

Without `idempotency_key`, a retried commit can apply twice when the first
attempt's outcome is unknown. With a key:

* The default fingerprint is value-insensitive (agent id + write footprint), so a
  retried read-modify-write replays even though it computes a different new value.
  Use `idempotency_fingerprint` when the payload itself must be bound.
* PostgreSQL retains idempotency rows durably and does not prune them
  automatically in 0.1.0.
* Redis expires records after `idempotency_ttl_seconds` (30 days by default).
* The memory backend keeps up to 100,000 keys and evicts the oldest.

A replay that arrives after the record is gone is a new commit.

## External side effects cannot be atomic

The effect journal (`IdempotentExecutor` + an `EffectStore`) gives at-most-once
**completion**, not exactly-once delivery. If a charge succeeds and the process
dies before recording `SUCCEEDED`, the effect stays `EXECUTING` and must be
reconciled with the provider. Provider-side idempotency keys are what make that
safe. AgentCommit does not pretend otherwise.

## No distributed transactions across backends or services

One manager talks to one backend. There is no two-phase commit across PostgreSQL
and Redis, and no protocol spanning microservices. Use sagas or external
coordination for cross-service atomicity.

## PostgreSQL commits are globally serialised

Every commit takes a `pg_advisory_xact_lock`, which makes commits linearizable
across processes but serialises all of them. The backend is correctness-first and
not throughput-optimised. Shard resources across schemas/databases or use Redis
when commit throughput matters more than SQL-side durability.

## Redis durability depends on server configuration

AgentCommit writes through the Redis protocol; it does not fsync. An instance with
persistence disabled can lose recently committed state on restart. The bundled
`docker-compose.yml` enables AOF, but that is a local development default, not a
production guarantee.

## Pessimistic locks can expire before the work finishes

Redis locks carry a TTL, and PostgreSQL lock rows also expire. A crashed process
must not block a key forever, but a slow client whose lock expired can still
believe it holds the lock. Keep the TTL comfortably above the critical section and
treat `LockTimeoutError` as retryable.

## The serialization audit is process-local

The `serializable` mode's graph audit only sees transactions committed through the
same backend instance (PostgreSQL) or is not run at all (Redis). Full backward
validation of reads and writes is the portable guarantee; the audit is a
diagnostic safety net.

## Leases do not block writes by themselves

A lease records ownership; the fencing token is what rejects stale writers. An
owner that pauses past its TTL can still write **if no newer owner has acquired the
resource**. Its writes are rejected once a higher token exists. This is the
standard fencing model, not a lease-based mutex.

## MemoryBackend is single-process only

State is process-local and lost on exit. It is not safe across OS threads or
processes. Use PostgreSQL or Redis for multi-process deployments.

## Mixed isolation modes on one invariant are unsafe

A transaction using `none` or `snapshot` can violate an invariant that another
transaction protects with `optimistic` or `serializable`. Pick one mode per shared
invariant and do not mix them casually.

## No built-in conflict resolution policy

AgentCommit retries with backoff. It does not implement last-writer-wins,
compensation, automatic merging or CRDTs. Those belong in application code.

## Observability is optional

Without the `otel` extra there are no OpenTelemetry spans; the event bus still
emits in-process events. The OpenTelemetry bridge is new and its API may change
before 1.0.0. The in-memory `TransactionLog` is bounded and lossy under load.

## Framework adapters are thin and optional

The LangGraph, CrewAI and AutoGen adapters depend on third-party APIs that can
change independently. They do not import the framework at module load, but they
are tested against the versions in the development environment, not every version.

## Status

0.1.0 is alpha. The core transaction path is tested against the invariants in
[docs/benchmarks.md](benchmarks.md), but the public API may change before 1.0.0.
Pin an exact version in production and read the
[changelog](https://github.com/mithun-27/AgentCommit/blob/main/CHANGELOG.md) before upgrading.

## Getting help

Start with [Quickstart](quickstart.md), [Concepts](concepts.md) and the runnable
examples in `examples/`. If you open an issue, include a minimal reproduction, the
Python version and platform, and the backend you used.
