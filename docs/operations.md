# Operations

This document describes how to operate and maintain an StatePact deployment in production.

## Deployment checklists

### MemoryBackend (single-process)

* Memory is not persisted across process restarts.
* Configure one manager per process; sharing a manager across threads is not supported.
* Set `lock_timeout` to match your application’s latency SLAs.
* Use `serialization_window` large enough to cover your longest audit horizon; the window is per-process, not global.
* Monitor `metrics.snapshot()` for high conflict or abort rates; these indicate contention that may require sharding or a stronger isolation mode.
* Memory usage follows retained history. Keep `gc=True`; `retain_versions` is a test aid and can grow without bound.

### PostgresBackend

* Ensure the database user has `CREATE TABLE` permission on the first connect (the schema is created lazily).
* Back up the tables periodically (`statepact_meta`, `statepact_resources`, `statepact_values`, `statepact_history`, `statepact_idempotency`, `statepact_locks`, `statepact_leases`).
* Tune the connection pool (`min_size`/`max_size`) to your worker count and desired throughput. Each commit needs one connection for the advisory lock.
* Set `lock_timeout` and `serialization_window` in `connect()`, or pass them from your application configuration.
* Idempotency records are retained for `idempotency_retention_days`. After that window the same idempotency key is treated as a new commit. Prune `statepact_idempotency` when it grows large.
* The commit path uses `pg_advisory_xact_lock` across **all** resources, making it a shared global lock. If you need higher commit throughput, shard resources across databases/schemas.
* Use the manager's in-memory `TransactionLog` (bounded, default 20,000 records) for recent debugging. `statepact_history` is the durable MVCC value history, not a request log.

### RedisBackend

* Redis durability depends on your server configuration (AOF, RDB). The bundled `docker-compose.yml` enables AOF; a managed Redis with no persistence can lose recent commits on restart.
* Choose a `prefix` that isolates StatePact from other keys in the Redis database.
* History is capped at `history_limit`. Tune per key or per-resource retention needs.
* Idempotency records expire after `idempotency_ttl_seconds`. Choose a TTL that covers the longest client retry window, then clean up expired keys periodically.
* Pessimistic lock TTLs are `lock_ttl_seconds`. Keep them comfortably above your longest critical section to avoid starvation.
* Redis transactions are executed under a Lua script, which means each commit incurs a single round-trip. The global commit lock of PostgreSQL is not present, so contention is limited to the hot key.

## Monitoring and alerts

### Metrics

The `Metrics` class tracks counters, gauges and histograms. Export them via Prometheus, Datadog, New Relic or integrate with your existing metrics pipeline.

| Metric name | Description |
|---|---|
| `statepact.tx.committed` | number of transactions that reached `SESSION_COMMITTED` |
| `statepact.tx.aborted` | number of transactions that ended as conflicts or clean aborts |
| `statepact.tx.failed` | number of transactions that crashed (unknown outcome) |
| `statepact.commit.latency_ms` | time spent in a successful backend `commit()` call (one attempt) |
| `statepact.tx.latency_ms` | end-to-end latency of one session attempt, including booking and cleanup |
| `statepact.retry.attempts` | number of retries scheduled; does not include the first attempt |
| `statepact.retry.delay_ms` | jittered delay before each retry |

### Event bus

The `EventBus` feeds events to Python `logging`, OpenTelemetry spans, the `TransactionLog` and the Gradio playground’s timeline. Subscribe to capture custom analytics or alert on high conflict rates.

### Transaction log

The `TransactionLog` is a bounded in-memory record of recent transactions (committed, aborted, failed). It is deliberately lossy under sustained load, so treat it as a debugging view rather than an audit trail. Query it with `manager.transactions()`, `.committed()`, `.conflicts()` and `.for_agent()`.

## Scaling and sharding

* **Hot keys** – use `pessimistic` locking with jitter or shard the resource across multiple backends. Do not rely on optimistic retries when the contention rate is above ~5%.
* **Multiple agents** – keep one manager per agent process; managers do not share state, so agents can run on independent machines.
* **Multi-region** – configure separate Postgres/Redis clusters per region and route agent requests to the nearest one.
* **Backup and failover** – StatePact does not provide its own replication. Use your database’s built‑in replication, Patroni, or a managed service with automated failover.

## License and attribution

This library is Apache‑2.0. When you distribute a product that includes StatePact, the license text must accompany the source distribution.

## Security considerations

* Idempotency keys are global per backend; namespace them with your tenant or session identifier.
* The lease/fence token is a monotonic counter; never reset the backend if you have active leases. Tokens are never reused, even after release.
* Store database credentials securely; the package does not provide its own credential management.
* Leases and locks are TTL‑ed but can expire while a process still holds the logical resource. Implement application‑level health checks.

## Troubleshooting

| Symptom | Possible cause | Fix |
|---|---|---|
| All commits fail with `ConflictError` | Over‑subscription of a hot key, retries colliding | Add jitter, raise `max_attempts`, shard the key or switch to `pessimistic` |
| `LockTimeoutError` under load | Lock held longer than `lock_timeout`, or a crashed lock holder | Raise the timeout, shorten the critical section, or lower the lock TTL on Redis |
| `LeaseLostError` on renewal | Lease expired, another owner took it, or the TTL was too short | Extend TTL, renew at ~TTL/3, re-acquire before retrying |
| `RetryExhaustedError` after a few attempts | Conflict rate is too high for your workload | Increase `max_attempts`, add exponential backoff, shard resources |
| Memory usage grows without bound | History retained, or `retain_versions` set | Keep `gc=True`; do not use `retain_versions` outside tests |

## Rollback strategy

If a new release breaks a production workflow:

1. Pin the existing version in your package manager (`pip install statepact==0.1.0`).
2. Use the `TransactionLog` to replay the last few transactions and verify the effects.
3. If necessary, revert to a known‑good checkpoint in your framework (LangGraph, CrewAI, …) because StatePact does not provide its own checkpointing.

## Upgrading

0.1.0 is the first public release. The documented public surface is
`TransactionManager`, `AgentSession`, `SharedResource`, `RetryPolicy`,
`LeaseManager`, `IdempotentExecutor` and the exception hierarchy. Imports under
`statepact.core`, `statepact.concurrency`, `statepact.coordination` and
`statepact.storage` are supported but may change before 1.0.0.

The `otel` extra is optional and does not affect core functionality. The default
idempotency fingerprint is **value-insensitive**; confirm that this replay
semantics is safe for your operation before relying on it.

Pin an exact version in production. Breaking changes are possible before 1.0.0
and will be listed in the
[changelog](https://github.com/mithun-27/StatePact/blob/main/CHANGELOG.md).

## Contributing

See [CONTRIBUTING.md](https://github.com/mithun-27/StatePact/blob/main/CONTRIBUTING.md)
for code-style guidelines, testing checklist and how to propose changes.
