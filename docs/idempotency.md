# Idempotency and side effects

Two different problems need two different tools:

1. **Commit deduplication** — the same transaction is submitted twice (retry
   after an ambiguous timeout) and must not apply its writes twice. This is
   built into the storage backends.
2. **External side effects** — a card charge, email or third-party API call
   cannot join a database transaction. AgentCommit provides a separate effect
   journal with at-most-once *completion*.

## Idempotent commits

Pass `idempotency_key` to a session or to `manager.run`:

```python
result = await manager.run(
    "billing",
    [wallet],
    charge,
    idempotency_key="invoice-42",
)
```

```python
async with manager.session(
    agent_id="billing",
    resources=[wallet],
    idempotency_key="invoice-42",
) as session:
    balance = await session.read("balance")
    await session.write("balance", balance - 30)
    result = await session.commit()
    print(result.replayed)   # False first time, True on replay
```

The first commit stores the key, the request fingerprint and the `CommitResult`.
A replay with the same key and fingerprint returns the stored result with
`replayed=True` **without reapplying writes**. The same key with a different
fingerprint raises `IdempotencyMismatchError`.

Keys are global per backend/schema, not per agent. Namespace them with your own
prefix (`tenant-7:invoice-42`) to avoid collisions.

### The default fingerprint is value-insensitive

The default request fingerprint hashes:

* `agent_id`;
* the write footprint: `(resource, key, op)` for each staged write.

It deliberately **excludes values**. A retried read-modify-write recomputes a
different new value (because the base changed), but it is still the same logical
operation and must replay rather than raise a mismatch.

If you need strict payload binding, pass `idempotency_fingerprint`:

```python
async with manager.session(
    agent_id="billing",
    resources=[wallet],
    idempotency_key="invoice-42",
    idempotency_fingerprint=f"charge:{amount}:{currency}",
) as session:
    ...
```

With an explicit fingerprint, the fingerprint is hashed with `agent_id` and the
values are not inspected. Include every field that makes two operations
different.

### What counts as the same operation?

| Change between attempts | Default fingerprint | Explicit fingerprint |
|---|---|---|
| same agent, same writes, different new values | same → replays | depends on fingerprint |
| same agent, additional write key | different → mismatch | depends on fingerprint |
| different agent, same key | different → mismatch | different (agent id included) |
| read-only commit | same for that agent (empty write set) | depends on fingerprint |

### Replay with every isolation mode

Idempotency is checked before fence validation and before read/write validation.
A replay returns the stored result even if the current state has moved on. That is
the point: the operation already happened.

Read-only commits are also recorded, so a replay returns the same commit version
instead of observing a newer sequence.

### Retention

| Backend | Retention |
|---|---|
| memory | up to 100,000 keys, oldest evicted in insertion order |
| PostgreSQL | durable rows; no automatic pruning in 0.1.0 |
| Redis | `idempotency_ttl_seconds`, default 30 days |

If a replay arrives after the record is gone, it is a new commit. Size the
retention window to cover the longest possible client retry, and prune
explicitly if your idempotency table grows.

## External side effects

An external API cannot be made exactly-once by a library. Without cooperation
from the external system, the choice is at-least-once or at-most-once. The
effect journal gives **at-least-once execution with at-most-once completion**:

```python
from agentcommit import IdempotentExecutor, MemoryEffectStore

effects = IdempotentExecutor(MemoryEffectStore())

charge = await effects.run(
    "charge:invoice-42",
    lambda: payment_api.charge(amount=30, idempotency_key="invoice-42"),
)
```

The journal state machine is:

```text
PENDING ──► EXECUTING ──► SUCCEEDED
                │
                └────────► FAILED
```

* `SUCCEEDED` is never executed again; the stored result is returned.
* `EXECUTING` raises `EffectInProgressError` (retryable) to another worker.
* `FAILED` raises `EffectFailedError` unless `retry_failed=True`.
* `PENDING` (reserved but never executed) raises `EffectFailedError` unless
  `retry_failed=True`.

Two-phase use, when the local commit must happen before the external call:

```python
await effects.reserve("charge:invoice-42")

async with manager.session(agent_id="billing", resources=[wallet]) as session:
    await session.write("balance", new_balance)
    await session.commit()

await effects.execute_reserved(
    "charge:invoice-42",
    lambda: payment_api.charge(30, idempotency_key="invoice-42"),
)
```

If the process dies after the local commit but before the external call, the
effect stays `PENDING`. A recovery worker can inspect the journal and decide to
execute or compensate. If it dies after the external call but before recording
`SUCCEEDED`, the effect is `EXECUTING` and must be reconciled with the provider.
Passing an idempotency key to the provider is what makes that reconciliation
safe. AgentCommit does not pretend to solve this without provider support.

`MemoryEffectStore` is process-local and intended for tests and examples. For
production, implement the four-method `EffectStore` interface on your database
(`get`, `create`, `compare_and_set`, `put`) so the journal survives a restart and
is shared between workers.

## Related errors

| Error | Retryable | Meaning |
|---|---|---|
| `IdempotencyMismatchError` | no | same key, different fingerprint |
| `EffectInProgressError` | yes | another worker owns the effect |
| `EffectFailedError` | yes | previous attempt failed/was not executed |
| `SideEffectError` | no | journal could not be recorded safely |

## What idempotency does not do

* It does not make a non-idempotent HTTP call safe to repeat. The provider must
  accept an idempotency key, or you must reconcile by reading provider state.
* It does not roll back external effects when a later local step fails.
* It does not provide a distributed transaction across your database and a
  third-party service. See [Limitations](limitations.md).
