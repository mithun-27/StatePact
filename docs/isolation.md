# Isolation and anomalies

AgentCommit gives each transaction a consistent snapshot and validates its
conflicts at commit. The isolation mode decides **what is read** and **what is
validated**. No mode can protect a key that a transaction never declared.

## Modes at a glance

| Mode | Reads | Commit validation | Anomalies for tracked keys | Cost |
|---|---|---|---|---|
| `none` | latest committed value | none | lost updates, write skew, everything | lowest |
| `snapshot` | MVCC snapshot | write base versions only | write skew; no lost updates | low |
| `optimistic` | MVCC snapshot | every read **and** write base | none for tracked keys | medium |
| `serializable` | MVCC snapshot | `optimistic` + graph audit | none for tracked keys; cycle diagnostics | medium |
| `pessimistic` | locked latest value | 2PL + defensive validation | none for locked keys | highest contention cost |

```python
async with manager.session(
    agent_id="planner",
    resources=[inventory],
    isolation="optimistic",       # default
) as session:
    stock = await session.read("sku-1")
    await session.write("sku-1", stock - 1)
    await session.commit()
```

The mode is per session. Different agents may use different modes against the
same resource, but a transaction using a weaker mode can still break another
transaction's invariant. Pick one mode per shared resource in production.

## What validation actually does

Every write records an **expected base version**: the version of the key that the
agent based its new value on. Every read records the version it observed.

At commit, the backend compares those recorded versions with the latest committed
versions:

* a **read** is invalid if the key's latest version is not the version observed;
* a **write** is invalid if the key's latest version is not the expected base.

A read of an absent key records version `0`; it is invalidated if the key is
created before commit. A write to an absent key has base version `0`; it is
invalidated if another transaction creates the key first.

`optimistic` and `serializable` validate both sets. `snapshot` validates writes
only, which is first-committer-wins snapshot isolation. `none` validates nothing.
`pessimistic` validates defensively in addition to holding locks.

### Read-only transactions

A commit with no writes validates nothing and does not consume a version. A
read-only transaction cannot violate a write invariant, and retrying it would
only waste agent work. Its `CommitResult.commit_version` is the sequence observed
at commit time.

## The classic anomalies

### Lost update

Two agents read `10`, both write `9`. One increment is lost.

| Mode | Result |
|---|---|
| `none` | lost update |
| `snapshot` | second writer rejected (write base mismatch) |
| `optimistic` | second writer rejected |
| `serializable` | second writer rejected |
| `pessimistic` | serialised by the exclusive lock |

The benchmark's 400-increment counter demonstrates this on all three backends:

| Mode | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| `none` | 10 / 400 | 24 / 400 | 10 / 400 |
| `optimistic` | 400 / 400 | 400 / 400 | 400 / 400 |

### Write skew

Two on-call doctors each check that the other is on call, then each goes off
call. Both reads are valid individually, and the two writes touch different keys,
so snapshot isolation commits both and the invariant "at least one doctor is on
call" is violated.

```python
async def go_off_call(session, me: str, other: str) -> None:
    if await session.read(other):
        await session.write(me, False)
```

| Mode | 20 trials |
|---|---:|
| `snapshot` | 20 / 20 violations |
| `optimistic` | 0 / 20 (second commit read-validates the other key) |
| `serializable` | 0 / 20 |

`optimistic` prevents this because it validates **reads**, not just writes. The
`serializable` mode additionally runs a serialization-graph audit and raises
`SerializationError` when it can prove a cycle. The audit is a diagnostic safety
net; full read validation is what enforces the guarantee for tracked keys.

## Pessimistic locking

`pessimistic` uses strict two-phase locking:

* reads take shared locks on `(resource, key)`;
* writes take exclusive locks;
* a shared lock is upgraded in place when the same transaction writes a key it
  already read;
* all locks are held until commit or abort.

In the in-memory backend, a wait-for graph detects cycles at the moment a
transaction starts waiting and aborts the requester with `DeadlockError`. The
PostgreSQL and Redis backends acquire locks through separate short operations, so
a cycle cannot be detected inside a database transaction; it surfaces as
`LockTimeoutError` instead. Both errors are retryable.

```python
async with manager.session(
    agent_id="worker",
    resources=[inventory],
    isolation="pessimistic",
    lock_timeout=2.0,
) as session:
    stock = await session.read("sku-1")
    await session.write("sku-1", stock - 1)
    await session.commit()
```

Use `pessimistic` for hot keys where retrying optimistic conflicts would be more
expensive than queueing. Use `optimistic` when conflicts are rare. Do not mix
them on the same invariant without understanding the wait/retry behaviour.

## Conflict classes

Every conflict is a subclass of `ConflictError` and means **the transaction did
not commit**.

| Exception | `conflict_class` | Typical cause |
|---|---|---|
| `ReadWriteConflict` | `read_write` | a read key changed before commit |
| `WriteWriteConflict` | `write_write` | another transaction wrote a key first |
| `DeadlockError` | `deadlock` | wait-for cycle victim |
| `LockTimeoutError` | `deadlock` | lock wait exceeded the timeout |
| `SerializationError` | `serialization` | audit found a cycle |
| `LeaseLostError` | `lease_lost` | stale fencing token (not a retryable conflict) |

**Note:** `StaleSnapshotError` is defined but not raised by any backend. It is retained for API completeness.

Each carries `resource`, `key`, `expected_version` and `actual_version` when the
backend can supply them.

## Choosing a mode

| Situation | Recommended |
|---|---|
| Default read-modify-write across agents | `optimistic` |
| High-contention hot key, predictable queueing | `pessimistic` with jitter |
| Read-heavy analytics, skew acceptable | `snapshot` |
| Auditing or high-assurance invariants | `serializable` |
| Demonstrating the bug in a talk | `none` — never production |

Start with `optimistic`. Measure conflict rates. Only move to `pessimistic` when
retry amplification is the bottleneck, and only move to `snapshot` when you can
prove the invariant is not vulnerable to write skew.

## Limits

* **Key granularity only.** Predicate reads (`SELECT ... WHERE status = 'open'`)
  are not tracked. A transaction that depends on "no matching rows" is not
  protected from a concurrent insert. See [Limitations](limitations.md).
* **Values are JSON.** `normalize_value` rejects non-finite floats and
  non-serialisable objects at write time.
* **Read-only commits do not validate.** If an agent must verify an invariant
  before acting, perform a write or use an explicit lock.
* **The serialization audit is process-local.** In PostgreSQL it runs inside the
  commit lock but only sees transactions committed through that backend
  instance; Redis does not run it at all. Full backward validation is the
  portable guarantee.
