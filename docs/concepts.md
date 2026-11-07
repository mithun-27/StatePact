# Concepts

## Agents, sessions and transactions

An agent is any concurrent actor with a stable id (`agent-1`, `researcher`,
`worker-a`). A **session** is one transaction attempt for that agent. It holds:

* the resources in scope,
* an isolation mode,
* the snapshot (for snapshot-based modes),
* the staged reads and writes,
* an optional fence token and idempotency key.

```python
async with manager.session(
    agent_id="agent-1",
    resources=[inventory, ledger],
    isolation="optimistic",
) as session:
    ...
    await session.commit()
```

The session is single-use. `manager.run` creates one per retry attempt; reuse of
staged writes across attempts never happens.

## Resources and keys

A **resource** is a named, schema-less collection of JSON values:

```python
inventory = manager.resource("inventory", {"sku-1": 10, "sku-2": 4})
```

* `initial_state` is applied by `ensure()` only when the resource does not exist
  yet. It never overwrites committed data.
* Keys are independent conflict units. Two transactions writing different keys of
  the same resource do not conflict.
* Values must be JSON-normalizable Pydantic-compatible data. `normalize_value`
  rejects non-finite floats, non-string keys and unsupported objects at write
  time, so a commit cannot fail halfway through serialization.
* A session must declare its resources. Passing a key from an undeclared resource
  raises `ResourceNotInSessionError`; omitting `resource=` when several are in
  scope raises `AmbiguousResourceError`.

## Versions

Every committed write consumes one global commit version (a monotonically
increasing integer). Keys carry the version at which they were last written.
Absent keys have version `0` (`ABSENT`).

Versions are used for:

* **validation** — the read set records the version observed; a later commit must
  still see that version or the transaction is stale;
* **write bases** — a write remembers the version it was derived from, which
  prevents lost updates even when the key was not read explicitly;
* **history** — `resource.history(key)` and `backend.history(...)` expose
  committed versions;
* **fencing** — leases have their own monotonic token namespace per resource.

Read-only commits do not consume a version.

## Commit lifecycle

```
open session
  ├─ snapshot / acquire locks (mode-dependent)
  ├─ read  → record (resource, key, version)
  ├─ write → stage (value, expected base version)
  └─ commit
       ├─ build CommitPlan
       ├─ backend: check fence → check idempotency → validate → apply
       ├─ success: return CommitResult (commit_version, replayed)
       └─ conflict: raise ConflictError, transaction did not commit
```

The backend is the only component that applies committed state. This separation
lets the in-memory, PostgreSQL and Redis backends share one test contract.

## CommitPlan and CommitResult

A `CommitPlan` is the immutable request a session sends to a backend:

| Field | Meaning |
|---|---|
| `transaction_id` | unique id for this attempt |
| `agent_id` | owner of the transaction |
| `isolation` | selected mode |
| `reads` | `(resource, key, version)` observed |
| `writes` | `(resource, key, value, expected_version, op)` |
| `fence` | lease token, if any |
| `idempotency_key` / `request_hash` | deduplication fields |
| `read_only` | true when there are no writes |

A `CommitResult` says:

| Field | Meaning |
|---|---|
| `committed` | always `True` on a returned result |
| `commit_version` | version assigned, or current sequence for read-only |
| `committed_at` | backend timestamp |
| `replayed` | `True` when an idempotency key returned a stored result |
| `read_set` / `write_set` | what the plan contained |

A conflict **raises**; it never returns `committed=False`. Code that checks
`result.committed` is defensive, not required.

## Direct (non-transactional) helpers

`SharedResource` exposes `read`, `get`, `version`, `versions`, `history`,
`snapshot` and `state` for diagnostics, admin tools and examples. They bypass
conflict detection entirely. Never use them for an agent's read-modify-write.

## Errors

All errors derive from `AgentCommitError` and expose `retryable`, `details` and,
for conflicts, `conflict_class`, `resource` and `key`. See
[Isolation](isolation.md#conflict-classes) and the
[API reference](api.md#exceptions-hierarchy).

## Observability

Every session publishes events (`session_started`, `conflict`, `retry`,
`commit_applied`, `lease_lost`, …), appends `TransactionRecord`s to a
`TransactionLog`, and updates `Metrics`. All are optional and process-local by
default; the OpenTelemetry bridge is an extra. See [Operations](operations.md).
