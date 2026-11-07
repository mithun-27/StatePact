# Leases and fencing

Transactions protect a *read-modify-write*. A lease protects **ownership of a
long-running job** — for example, one worker must process a queue item, and a
duplicate worker must not run at the same time.

A lease alone is not enough. An owner can pause (GC, VM migration, network
partition), lose the lease, and wake up later believing it still owns the job.
AgentCommit pairs every lease with a **fencing token** that the backend checks on
every write.

## The model

* A lease is `(resource, owner, token, expires_at, ttl_seconds)`.
* Tokens are monotonic **per resource** and never reused, even after release or
  expiry.
* Re-acquiring a live lease as the same owner extends it and keeps the same
  token.
* A new owner after expiry/release gets a strictly higher token.
* The backend remembers the highest token it has observed for each resource and
  rejects any write carrying an older token with `LeaseLostError`.
* A lease does not replace transaction validation. A commit still validates reads
  and writes according to its isolation mode.

```python
lease = await manager.lease("job-42", owner="worker-a", ttl_seconds=30)

async with manager.session(
    agent_id="worker-a",
    resources=[job],
    fence=lease.token,
) as session:
    await session.write("status", "running")
    await session.commit()

await manager.backend.renew_lease(lease)     # heartbeat before expiry
await manager.backend.release_lease(lease)
```

`manager.lease(resource_name, ...)` registers the resource with an empty state if
it does not exist, so it works before the first session.

## Why the token must be checked by storage

Consider this interleaving:

```text
worker-a: acquire lease (token 1)
worker-a: ... pauses ...
lease expires
worker-b: acquire lease (token 2)
worker-b: write status = "done"
worker-a: wakes up, writes status = "running"   # stale write
```

If the backend only checks "does `worker-a` believe it has a lease?", the stale
write lands. AgentCommit records token `2` as the high-water mark and rejects
`worker-a`'s token `1`. Storage is the arbiter, not the client.

A consequence worth stating plainly: **expiry alone does not block writes**. A
paused owner can still write after its TTL if no newer owner has acquired the
resource. Its write is only rejected once a newer token exists. This is the
standard fencing model — the stale owner is harmless while nobody else owns the
job, and is fenced the moment a takeover happens.

## Renewal

Use a heartbeat well inside the TTL (one third is a good default):

```python
from agentcommit import LeaseManager

leases = LeaseManager(manager.backend, ttl_seconds=30)

async with leases.hold("job-42", "worker-a", auto_renew=True) as lease:
    await do_long_work(lease)
```

`LeaseManager.hold()`:

* acquires the lease on entry;
* optionally renews it every `ttl / 3` seconds;
* releases it on exit;
* re-raises a renewal failure on exit instead of silently losing ownership.

If a renewal fails with `LeaseLostError`, the work must stop. The process should
not commit under the old token. `LeaseLostError` is **not** in the default retry
policy (`retryable = False`) because retrying the write with the same token is
wrong. The correct recovery is:

1. stop work;
2. re-acquire the lease (which may fail with `LeaseNotAvailableError`);
3. re-read state and redo the operation under the new token.

```python
try:
    await run_job(lease.token)
except LeaseLostError:
    lease = await manager.lease("job-42", owner="worker-a", ttl_seconds=30)
    await run_job(lease.token)
```

## API

| Call | Result |
|---|---|
| `manager.lease(resource, owner, ttl_seconds=30)` | `Lease`; auto-registers resource |
| `backend.acquire_lease(resource, owner, ttl_seconds)` | `Lease`; raises `LeaseNotAvailableError` if another live owner holds it |
| `backend.renew_lease(lease, ttl_seconds=None)` | extended `Lease`; raises `LeaseLostError` if expired/superseded |
| `backend.release_lease(lease)` | `bool`; `False` if not the current owner |
| `backend.current_fence(resource)` | highest token ever issued |
| `LeaseManager.guard(lease)` | raises `LeaseLostError` if superseded |
| `LeaseManager.hold(...)` | async context manager with optional auto-renew |

`Lease.is_expired(now)` is a local clock check. It is optimistic: the backend is
still the authority.

## Backend behaviour

* **memory** — tokens and leases live in the process; same-owner re-acquire
  extends, release deletes the lease but not the token counter.
* **PostgreSQL** — `agentcommit_leases` stores the current owner, token and
  expiry; acquisition takes an advisory transaction lock plus a row lock.
  Release sets the expiry to the epoch; the token remains as the high-water mark.
* **Redis** — `{prefix}:lease:{resource}` stores the lease and
  `{prefix}:fence:{resource}` stores the high-water token. Acquisition and
  release are Lua operations.

## Failure modes

| Error | Meaning | Action |
|---|---|---|
| `LeaseNotAvailableError` | another owner holds a live lease | wait/back off, do not force |
| `LeaseLostError` (commit) | token older than the observed high-water mark | stop, re-acquire, re-read |
| `LeaseLostError` (renew) | lease expired or was superseded | stop, re-acquire |
| `LeaseLostError` (guard) | local token is stale | stop, re-acquire |

Do not use a lease as a mutex around a transaction that also needs isolation.
Use both: the lease prevents duplicate long-running owners, the transaction
validates the state read-modify-write. See
[Isolation](isolation.md) and [Limitations](limitations.md).
