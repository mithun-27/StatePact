# Retries

A conflict is a normal outcome, not a bug. The transaction did not commit, so the
safe response is to run the **whole body again on a fresh snapshot**. Staged
writes from the failed attempt are discarded; they are never reused.

`manager.run` does this for you:

```python
value = await manager.run(
    "agent-1",
    [inventory],
    reserve_one_unit,
    isolation="optimistic",
    retry_policy=RetryPolicy(max_attempts=100, base_delay=0.002, jitter=0.3),
)
```

The callable receives a new `AgentSession` per attempt:

```python
async def reserve_one_unit(session):
    stock = await session.read("sku-1")
    if stock <= 0:
        return False
    await session.write("sku-1", stock - 1)
    return True
```

Never retry inside the body. Re-reading a key after a conflict and writing again
inside the same session is wrong: the transaction is already aborted.

## RetryPolicy

```python
from agentcommit import RetryPolicy

policy = RetryPolicy(
    max_attempts=5,
    base_delay=0.005,
    max_delay=1.0,
    multiplier=2.0,
    jitter=0.2,
    retry_on=(ConflictError,),
)
```

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `5` | total attempts, including the first |
| `base_delay` | `0.005` | delay after the first failure (seconds) |
| `max_delay` | `1.0` | cap on the exponential delay |
| `multiplier` | `2.0` | growth factor, must be `>= 1` |
| `jitter` | `0.2` | fraction of the delay randomised, `0..1` |
| `retry_on` | `(ConflictError,)` | exception types eligible for retry |

The delay before attempt `n + 1` is:

```text
raw   = min(max_delay, base_delay * multiplier ** (n - 1))
delay = uniform(raw * (1 - jitter), raw)
```

With `jitter=0` the delay is exactly `raw`. With `base_delay=0` retries are
immediate — useful for tests and for demonstrating retry amplification, not for
production hot keys.

## What is retried

The policy retries an error only if **both** are true:

1. the error is an instance of one of `retry_on`;
2. for `AgentCommitError`, `error.retryable` is `True`.

| Error | Retryable | Notes |
|---|---|---|
| `ReadWriteConflict`, `WriteWriteConflict` | yes | transaction not committed |
| `StaleSnapshotError` | yes | transaction not committed |
| `DeadlockError`, `LockTimeoutError` | yes | transaction not committed |
| `SerializationError` | yes | transaction not committed |
| `LeaseLostError` | **no** | re-acquire the lease first |
| `IdempotencyMismatchError` | no | programming error; do not retry |
| `RetryExhaustedError` | no | policy already gave up |
| `StorageError` | no | commit state may be unknown |
| arbitrary exception | no | by default it is not safe to assume anything |

An unknown exception (a bug in your code, a timeout from an external API, an
unexpected database error) is never retried implicitly. If an application-level
exception is genuinely safe to repeat, you can opt it in:

```python
class TransientToolError(Exception):
    ...

policy = RetryPolicy(
    max_attempts=10,
    retry_on=(ConflictError, TransientToolError),
)
```

`retry_on` can select non-AgentCommit exception types, but it cannot override
`retryable=False` on an AgentCommit error. A `StorageError` stays non-retryable
even if you list it, because the commit outcome may be unknown. A plain
`TransientToolError` is retried only because the application asserts it is safe.

## Attempts, errors and outcomes

`run` returns the work result and raises on exhaustion. Use `run_outcome` when
you need observability or custom handling:

```python
outcome = await manager.run_outcome("agent-1", [inventory], reserve_one_unit)
print(outcome.value)
print(outcome.attempts)        # 1 on first-try success
print(outcome.total_delay)     # seconds slept
print(outcome.errors)          # every conflict, in order
print(outcome.retried)         # attempts > 1
```

When the policy gives up:

* if the first attempt failed with a non-retryable error, that original error is
  raised;
* otherwise `RetryExhaustedError(attempts, last_error)` is raised, with the last
  error as `__cause__`.

## Replanning between attempts

The agent may need to react to a conflict (discard a stale plan, re-check a tool
result, choose a different product):

```python
async def replan(error: BaseException, attempt: int) -> None:
    await agent.observe_conflict(error, attempt)

await manager.run("agent-1", [inventory], work, replan=replan)
```

`replan` is called **after** a retryable failure and **before** the next attempt
takes its fresh snapshot. It may be sync or async. The manager's own metrics and
event emission happen through an internal `on_retry` callback; user code should
use `replan`.

## Idempotency and ambiguous failures

A conflict is unambiguous: the transaction did not commit. A transport failure
during `commit()` is ambiguous: the backend may have applied the writes before
the reply was lost. AgentCommit logs such an attempt as `FAILED` and does not
retry it by default, because `StorageError` is not retryable.

To make a manual resubmission safe, pass an idempotency key and replay the same
operation yourself:

```python
from agentcommit import StorageError

async def charge_with_replay() -> None:
    try:
        await manager.run(
            "billing",
            [wallet],
            charge,
            idempotency_key="invoice-42",
        )
    except StorageError:
        # The first attempt may have committed. The same key makes this safe.
        await manager.run(
            "billing",
            [wallet],
            charge,
            idempotency_key="invoice-42",
        )
```

The replay returns the original result instead of applying twice. Do not add
`StorageError` to `retry_on` and expect an automatic retry; the `retryable=False`
flag is authoritative. See [Idempotency](idempotency.md).

## Backoff and hot keys

Immediate retries synchronise agents into a thundering herd: everyone retries,
everyone conflicts again. Jitter desynchronises them.

From the [benchmark](benchmarks.md), same 40 agents and 400 commits on one key:

| Backend | Zero backoff | Jittered backoff |
|---|---:|---:|
| memory | 348 ops/s, 10.75 attempts/op | 246 ops/s, 3.31 attempts/op |
| PostgreSQL | 8.3 ops/s, 13.03 attempts/op | 7.6 ops/s, 13.03 attempts/op |
| Redis | 19.5 ops/s, 20.60 attempts/op | 26.7 ops/s, 14.21 attempts/op |

Jitter does not automatically raise throughput. It reduces wasted attempts;
whether that becomes throughput depends on the backend and the critical section.
For a genuinely hot key, shard it or use `pessimistic` locking with a queue
instead of hoping the retry policy saves you.

## Tuning checklist

1. Start with `optimistic` and a small number of attempts.
2. Measure `outcome.attempts` and conflict rate per resource; aggregate with
   `Metrics` (see [Operations](operations.md)).
3. If conflicts are rare, keep retries cheap (small base delay, moderate jitter).
4. If conflicts are frequent, add jitter and raise `max_attempts`; consider
   sharding or pessimistic locking.
5. Always add an idempotency key when the operation is a business action that
   must not repeat.
