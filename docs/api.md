# API Reference

This document lists the public, stable API of StatePact 0.1.0. The surface is intentionally small so you can build multiple agents on a single manager.

## Top‑level exports

### ``TransactionManager``

The composition root. Owns a backend and provides session/retry helpers.

```python
from statepact import TransactionManager

manager = TransactionManager()  # defaults to MemoryBackend
```

**Methods**

| Method | Signature | Description |
|---|---|---|
| ``resource(name, initial_state=None, *, metadata=None)`` | Creates a ``SharedResource`` bound to this manager. |
| ``ensure_resources(*resources)`` | Registers resources with the backend. Sessions also do this on entry. |
| ``session(*, agent_id, resources=(), isolation="optimistic", idempotency_key=None, idempotency_fingerprint=None, fence=None, metadata=None, lock_timeout=None)`` | Returns an async context manager that yields an ``AgentSession``. |
| ``run(agent_id, resources, work, *, isolation="optimistic", retry_policy=None, replan=None, idempotency_key=None, idempotency_fingerprint=None, metadata=None, fence=None, lock_timeout=None, sleep=asyncio.sleep, rng=None)`` | Runs ``work(session)`` with retry handling; returns the work’s result. |
| ``run_outcome(...)`` | Same arguments as ``run``; returns ``RetryOutcome`` instead of the bare value. |
| ``lease(resource, owner, ttl_seconds=30)`` | Acquires a lease and registers the resource if absent. |
| ``health_check()`` | Returns ``True`` when the backend responds; backends catch their own transport errors and return ``False``. |
| ``transactions(limit=100)`` | Returns recent ``TransactionRecord``s from the ``TransactionLog``. |
| ``stats()`` | Backend stats merged with a metrics snapshot. |
| ``close()`` | Closes the backend and awaits in-flight async event hooks. |

``run_outcome`` returns ``RetryOutcome`` with ``value``, ``attempts``,
``total_delay``, ``errors`` and the ``retried`` property.

**Attributes**

* ``backend`` – the ``StorageBackend`` instance.
* ``backend_name`` – short backend identifier.
* ``event_bus`` – the ``EventBus`` for observability hooks.
* ``log`` – the ``TransactionLog`` for debugging.
* ``metrics`` – the ``Metrics`` collector.
* ``retry_policy`` – the default ``RetryPolicy`` for ``run``.

### ``AgentSession``

A read‑modify‑write session that stages writes and commits atomically.

**Constructor**

```python
async with manager.session(agent_id="worker", resources=[inventory]) as session:
```

**Methods**

| Method | Signature | Description |
|---|---|---|
| ``read(key, *, resource=None)`` | Returns a value; raises ``KeyNotFoundError`` when absent. | |
| ``get(key, *, resource=None, default=None)`` | Returns a value or ``default``; ``default`` is keyword‑only. | |
| ``get_entry(key, *, resource=None)`` | Returns a ``StoredValue`` or ``None``; internal. | |
| ``version(key, *, resource=None)`` | Returns the version observed by this transaction (0 when absent). | |
| ``write(key, value, *, resource=None, expected_version=None)`` | Stages a write; nothing is published until ``commit``. | |
| ``delete(key, *, resource=None, expected_version=None)`` | Stages a delete; absent keys are a no‑op only if base is 0. | |
| ``commit()`` | Validates and applies staged writes; returns ``CommitResult``. | |
| ``abort(reason="aborted by caller")`` | Discards staged writes and releases locks/snapshots. | |

**Attributes**

* ``agent_id``, ``transaction_id``, ``isolation`` – session identity.
* ``state`` – the ``TransactionState`` (``active``, ``committing``, ``committed``, ``aborted``, ``failed``).
* ``read_set`` – mapping ``(resource, key) → version observed``.
* ``write_set`` – mapping ``(resource, key) → staged value``.
* ``commit_result`` – last ``CommitResult`` from ``commit`` (or ``None``).
* ``resources`` – the resources declared for this session.
* ``retry_attempt`` – 1-based attempt number assigned by the manager.

### ``SharedResource``

Non‑transactional diagnostics: reads current state, versions and history. These
helpers take no part in transaction validation.

**Methods**

| Method | Signature | Description |
|---|---|---|
| ``ensure(manager)`` | Binds the resource and registers it with the backend. | |
| ``read(key)`` | Returns the latest committed value (or raises ``KeyNotFoundError``). | |
| ``get(key, default=None)`` | Returns a value or ``default`` (positional or keyword). | |
| ``version(key)`` | Returns the latest committed version (0 when absent). | |
| ``versions()`` | Returns a mapping ``{key: version}`` for the whole resource. | |
| ``history(key, *, limit=50)`` | Returns ``StoredValue`` entries, newest first. Backends that cannot provide history raise ``NotImplementedError``. | |
| ``state()`` | Returns ``{key: value}`` from a temporary released snapshot. | |
| ``snapshot()`` | Returns a ``ResourceSnapshot``; the caller must call ``release()``. | |

### ``RetryPolicy``

Configures retry delays and eligible exception types.

```python
from statepact import RetryPolicy

policy = RetryPolicy(
    max_attempts=5,
    base_delay=0.005,
    max_delay=1.0,
    multiplier=2.0,
    jitter=0.2,
    retry_on=(ConflictError,),
)
```

**Fields**

* ``max_attempts`` – total attempts, including the first.
* ``base_delay`` – seconds after the first failure.
* ``max_delay`` – cap on exponential delay.
* ``multiplier`` – growth factor (``>= 1``).
* ``jitter`` – fraction of the delay randomised (``0..1``).
* ``retry_on`` – tuple of exception types that may be retried.

### ``Lease``

Represents a fencing token and ownership of a long‑running job.

**Constructor**

```python
lease = await manager.lease("job-42", owner="worker-a", ttl_seconds=30)
```

**Fields**

* ``resource`` – name of the leased resource.
* ``owner`` – client identifier.
* ``token`` – monotonic fence token (``int``).
* ``expires_at`` – ``datetime`` when the lease expires.
* ``ttl_seconds`` – lease duration.

**Methods**

* ``is_expired(now)`` – returns ``True`` if local clock indicates expiry.

### ``LeaseManager``

Helper for long‑running work with optional auto‑renewal.

```python
from statepact import LeaseManager

leases = LeaseManager(manager.backend, ttl_seconds=30)
async with leases.hold("job-42", "worker-a", auto_renew=True) as lease:
    await do_long_work(lease)
```

**Methods**

* ``acquire(resource, owner, ttl_seconds=None)`` – acquire or extend.
* ``renew(lease, ttl_seconds=None)`` – extend; raises ``LeaseLostError`` if superseded.
* ``release(lease)`` – release; returns ``False`` if no longer the owner.
* ``guard(lease)`` – raise ``LeaseLostError`` if a newer token exists.
* ``hold(...)`` – async context manager; optional background renewal at ``ttl / 3``,
  releasing on exit and re-raising a renewal failure.

### ``IdempotentExecutor``

Effect journal for at‑most‑once completion of external API calls.

```python
from statepact import IdempotentExecutor, MemoryEffectStore

effects = IdempotentExecutor(MemoryEffectStore())
charge = await effects.run("charge:invoice-42", lambda: payment_api.charge(amount=30))
```

**Methods**

* ``reserve(key, *, payload=None)`` – durably records intent; use with the two‑phase pattern.
* ``execute_reserved(key, action)`` – executes a previously reserved effect.
* ``run(key, action, ...)`` – one‑shot execution with at‑most‑once completion.

### ``ConfigurationError``

Raised when the manager, session or backend configuration is invalid (for example,
a missing `otel` extra or a resource bound to a different manager).

### ``BackendUnavailableError``

A ``StorageError`` subclass provided for applications that want to classify
connectivity failures. The built-in backends return ``False`` from
``health_check()`` instead of raising it.

## Exceptions hierarchy

All exceptions inherit from ``StatePactError``. ``ConflictError`` and
``RetryExhaustedError`` inherit from ``TransactionAbortedError``.

| Exception | ``retryable`` | When raised |
|---|---|---|
| ``ConflictError`` | depends on subclass | any conflict during validation |
| ``ReadWriteConflict`` | ``True`` | a key this transaction read changed before commit |
| ``WriteWriteConflict`` | ``True`` | another transaction wrote a key first |
| ``DeadlockError`` | ``True`` | wait‑for cycle victim |
| ``LockTimeoutError`` | ``True`` | lock acquisition timed out |
| ``SerializationError`` | ``True`` | audit found a cycle |
| ``LeaseLostError`` | ``False`` | stale fencing token (must re‑acquire lease) |
| ``LeaseNotAvailableError`` | ``False`` | another live owner holds the lease |
| ``IdempotencyMismatchError`` | ``False`` | same key, different fingerprint |
| ``EffectInProgressError`` | ``True`` | another worker owns the effect |
| ``EffectFailedError`` | ``True`` | a previous effect attempt failed |
| ``SideEffectError`` | ``False`` | the effect journal could not be recorded safely |
| ``RetryExhaustedError`` | ``False`` | retry policy gave up |
| ``StorageError`` | ``False`` | backend/transport failure; commit state unknown |
| ``BackendUnavailableError`` | ``False`` | connectivity failure (``StorageError`` subclass) |

``StaleSnapshotError`` is defined and exported for API completeness but is not
raised by the built-in backends.

Each conflict carries ``resource``, ``key``, ``expected_version`` and ``actual_version`` when the backend can supply them.

## Integration adapters

The adapters live in ``statepact.integrations`` and do not import the framework at module load.

* ``statepact.integrations.base`` – generic helpers (``run_transaction``, ``read_modify_write``, ``transactional``, ``run_sync``).
* ``statepact.integrations.langgraph`` – ``LangGraphCoordinator.node`` and ``.update``.
* ``statepact.integrations.crewai`` – ``transaction_tool`` and ``TransactionTool``.
* ``statepact.integrations.autogen`` – ``AutoGenTransactionMiddleware.transactional``.

## Observability components

All components are exported from ``statepact.observability``.

* ``Event``, ``EventBus``, ``EventType`` – structured lifecycle events.
* ``TransactionLog``, ``TransactionRecord``, ``TransactionStatus`` – bounded in‑memory transaction snapshot.
* ``Metrics`` – counters, gauges and latency histograms.
* ``summarize_latencies`` – utility for percentile calculations.

Optional OpenTelemetry bridge: ``statepact.observability.otel``.

## Backends

* ``statepact.storage.memory.MemoryBackend`` – in‑process, deterministic, test‑reference.
* ``statepact.storage.postgres.PostgresBackend`` – requires ``statepact[postgres]``.
* ``statepact.storage.redis.RedisBackend`` – requires ``statepact[redis]``.

## Configuration

``TransactionManager`` accepts an optional ``backend`` argument. If omitted, it defaults to ``MemoryBackend``.

For PostgreSQL and Redis, use the convenience factory methods:

```python
from statepact.storage.postgres import PostgresBackend
from statepact.storage.redis import RedisBackend

backend = await PostgresBackend.connect(dsn)  # schema is created automatically
manager = TransactionManager(backend)
```

``MemoryBackend`` accepts ``lock_timeout``, ``serialization_window``, ``retain_versions`` and ``gc`` parameters.

## Testing utilities

* ``statepact.testing.run_concurrently`` – runs multiple agents in parallel against a manager.
* ``statepact.storage.memory.MemoryBackend.fault_injector`` – injects failures for tests.

## Footnotes

* ``AgentSession.get`` takes ``default`` as keyword‑only; ``SharedResource.get`` accepts it positionally or by keyword.
* ``StoredValue`` and ``TransactionRecord`` are dataclasses that expose value, version and metadata.
* ``SharedResource`` is read‑only from the application perspective; all mutations go through a session.
* The ``otel`` extra installs ``opentelemetry-api`` and ``opentelemetry-sdk``; the bridge only needs the API.

## See also

* [Quickstart](quickstart.md)
* [Concepts](concepts.md)
* [Isolation](isolation.md)
* [Backends](backends.md)
* [Leases](leases.md)
* [Idempotency](idempotency.md)
* [Retries](retries.md)
* [Integrations](integrations.md)
* [Benchmarks](benchmarks.md)
* [Operations](operations.md)
* [Limitations](limitations.md)
