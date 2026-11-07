# Changelog

All notable changes to StatePact will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

* Renamed the project from **AgentCommit** to **StatePact** before the first
  PyPI release, because `agentcommit` normalizes to the already-published
  `agent-commit` distribution. The distribution is now `statepact`, the import
  package is `statepact`, the CLI is `statepact`, and environment variables use
  the `STATEPACT_` prefix. The GitHub repository is
  [mithun-27/StatePact](https://github.com/mithun-27/StatePact). There was no
  prior PyPI release under the old name.

### Added

### Changed

### Fixed

### Deprecated

### Removed

### Security

### Dependencies

## [0.1.0] - 2026-11-06

### Added

* Initial public release (alpha).
* Full core transaction management with isolation modes:
  * `none`, `snapshot`, `optimistic`, `serializable`, `pessimistic`
* Three storage backends: `MemoryBackend`, `PostgresBackend`, `RedisBackend`
* Leases with fencing tokens (`Lease`, `LeaseManager`)
* Idempotent commits via `idempotency_key` and `IdempotentExecutor`
* Retry coordination with exponential backoff (`RetryPolicy`, `run_with_retry`)
* Wait‑for graph deadlock detection and `CAS` operations
* Concurrency utilities (`run_concurrently`, `ConcurrencyLimiter`)
* Observability stack (`EventBus`, `TransactionLog`, `Metrics`)
* Optional OpenTelemetry bridge (`statepact.observability.otel`)
* Framework adapters: LangGraph, CrewAI, AutoGen
* Gradio playground (`demo/app.py`)
* Static browser playground on Hugging Face Spaces (simulation of the isolation rules)
* Benchmark runner with correctness invariants
* Comprehensive test suite (121 unit/property/concurrency/failure-injection, 14 integration)
* CLI (`statepact`)

### Changed

* Default isolation mode changed to `optimistic` (previously documented as default but not enforced).
* `idempotency_fingerprint` defaults to value‑insensitive hash of `(agent_id, resource, key, op)`.
* `LeaseLostError` is not retryable; callers must re‑acquire.
* `TransactionManager.lease` auto‑registers the resource before acquisition.
* Use `StrEnum` for enums and PEP 695 generics where applicable.
* Failure‑injection/unknown commit exceptions are logged as `FAILED`; conflicts are logged by the backend.
* `PostgresBackend` uses `asyncpg` directly with advisory transaction lock; global `pg_advisory_xact_lock` around commit validation/apply.
* `RedisBackend` uses Lua for atomic commit validation/apply with prefix `statepact`.
* Integrations are thin adapters with no framework imports at module load.
* Benchmark runner namespaces all resources with `RUN_ID`; includes `baseline_none` and `hot_key_backoff`.

### Fixed

* README states PyPI 0.1.0 publication is pending (install from source) until Trusted Publishing succeeds.
* README/docs must not claim an `statepact.observability.otel` bridge exists unless it is implemented.
* `LeaseLostError` table in README incorrectly marked `retryable` as `yes*`.
* Fixed docs inaccuracies:
  * `docs/backends.md` mentioned "configured schema" for `clear()`.
  * `docs/integrations.md` said callbacks "must not commit twice" though manager skips commit when already committed.
  * `docs/isolation.md` listed `StaleSnapshotError` as raised though no backend raises it.
* Removed "AMD EPYC-class" claim from README; actual env is AMD64 12-logical-CPU VM.
* Fixed exception inheritance and retryable flags in `exceptions.py`.

### Deprecated

* `statepact.storage.memory.MemoryBackend.fault_injector` – legacy test utility; use property tests instead.
* `statepact.observability.events.EventHook` – will be removed in favor of `Callable[[Event], Any]` in 1.0.0.

### Removed

* None.

## [0.0.1] - Unpublished alpha

### Added

* Proof of concept for optimistic concurrency control.
* Basic `MemoryBackend` implementation.
* Early iteration of lease/fencing and idempotent commit logic.
* Initial documentation draft.

### Changed

* Initial API design decisions.

### Fixed

* None.

### Deprecated

* None.

### Removed

* None.

### Security

* None.

### Dependencies

* None.
