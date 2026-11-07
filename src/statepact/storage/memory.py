"""Deterministic in-memory storage backend.

Guarantees
----------
* ACID for processes that share this backend instance: commit validation and
  application happen under one ``asyncio.Lock``.
* MVCC snapshots with garbage collection driven by active snapshot watermarks.
* Strict 2PL with wait-for-graph deadlock detection.
* Idempotent commits keyed by ``idempotency_key``.
* Fencing-token validation on every write.

Limitations (documented in ``docs/limitations.md``)
---------------------------------------------------
* State is process-local and lost on exit.
* Not safe across processes or threads. Use PostgreSQL or Redis for that.
* Mixing isolation modes in one process is supported but a ``none``/``snapshot``
  writer can still violate another transaction's invariants; only transactions
  using ``optimistic``/``serializable``/``pessimistic`` get the full contract.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from statepact.concurrency import occ
from statepact.concurrency.locking import LockManager, LockStats
from statepact.concurrency.mvcc import (
    VersionEntry,
    apply_entry,
    snapshot_values,
    trim_history,
)
from statepact.concurrency.serializability import (
    SerializationGraphAudit,
    TransactionFootprint,
)
from statepact.coordination.fencing import FenceGuard
from statepact.core.exceptions import (
    ConflictError,
    IdempotencyMismatchError,
    LeaseLostError,
    LeaseNotAvailableError,
    ResourceNotFoundError,
    SerializationError,
    StorageError,
)
from statepact.core.interfaces import (
    CommitPlan,
    CommitResult,
    Lease,
    LockMode,
    ResourceInfo,
    ResourceSnapshot,
    StorageBackend,
    StoredValue,
)
from statepact.core.serde import normalize_value
from statepact.core.version import Version
from statepact.observability.events import Event, EventBus, EventType
from statepact.observability.transaction_logs import (
    TransactionLog,
    TransactionRecord,
    TransactionStatus,
)


@dataclass(slots=True)
class _ResourceState:
    name: str
    metadata: dict[str, Any]
    histories: dict[str, list[VersionEntry]] = field(default_factory=dict)
    version: Version = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class _LeaseState:
    owner: str
    token: int
    expires_at: datetime
    ttl_seconds: float


def _utcnow() -> datetime:
    return datetime.now(UTC)


class MemoryBackend(StorageBackend):
    """Single-process, deterministic reference backend."""

    name = "memory"
    supports_mvcc = True
    supports_locking = True
    supports_leases = True
    supports_idempotency = True
    durable = False

    def __init__(
        self,
        *,
        event_bus: EventBus | None = None,
        transaction_log: TransactionLog | None = None,
        lock_timeout: float = 5.0,
        serialization_window: int = 512,
        retain_versions: int | None = None,
        gc: bool = True,
        clock: Any = _utcnow,
        fault_injector: Any = None,
    ) -> None:
        if retain_versions is not None and retain_versions < 1:
            raise ValueError("retain_versions must be >= 1 or None")
        self._resources: dict[str, _ResourceState] = {}
        self._seq: Version = 0
        self._apply_lock = asyncio.Lock()
        self._snapshot_refs: Counter[int] = Counter()
        self._idempotency: dict[str, tuple[str, CommitResult]] = {}
        self._idempotency_limit = 100_000
        self._leases: dict[str, _LeaseState] = {}
        self._fence_counter: dict[str, int] = {}
        self._fence_guard = FenceGuard()
        self._events = event_bus if event_bus is not None else EventBus()
        self._log = transaction_log if transaction_log is not None else TransactionLog()
        self._locks = LockManager(default_timeout=lock_timeout, events=self._events)
        self._audit = SerializationGraphAudit(window=serialization_window)
        self._retain_versions = retain_versions
        self._gc_enabled = gc
        self._clock = clock
        self._fault_injector = fault_injector
        self._closed = False

    # -- resource management -------------------------------------------------

    async def ensure_resource(
        self,
        name: str,
        initial_state: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._ensure_open()
        _validate_resource_name(name)
        normalized = {
            str(key): normalize_value(value, path=f"initial_state[{key!r}]")
            for key, value in (initial_state or {}).items()
        }
        async with self._apply_lock:
            existing = self._resources.get(name)
            if existing is not None:
                existing.metadata.update(dict(metadata or {}))
                return
            resource = _ResourceState(name=name, metadata=dict(metadata or {}))
            if normalized:
                self._seq += 1
                for key, value in normalized.items():
                    resource.histories[key] = [VersionEntry(self._seq, value)]
                resource.version = self._seq
            self._resources[name] = resource

    async def list_resources(self) -> Sequence[str]:
        self._ensure_open()
        return tuple(sorted(self._resources))

    async def snapshot(self, resource: str) -> ResourceSnapshot:
        self._ensure_open()
        state = self._require(resource)
        # No awaits in this section: the event loop guarantees a consistent view.
        taken_at = self._seq
        values = snapshot_values(state.histories, taken_at)
        self._snapshot_refs[taken_at] += 1
        return ResourceSnapshot(
            resource=resource,
            taken_at=taken_at,
            values=values,
            fence=self._fence_guard.current(resource),
            release_fn=lambda: self._release_snapshot(taken_at),
        )

    async def read_latest(self, resource: str, key: str) -> StoredValue:
        self._ensure_open()
        state = self._require(resource)
        history = state.histories.get(key)
        if not history:
            from statepact.core.exceptions import KeyNotFoundError

            raise KeyNotFoundError(resource, key)
        entry = history[-1]
        if entry.deleted:
            from statepact.core.exceptions import KeyNotFoundError

            raise KeyNotFoundError(resource, key)
        return StoredValue(value=entry.value, version=entry.version)

    async def versions(self, resource: str) -> Mapping[str, Version]:
        self._ensure_open()
        state = self._require(resource)
        return {
            key: history[-1].version
            for key, history in state.histories.items()
            if history and not history[-1].deleted
        }

    async def history(self, resource: str, key: str, *, limit: int = 50) -> list[StoredValue]:
        self._ensure_open()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        state = self._require(resource)
        history = state.histories.get(key, [])
        visible = [
            StoredValue(value=entry.value, version=entry.version)
            for entry in reversed(history)
            if not entry.deleted
        ]
        return visible[:limit]

    async def resource_info(self, resource: str) -> ResourceInfo:
        state = self._require(resource)
        versions = await self.versions(resource)
        return ResourceInfo(
            name=resource,
            keys=len(versions),
            version=state.version,
            metadata=dict(state.metadata),
        )

    # -- commit --------------------------------------------------------------

    async def commit(self, plan: CommitPlan) -> CommitResult:
        self._ensure_open()
        for resource in plan.resources:
            self._require(resource)
        async with self._apply_lock:
            if plan.idempotency_key is not None:
                replay = self._check_idempotency(plan)
                if replay is not None:
                    return replay
            try:
                result = self._commit_locked(plan)
            except ConflictError as exc:
                self._record_conflict(plan, exc)
                raise
            return result

    def _commit_locked(self, plan: CommitPlan) -> CommitResult:
        started = self._clock()
        if plan.read_only:
            # Read-only transactions have no writes to validate and cannot violate
            # an invariant. They commit at their snapshot point; retrying them would
            # only waste agent work.
            finished = self._clock()
            result = CommitResult(
                transaction_id=plan.transaction_id,
                committed=True,
                committed_at=finished,
                commit_version=self._seq,
                read_set=plan.reads,
                write_set=plan.writes,
            )
            self._log.append(
                TransactionRecord.from_plan(
                    plan,
                    status=TransactionStatus.READ_ONLY,
                    commit_version=self._seq,
                    finished_at=finished,
                )
            )
            self._store_idempotency(plan, result)
            return result

        # Fencing is enforced for every resource being written.
        for resource in {write.resource for write in plan.writes}:
            self._fence_guard.validate(resource, plan.fence)

        latest: dict[tuple[str, str], Version] = {}
        for read in plan.reads:
            state = self._require(read.resource)
            history = state.histories.get(read.key, [])
            latest[(read.resource, read.key)] = history[-1].version if history else 0
        for write in plan.writes:
            state = self._require(write.resource)
            history = state.histories.get(write.key, [])
            latest[(write.resource, write.key)] = history[-1].version if history else 0

        self._inject_fault("before_validate")
        outcome = occ.validate(plan, latest)
        if outcome.conflict is not None:
            raise outcome.conflict.to_exception()

        self._inject_fault("after_validate")
        self._inject_fault("before_apply")

        next_seq = self._seq + 1
        if plan.isolation == "serializable":
            footprint = TransactionFootprint(
                transaction_id=plan.transaction_id,
                started_at=plan.started_at,
                committed_at=started,
                commit_version=next_seq,
                reads=plan.reads,
                writes=plan.writes,
            )
            cycle = self._audit.check(footprint)
            if cycle is not None:
                raise SerializationError(
                    transaction_id=plan.transaction_id,
                    cycle=tuple(cycle),
                    message=f"serialization cycle detected: {' -> '.join(cycle)}",
                )

        # The commit point: no awaits, only deterministic dict/list mutations.
        for write in plan.writes:
            state = self._resources[write.resource]
            history = state.histories.setdefault(write.key, [])
            apply_entry(
                history,
                VersionEntry(
                    version=next_seq,
                    value=None if write.is_delete else write.value,
                    deleted=write.is_delete,
                ),
            )
            state.version = next_seq
            state.metadata["last_commit_at"] = started.isoformat()
            if plan.fence is not None:
                self._fence_guard.observe(write.resource, plan.fence)

        self._seq = next_seq
        finished = self._clock()
        result = CommitResult(
            transaction_id=plan.transaction_id,
            committed=True,
            committed_at=finished,
            commit_version=next_seq,
            read_set=plan.reads,
            write_set=plan.writes,
        )
        self._store_idempotency(plan, result)
        self._log.append(
            TransactionRecord.from_plan(
                plan,
                status=TransactionStatus.COMMITTED,
                commit_version=next_seq,
                finished_at=finished,
            )
        )
        self._events.emit(
            Event(
                type=EventType.COMMIT_APPLIED,
                transaction_id=plan.transaction_id,
                agent_id=plan.agent_id,
                message=plan.describe(),
                data={
                    "commit_version": next_seq,
                    "writes": len(plan.writes),
                    "reads": len(plan.reads),
                    "isolation": plan.isolation,
                },
            )
        )
        return result

    def _check_idempotency(self, plan: CommitPlan) -> CommitResult | None:
        assert plan.idempotency_key is not None
        existing = self._idempotency.get(plan.idempotency_key)
        if existing is None:
            return None
        request_hash, result = existing
        if request_hash != plan.request_hash:
            self._log.append(
                TransactionRecord.from_plan(
                    plan,
                    status=TransactionStatus.FAILED,
                    abort_reason="idempotency key reused with a different payload",
                )
            )
            raise IdempotencyMismatchError(
                f"idempotency key {plan.idempotency_key!r} was already used with a different payload",
                idempotency_key=plan.idempotency_key,
            )
        return replace(result, replayed=True)

    def _store_idempotency(self, plan: CommitPlan, result: CommitResult) -> None:
        if plan.idempotency_key is None:
            return
        if len(self._idempotency) >= self._idempotency_limit:
            oldest = next(iter(self._idempotency))
            self._idempotency.pop(oldest, None)
        self._idempotency[plan.idempotency_key] = (plan.request_hash or "", result)

    def _record_conflict(self, plan: CommitPlan, error: ConflictError) -> None:
        self._log.append(
            TransactionRecord.from_plan(
                plan,
                status=TransactionStatus.ABORTED,
                finished_at=self._clock(),
                conflict_class=error.conflict_class,
                abort_reason=str(error),
            )
        )

    # -- snapshots / GC ------------------------------------------------------

    def _release_snapshot(self, taken_at: Version) -> None:
        count = self._snapshot_refs.get(taken_at, 0)
        if count <= 0:  # pragma: no cover - double release guard
            return
        if count == 1:
            del self._snapshot_refs[taken_at]
        else:
            self._snapshot_refs[taken_at] = count - 1
        if self._gc_enabled:
            self._collect_garbage()

    def _collect_garbage(self) -> None:
        watermark = min(self._snapshot_refs) if self._snapshot_refs else self._seq
        reclaimed = 0
        for state in self._resources.values():
            for key, history in list(state.histories.items()):
                trimmed = trim_history(history, watermark)
                if self._retain_versions is not None:
                    # Never trim below the requested retention: keep the newest
                    # ``retain_versions`` entries *and* anything a live snapshot
                    # still needs. Safe because older versions are only needed
                    # for audit/history, not for correctness.
                    start = len(history) - len(trimmed)
                    start = min(start, max(0, len(history) - self._retain_versions))
                    trimmed = list(history[start:])
                if len(trimmed) != len(history):
                    reclaimed += len(history) - len(trimmed)
                    state.histories[key] = trimmed
        if reclaimed:
            self._events.emit(
                Event(
                    type=EventType.GC,
                    message=f"reclaimed {reclaimed} obsolete versions",
                    data={"watermark": watermark, "reclaimed": reclaimed},
                )
            )

    # -- locking -------------------------------------------------------------

    async def acquire_lock(
        self,
        transaction_id: str,
        resource: str,
        key: str,
        *,
        mode: LockMode,
        timeout: float,
        agent_id: str | None = None,
    ) -> None:
        self._ensure_open()
        self._require(resource)
        await self._locks.acquire(transaction_id, resource, key, mode, timeout, agent_id=agent_id)

    async def release_locks(self, transaction_id: str) -> None:
        self._locks.release_all(transaction_id)

    def lock_snapshot(self) -> dict[str, Any]:
        return self._locks.snapshot()

    @property
    def lock_stats(self) -> LockStats:
        return self._locks.stats

    # -- leases --------------------------------------------------------------

    async def acquire_lease(self, resource: str, owner: str, ttl_seconds: float) -> Lease:
        self._ensure_open()
        self._require(resource)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        now = self._clock()
        current = self._leases.get(resource)
        if current is not None and current.expires_at > now:
            if current.owner != owner:
                raise LeaseNotAvailableError(resource, owner, current.owner)
            current.expires_at = now + _seconds(ttl_seconds)
            current.ttl_seconds = ttl_seconds
            return Lease(resource, owner, current.token, current.expires_at, ttl_seconds)
        token = self._fence_counter.get(resource, 0) + 1
        self._fence_counter[resource] = token
        self._fence_guard.observe(resource, token)
        expires_at = now + _seconds(ttl_seconds)
        self._leases[resource] = _LeaseState(owner, token, expires_at, ttl_seconds)
        lease = Lease(resource, owner, token, expires_at, ttl_seconds)
        self._events.emit(
            Event(
                type=EventType.LEASE_ACQUIRED,
                agent_id=owner,
                resource=resource,
                data={"token": token, "ttl_seconds": ttl_seconds},
            )
        )
        return lease

    async def renew_lease(self, lease: Lease, ttl_seconds: float | None = None) -> Lease:
        self._ensure_open()
        current = self._leases.get(lease.resource)
        now = self._clock()
        if (
            current is None
            or current.owner != lease.owner
            or current.token != lease.token
            or current.expires_at <= now
        ):
            raise LeaseLostError(
                resource=lease.resource,
                token=lease.token,
                message=f"cannot renew expired or superseded lease {lease.token}",
            )
        ttl = ttl_seconds if ttl_seconds is not None else current.ttl_seconds
        if ttl <= 0:
            raise ValueError("ttl_seconds must be positive")
        current.expires_at = now + _seconds(ttl)
        current.ttl_seconds = ttl
        self._events.emit(
            Event(
                type=EventType.LEASE_RENEWED,
                agent_id=lease.owner,
                resource=lease.resource,
                data={"token": lease.token, "ttl_seconds": ttl},
            )
        )
        return Lease(lease.resource, lease.owner, lease.token, current.expires_at, ttl)

    async def release_lease(self, lease: Lease) -> bool:
        self._ensure_open()
        current = self._leases.get(lease.resource)
        if current is None or current.owner != lease.owner or current.token != lease.token:
            return False
        del self._leases[lease.resource]
        self._events.emit(
            Event(
                type=EventType.LEASE_RELEASED,
                agent_id=lease.owner,
                resource=lease.resource,
                data={"token": lease.token},
            )
        )
        return True

    async def current_fence(self, resource: str) -> int:
        self._ensure_open()
        return self._fence_counter.get(resource, 0)

    # -- observability / lifecycle ------------------------------------------

    @property
    def log(self) -> TransactionLog:
        return self._log

    @property
    def event_bus(self) -> EventBus:
        return self._events

    @property
    def sequence(self) -> Version:
        return self._seq

    def transactions(self, limit: int = 100) -> list[TransactionRecord]:
        return self._log.recent(limit)

    def stats(self) -> dict[str, Any]:
        return {
            "backend": self.name,
            "sequence": self._seq,
            "resources": len(self._resources),
            "keys": sum(len(state.histories) for state in self._resources.values()),
            "active_snapshots": sum(self._snapshot_refs.values()),
            "snapshot_watermarks": len(self._snapshot_refs),
            "idempotency_records": len(self._idempotency),
            "active_leases": len(self._leases),
            "locks": self._locks.snapshot(),
            "serialization": dict(self._audit.stats()),
            "transactions_logged": len(self._log),
        }

    async def health_check(self) -> bool:
        return not self._closed

    async def close(self) -> None:
        if self._closed:
            return
        for transaction_id in list(self._locks._held):
            self._locks.release_all(transaction_id)
        self._closed = True

    def clear(self) -> None:
        """Reset all state. Intended for tests and the deterministic playground."""

        self._resources.clear()
        self._idempotency.clear()
        self._leases.clear()
        self._fence_counter.clear()
        self._fence_guard = FenceGuard()
        self._snapshot_refs.clear()
        self._log.clear()
        self._events.clear()
        self._seq = 0
        self._closed = False

    # -- internals -----------------------------------------------------------

    def _require(self, resource: str) -> _ResourceState:
        state = self._resources.get(resource)
        if state is None:
            raise ResourceNotFoundError(
                f"resource {resource!r} is not registered", resource=resource
            )
        return state

    def _ensure_open(self) -> None:
        if self._closed:
            raise StorageError("memory backend is closed")

    def _inject_fault(self, point: str) -> None:
        """Test-only hook. Never set in production; see ``tests/failure_injection``."""

        if self._fault_injector is not None:
            self._fault_injector(point)


def _seconds(ttl_seconds: float) -> Any:
    from datetime import timedelta

    return timedelta(seconds=ttl_seconds)


def _validate_resource_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("resource name must be a non-empty string")
    if "\x00" in name:
        raise ValueError("resource name must not contain NUL characters")
