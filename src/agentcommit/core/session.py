"""The developer-facing transaction session.

A session captures a snapshot, lets agent code read and stage writes without
holding storage locks, and validates everything atomically at commit. That is the
whole point: the expensive part of an agent (reasoning, tool calls, LLM latency)
runs outside the critical section.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agentcommit.core.context import Token, bind_session, unbind_session
from agentcommit.core.exceptions import (
    AmbiguousResourceError,
    ConflictError,
    KeyNotFoundError,
    ResourceNotInSessionError,
    TransactionStateError,
)
from agentcommit.core.resource import SharedResource
from agentcommit.core.serde import normalize_value
from agentcommit.core.transaction import (
    Transaction,
    TransactionState,
    new_transaction_id,
)
from agentcommit.core.version import UNVERSIONED, Version
from agentcommit.observability.events import Event, EventType
from agentcommit.observability.transaction_logs import TransactionRecord, TransactionStatus

if TYPE_CHECKING:  # pragma: no cover
    from agentcommit.core.interfaces import CommitResult, IsolationMode, StoredValue
    from agentcommit.core.manager import TransactionManager

logger = logging.getLogger("agentcommit.session")


class AgentSession:
    """Scope one agent's transaction over one or more shared resources.

    Example::

        async with AgentSession(agent_id="planner-1", resources=[inventory]) as session:
            stock = await session.read("sku-1")
            if stock <= 0:
                return "OUT_OF_STOCK"
            await session.write("sku-1", stock - 1)
            await session.commit()

    Exiting the block without calling :meth:`commit` aborts the transaction. There
    is no implicit auto-commit: a successful commit is always explicit in both
    code and logs.
    """

    __slots__ = (
        "_agent_id",
        "_commit_result",
        "_entered",
        "_exited",
        "_fence",
        "_idempotency_fingerprint",
        "_idempotency_key",
        "_isolation",
        "_lock_timeout",
        "_manager",
        "_metadata",
        "_resources",
        "_retry_attempt",
        "_session_token",
        "_started_perf",
        "_transaction_id",
        "_tx",
    )

    def __init__(
        self,
        *,
        agent_id: str,
        resources: Sequence[SharedResource] = (),
        isolation: IsolationMode = "optimistic",
        manager: TransactionManager | None = None,
        idempotency_key: str | None = None,
        idempotency_fingerprint: str | None = None,
        fence: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        transaction_id: str | None = None,
        lock_timeout: float | None = None,
        retry_attempt: int = 1,
    ) -> None:
        if not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id must be a non-empty string")
        from agentcommit.core.interfaces import ISOLATION_MODES

        if isolation not in ISOLATION_MODES:
            raise ValueError(
                f"unknown isolation {isolation!r}; expected one of {sorted(ISOLATION_MODES)}"
            )
        if fence is not None and fence < 0:
            raise ValueError("fence must be non-negative")
        names = [resource.name for resource in resources]
        if len(names) != len(set(names)):
            raise ValueError("resources must be unique within a session")

        self._agent_id = agent_id
        self._resources = tuple(resources)
        self._isolation = isolation
        self._manager = manager
        self._idempotency_key = idempotency_key
        self._idempotency_fingerprint = idempotency_fingerprint
        self._fence = fence
        self._metadata = dict(metadata or {})
        self._transaction_id = transaction_id or new_transaction_id()
        self._lock_timeout = lock_timeout
        self._retry_attempt = retry_attempt
        self._tx: Transaction | None = None
        self._entered = False
        self._exited = False
        self._commit_result: CommitResult | None = None
        self._session_token: Token[AgentSession | None] | None = None
        self._started_perf = 0.0

    # -- properties ----------------------------------------------------------

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    @property
    def isolation(self) -> IsolationMode:
        return self._isolation

    @property
    def state(self) -> TransactionState:
        if self._tx is None:
            return TransactionState.ACTIVE if self._entered else TransactionState.ABORTED
        return self._tx.state

    @property
    def manager(self) -> TransactionManager:
        return self._get_manager()

    @property
    def commit_result(self) -> CommitResult | None:
        return self._commit_result

    @property
    def read_set(self) -> Mapping[tuple[str, str], Version]:
        return dict(self._tx.read_set) if self._tx else {}

    @property
    def write_set(self) -> Mapping[tuple[str, str], Any]:
        return dict(self._tx.write_set) if self._tx else {}

    @property
    def resources(self) -> Sequence[SharedResource]:
        return self._resources

    @property
    def retry_attempt(self) -> int:
        return self._retry_attempt

    # -- lifecycle -----------------------------------------------------------

    def _get_manager(self) -> TransactionManager:
        if self._manager is not None:
            return self._manager
        for resource in self._resources:
            if resource.manager is not None:
                self._manager = resource.manager
                return self._manager
        from agentcommit.core.manager import default_manager

        self._manager = default_manager()
        return self._manager

    async def __aenter__(self) -> AgentSession:
        if self._entered:
            raise TransactionStateError("this AgentSession has already been entered")
        manager = self._get_manager()
        self._entered = True
        self._started_perf = time.perf_counter()
        try:
            for resource in self._resources:
                await resource.ensure(manager)
            self._tx = Transaction(
                id=self._transaction_id,
                agent_id=self._agent_id,
                isolation=self._isolation,
                resources=tuple(resource.name for resource in self._resources),
                started_at=datetime.now(UTC),
                fence=self._fence,
                idempotency_key=self._idempotency_key,
                idempotency_fingerprint=self._idempotency_fingerprint,
                attempt=self._retry_attempt,
                metadata=dict(self._metadata),
            )
            if self._isolation in {"optimistic", "snapshot", "serializable"}:
                for resource in self._resources:
                    self._tx.snapshots[resource.name] = await manager.backend.snapshot(
                        resource.name
                    )
        except BaseException:
            self._release_snapshots()
            self._entered = False
            raise
        self._session_token = bind_session(self)
        manager.event_bus.emit(
            Event(
                type=EventType.SESSION_STARTED,
                transaction_id=self._transaction_id,
                agent_id=self._agent_id,
                message=f"isolation={self._isolation}",
                data={"attempt": self._retry_attempt, "isolation": self._isolation},
            )
        )
        return self

    async def __aexit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        try:
            if self._tx is not None and self._tx.state in {
                TransactionState.ACTIVE,
                TransactionState.COMMITTING,
            }:
                reason = (
                    f"session exited without commit ({type(exc).__name__})"
                    if exc
                    else ("session exited without commit")
                )
                try:
                    await self.abort(reason)
                except Exception:  # pragma: no cover - must not mask the original error
                    logger.exception("abort during session exit failed")
        finally:
            self._exited = True
            if self._session_token is not None:
                unbind_session(self._session_token)
                self._session_token = None

    # -- reads ---------------------------------------------------------------

    def _require_active(self) -> Transaction:
        if self._tx is None:
            raise TransactionStateError(
                "session is not active; use `async with AgentSession(...)` before reading or writing"
            )
        if not self._tx.is_active:
            raise TransactionStateError(
                f"transaction {self._tx.id} is {self._tx.state.value}; it can no longer read or write"
            )
        return self._tx

    def _resolve(self, resource: SharedResource | str | None) -> str:
        if resource is None:
            if len(self._resources) == 1:
                return self._resources[0].name
            if not self._resources:
                raise ResourceNotInSessionError("session has no resources")
            raise AmbiguousResourceError(
                "session has multiple resources; pass resource=<name or SharedResource>"
            )
        name = resource.name if isinstance(resource, SharedResource) else resource
        for candidate in self._resources:
            if candidate.name == name:
                return name
        raise ResourceNotInSessionError(
            f"resource {name!r} is not part of this session", resource=name
        )

    async def get_entry(
        self, key: str, *, resource: SharedResource | str | None = None
    ) -> StoredValue | None:
        """Read a key and track the version, returning ``None`` when absent."""

        tx = self._require_active()
        name = self._resolve(resource)
        manager = self._get_manager()
        backend = manager.backend

        staged = tx.staged(name, key)
        if staged is not None:
            if staged.is_delete:
                return None
            known_version = tx.read_version(name, key) or staged.expected_version
            from agentcommit.core.interfaces import StoredValue

            return StoredValue(value=staged.value, version=known_version)

        if self._isolation == "pessimistic":
            await backend.acquire_lock(
                tx.id,
                name,
                key,
                mode="shared",
                timeout=self._lock_timeout or backend_default_timeout(backend),
                agent_id=self._agent_id,
            )
            try:
                stored = await backend.read_latest(name, key)
                tx.record_read(name, key, stored.version)
                return stored
            except KeyNotFoundError:
                tx.record_read(name, key, UNVERSIONED)
                return None

        if self._isolation == "none":
            try:
                stored = await backend.read_latest(name, key)
            except KeyNotFoundError:
                tx.record_read(name, key, UNVERSIONED)
                return None
            tx.record_read(name, key, stored.version)
            return stored

        snapshot = tx.snapshots[name]
        entry = snapshot.get(key)
        tx.record_read(name, key, snapshot.version_of(key))
        return entry

    async def read(self, key: str, *, resource: SharedResource | str | None = None) -> Any:
        """Read a value, raising :class:`KeyNotFoundError` when absent."""

        name = self._resolve(resource)
        stored = await self.get_entry(key, resource=name)
        if stored is None:
            raise KeyNotFoundError(name, key)
        return stored.value

    async def get(
        self,
        key: str,
        *,
        resource: SharedResource | str | None = None,
        default: Any = None,
    ) -> Any:
        """Read a value, returning ``default`` when absent (absence is tracked)."""

        stored = await self.get_entry(key, resource=resource)
        return default if stored is None else stored.value

    async def version(self, key: str, *, resource: SharedResource | str | None = None) -> Version:
        """Version observed by this transaction (0 when absent)."""

        tx = self._require_active()
        name = self._resolve(resource)
        known = tx.read_version(name, key)
        if known is not None:
            return known
        stored = await self.get_entry(key, resource=name)
        return UNVERSIONED if stored is None else stored.version

    # -- writes --------------------------------------------------------------

    async def write(
        self,
        key: str,
        value: Any,
        *,
        resource: SharedResource | str | None = None,
        expected_version: Version | None = None,
    ) -> None:
        """Stage a write. Nothing is published until :meth:`commit`."""

        tx = self._require_active()
        name = self._resolve(resource)
        normalized = normalize_value(value, path=f"write[{name}.{key}]")
        base = await self._base_version(tx, name, key, expected_version)
        tx.stage_write(name, key, normalized, base)

    async def delete(
        self,
        key: str,
        *,
        resource: SharedResource | str | None = None,
        expected_version: Version | None = None,
    ) -> None:
        """Stage a delete. Absent keys are a no-op only if the base is 0."""

        tx = self._require_active()
        name = self._resolve(resource)
        base = await self._base_version(tx, name, key, expected_version)
        tx.stage_write(name, key, None, base, op="delete")

    async def _base_version(
        self,
        tx: Transaction,
        name: str,
        key: str,
        expected_version: Version | None,
    ) -> Version:
        if expected_version is not None and expected_version < 0:
            raise ValueError("expected_version must be non-negative")
        if self._isolation == "pessimistic":
            backend = self._get_manager().backend
            await backend.acquire_lock(
                tx.id,
                name,
                key,
                mode="exclusive",
                timeout=self._lock_timeout or backend_default_timeout(backend),
                agent_id=self._agent_id,
            )
            if expected_version is not None:
                return expected_version
            try:
                stored = await backend.read_latest(name, key)
            except KeyNotFoundError:
                tx.record_read(name, key, UNVERSIONED)
                return UNVERSIONED
            tx.record_read(name, key, stored.version)
            return stored.version
        if expected_version is not None:
            return expected_version
        if self._isolation == "none":
            return UNVERSIONED
        snapshot = tx.snapshots.get(name)
        if snapshot is None:  # pragma: no cover - guarded by isolation
            return UNVERSIONED
        return snapshot.version_of(key)

    # -- commit / abort ------------------------------------------------------

    async def commit(self) -> CommitResult:
        """Validate and atomically apply staged writes."""

        tx = self._require_active()
        manager = self._get_manager()
        if not tx.has_writes and not tx.read_set:
            # Nothing to validate or apply.
            result = await manager.backend.commit(tx.to_plan())
            tx.mark_committed(result)
            self._commit_result = result
            self._finish_booking(manager, tx, "committed", result.commit_version)
            return result

        tx.mark_committing()
        started = time.perf_counter()
        try:
            result = await manager.backend.commit(tx.to_plan())
        except ConflictError as exc:
            tx.mark_aborted(str(exc))
            self._commit_result = None
            self._release_snapshots()
            await self._release_locks_async()
            self._finish_booking(manager, tx, "conflict", None)
            manager.metrics.increment("agentcommit.tx.conflict")
            manager.event_bus.emit(
                Event(
                    type=EventType.CONFLICT,
                    transaction_id=tx.id,
                    agent_id=tx.agent_id,
                    resource=exc.resource,
                    key=exc.key,
                    message=str(exc),
                    data={"conflict_class": exc.conflict_class, "attempt": self._retry_attempt},
                )
            )
            raise
        except BaseException as exc:
            # Unknown outcome: the commit may or may not have reached the backend.
            # Retrying is only safe with an idempotency key.
            tx.mark_failed(str(exc))
            self._commit_result = None
            self._release_snapshots()
            await self._release_locks_async()
            manager.log.append(
                TransactionRecord.from_plan(
                    tx.to_plan(),
                    status=TransactionStatus.FAILED,
                    abort_reason=str(exc),
                    retry_attempt=self._retry_attempt,
                )
            )
            self._finish_booking(manager, tx, "failed", None)
            manager.metrics.increment("agentcommit.tx.failed")
            raise

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        tx.mark_committed(result)
        self._commit_result = result
        manager.metrics.observe("agentcommit.commit.latency_ms", elapsed_ms)
        self._release_snapshots()
        await self._release_locks_async()
        self._finish_booking(manager, tx, "committed", result.commit_version)
        return result

    async def abort(self, reason: str = "aborted by caller") -> None:
        """Discard staged writes and release locks/snapshots."""

        if self._tx is None:
            return
        tx = self._tx
        if tx.state is TransactionState.COMMITTED:
            raise TransactionStateError("cannot abort a committed transaction")
        if tx.state is TransactionState.ABORTED:
            return
        if tx.state is TransactionState.COMMITTING:
            # The commit call did not return; state is indeterminate, not a clean abort.
            tx.mark_failed(reason)
            status = TransactionStatus.FAILED
        else:
            tx.mark_aborted(reason)
            status = TransactionStatus.ABORTED
        self._release_snapshots()
        await self._release_locks_async()
        manager = self._get_manager()
        manager.log.append(
            TransactionRecord.from_plan(
                tx.to_plan(),
                status=status,
                abort_reason=reason,
                retry_attempt=self._retry_attempt,
            )
        )
        self._finish_booking(manager, tx, "aborted", None)
        manager.event_bus.emit(
            Event(
                type=EventType.SESSION_ABORTED,
                transaction_id=tx.id,
                agent_id=tx.agent_id,
                message=reason,
                data={"reason": reason},
            )
        )

    # -- cleanup helpers -----------------------------------------------------

    def _release_snapshots(self) -> None:
        if self._tx is None:
            return
        for snapshot in self._tx.snapshots.values():
            snapshot.release()
        self._tx.snapshots.clear()

    async def _release_locks_async(self) -> None:
        if self._tx is None:
            return
        if self._isolation == "pessimistic":
            await self._get_manager().backend.release_locks(self._tx.id)

    def _finish_booking(
        self, manager: TransactionManager, tx: Transaction, outcome: str, commit_version: int | None
    ) -> None:
        elapsed_ms = (
            (time.perf_counter() - self._started_perf) * 1000.0 if self._started_perf else 0.0
        )
        manager.metrics.observe("agentcommit.tx.latency_ms", elapsed_ms)
        if outcome == "committed":
            manager.metrics.increment("agentcommit.tx.committed")
        elif outcome == "conflict" or outcome == "aborted":
            manager.metrics.increment("agentcommit.tx.aborted")
        else:
            manager.metrics.increment("agentcommit.tx.failed")
        manager.event_bus.emit(
            Event(
                type=(
                    EventType.SESSION_COMMITTED
                    if outcome == "committed"
                    else EventType.SESSION_ABORTED
                ),
                transaction_id=tx.id,
                agent_id=tx.agent_id,
                message=outcome,
                data={
                    "commit_version": commit_version,
                    "duration_ms": elapsed_ms,
                    "attempt": self._retry_attempt,
                    "isolation": tx.isolation,
                },
            )
        )


def backend_default_timeout(backend: Any) -> float:
    locks = getattr(backend, "_locks", None)
    return float(getattr(locks, "default_timeout", 5.0))
