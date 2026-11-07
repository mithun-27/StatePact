"""Backend-facing contracts.

A storage backend is responsible for exactly one thing: atomically validating a
:class:`CommitPlan` and applying it, or rejecting it without side effects. Sessions
never apply state themselves.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from agentcommit.core.version import Version

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

IsolationMode = Literal["none", "optimistic", "snapshot", "serializable", "pessimistic"]

ISOLATION_MODES: frozenset[str] = frozenset(
    {"none", "optimistic", "snapshot", "serializable", "pessimistic"}
)

WriteOp = Literal["put", "delete"]
LockMode = Literal["shared", "exclusive"]

#: Version used for a key that was observed to be absent.
ABSENT: Version = 0


@dataclass(frozen=True, slots=True)
class StoredValue:
    """A value as committed at ``version``."""

    value: Any
    version: Version


class ResourceSnapshot:
    """A consistent view of one resource at a global commit sequence.

    Snapshots returned by :meth:`StorageBackend.snapshot` may hold on to version
    history (MVCC). Call :meth:`release` when the snapshot is no longer needed so
    the backend can garbage-collect. Sessions do this automatically.
    """

    __slots__ = ("_release_fn", "_released", "fence", "resource", "taken_at", "values")

    def __init__(
        self,
        resource: str,
        taken_at: Version,
        values: Mapping[str, StoredValue],
        *,
        fence: int = 0,
        release_fn: Callable[[], None] | None = None,
    ) -> None:
        self.resource = resource
        self.taken_at = taken_at
        self.values = dict(values)
        self.fence = fence
        self._release_fn = release_fn
        self._released = False

    def get(self, key: str) -> StoredValue | None:
        return self.values.get(key)

    def version_of(self, key: str) -> Version:
        stored = self.values.get(key)
        return ABSENT if stored is None else stored.version

    def keys(self) -> Sequence[str]:
        return tuple(self.values)

    def items(self) -> Sequence[tuple[str, StoredValue]]:
        return tuple(self.values.items())

    def __contains__(self, key: object) -> bool:
        return key in self.values

    def __len__(self) -> int:
        return len(self.values)

    def __repr__(self) -> str:
        return (
            f"ResourceSnapshot(resource={self.resource!r}, taken_at={self.taken_at}, "
            f"keys={len(self.values)})"
        )

    def release(self) -> None:
        if not self._released:
            self._released = True
            if self._release_fn is not None:
                self._release_fn()


@dataclass(frozen=True, slots=True)
class ReadRecord:
    """A key observed by a transaction, with the version that was observed."""

    resource: str
    key: str
    version: Version


@dataclass(frozen=True, slots=True)
class WriteRecord:
    """A staged write. ``expected_version`` is the base the write was planned on."""

    resource: str
    key: str
    value: Any
    expected_version: Version
    op: WriteOp = "put"

    @property
    def is_delete(self) -> bool:
        return self.op == "delete"


@dataclass(frozen=True, slots=True)
class CommitPlan:
    """Everything a backend needs to validate and apply a transaction atomically."""

    transaction_id: str
    agent_id: str
    isolation: IsolationMode
    reads: tuple[ReadRecord, ...]
    writes: tuple[WriteRecord, ...]
    started_at: datetime
    fence: int | None = None
    idempotency_key: str | None = None
    request_hash: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def read_only(self) -> bool:
        return not self.writes

    @property
    def resources(self) -> tuple[str, ...]:
        names = {record.resource for record in self.reads}
        names.update(record.resource for record in self.writes)
        return tuple(sorted(names))

    def describe(self) -> str:
        return (
            f"{self.transaction_id} agent={self.agent_id} isolation={self.isolation} "
            f"reads={len(self.reads)} writes={len(self.writes)}"
        )


@dataclass(frozen=True, slots=True)
class CommitResult:
    """Return value of ``session.commit()`` and ``backend.commit()``."""

    transaction_id: str
    committed: bool
    committed_at: datetime
    commit_version: Version | None
    read_set: tuple[ReadRecord, ...] = ()
    write_set: tuple[WriteRecord, ...] = ()
    replayed: bool = False

    @property
    def read_only(self) -> bool:
        return not self.write_set


@dataclass(frozen=True, slots=True)
class Lease:
    """A time-bounded ownership token with a monotonically increasing fence."""

    resource: str
    owner: str
    token: int
    expires_at: datetime
    ttl_seconds: float

    def is_expired(self, now: datetime) -> bool:
        return now >= self.expires_at


@dataclass(frozen=True, slots=True)
class ResourceInfo:
    name: str
    keys: int
    version: Version
    metadata: Mapping[str, Any] = field(default_factory=dict)


class StorageBackend(ABC):
    """Storage abstraction shared by the in-memory, PostgreSQL and Redis backends.

    All methods must be safe to call concurrently from asyncio tasks in one
    process. Cross-process safety is backend-specific and documented per backend.
    """

    #: Short backend identifier used in logs and benchmarks.
    name: str = "base"

    supports_mvcc: bool = False
    supports_locking: bool = False
    supports_leases: bool = False
    supports_idempotency: bool = False
    durable: bool = False

    @abstractmethod
    async def ensure_resource(
        self,
        name: str,
        initial_state: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Create the resource if absent. Never overwrites an existing resource."""

    @abstractmethod
    async def snapshot(self, resource: str) -> ResourceSnapshot:
        """Return a consistent snapshot of the resource."""

    @abstractmethod
    async def read_latest(self, resource: str, key: str) -> StoredValue:
        """Return the newest committed value or raise :class:`KeyNotFoundError`."""

    @abstractmethod
    async def versions(self, resource: str) -> Mapping[str, Version]:
        """Return the latest committed version of every existing key."""

    @abstractmethod
    async def commit(self, plan: CommitPlan) -> CommitResult:
        """Validate and apply ``plan`` atomically, or raise :class:`ConflictError`."""

    @abstractmethod
    async def list_resources(self) -> Sequence[str]:
        """Return every registered resource name."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Cheap liveness probe used by the demo and Docker health checks."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections and resources. Idempotent."""

    # -- optional capabilities -------------------------------------------------

    async def history(self, resource: str, key: str, *, limit: int = 50) -> list[StoredValue]:
        """Return committed history newest-first. Only MVCC backends implement this."""
        raise NotImplementedError(f"{self.name} does not expose version history")

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
        raise NotImplementedError(f"{self.name} does not support pessimistic locking")

    async def release_locks(self, transaction_id: str) -> None:
        raise NotImplementedError(f"{self.name} does not support pessimistic locking")

    async def acquire_lease(self, resource: str, owner: str, ttl_seconds: float) -> Lease:
        raise NotImplementedError(f"{self.name} does not support leases")

    async def renew_lease(self, lease: Lease, ttl_seconds: float | None = None) -> Lease:
        raise NotImplementedError(f"{self.name} does not support leases")

    async def release_lease(self, lease: Lease) -> bool:
        raise NotImplementedError(f"{self.name} does not support leases")

    async def current_fence(self, resource: str) -> int:
        raise NotImplementedError(f"{self.name} does not support leases")

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r}>"
