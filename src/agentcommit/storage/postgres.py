"""PostgreSQL storage backend.

Design
------
* Commit validation and application run inside one database transaction guarded
  by a PostgreSQL advisory transaction lock. That makes commits linearizable
  across every process sharing the schema.
* Versions come from a single row in ``agentcommit_meta``; read-only commits do
  not advance it.
* MVCC history is kept in ``agentcommit_history`` and exposed through
  :meth:`PostgresBackend.history`.
* Idempotency keys are stored durably with the request fingerprint.
* Pessimistic locks live in ``agentcommit_locks`` and are polled with a timeout.
  Deadlocks cannot form in the database because acquisition checks are separate
  short transactions; a lock cycle therefore surfaces as a timeout.
* Leases and fencing tokens are durable. The token is a high-water mark and is
  never reused, even after the lease is released or expires.

Schema is created automatically by :meth:`PostgresBackend.connect` (or lazily on
first use when the backend is constructed with ``initialize=True``).

Requires ``pip install agentcommit[postgres]``.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Self

from agentcommit.concurrency import occ
from agentcommit.concurrency.serializability import (
    SerializationGraphAudit,
    TransactionFootprint,
)
from agentcommit.core.exceptions import (
    ConflictError,
    IdempotencyMismatchError,
    KeyNotFoundError,
    LeaseLostError,
    LeaseNotAvailableError,
    LockTimeoutError,
    ResourceNotFoundError,
    SerializationError,
    StorageError,
)
from agentcommit.core.interfaces import (
    CommitPlan,
    CommitResult,
    Lease,
    LockMode,
    ResourceInfo,
    ResourceSnapshot,
    StorageBackend,
    StoredValue,
)
from agentcommit.core.serde import normalize_value
from agentcommit.core.version import Version
from agentcommit.observability.events import Event, EventBus, EventType
from agentcommit.observability.transaction_logs import (
    TransactionLog,
    TransactionRecord,
    TransactionStatus,
)

try:  # pragma: no cover - import guard is exercised by the extras test
    import asyncpg  # type: ignore[import-untyped]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "PostgresBackend requires asyncpg; install with `pip install agentcommit[postgres]`"
    ) from exc


_SCHEMA = """
CREATE TABLE IF NOT EXISTS agentcommit_meta (
    id smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    seq bigint NOT NULL DEFAULT 0
);
INSERT INTO agentcommit_meta (id, seq) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS agentcommit_resources (
    name text PRIMARY KEY,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_commit_at timestamptz
);

CREATE TABLE IF NOT EXISTS agentcommit_values (
    resource text NOT NULL REFERENCES agentcommit_resources(name) ON DELETE CASCADE,
    key text NOT NULL,
    value jsonb,
    version bigint NOT NULL,
    deleted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (resource, key)
);

CREATE TABLE IF NOT EXISTS agentcommit_history (
    resource text NOT NULL,
    key text NOT NULL,
    version bigint NOT NULL,
    value jsonb,
    deleted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (resource, key, version)
);

CREATE TABLE IF NOT EXISTS agentcommit_idempotency (
    idempotency_key text PRIMARY KEY,
    request_hash text NOT NULL,
    transaction_id text NOT NULL,
    commit_version bigint,
    committed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agentcommit_locks (
    resource text NOT NULL,
    key text NOT NULL,
    transaction_id text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('shared', 'exclusive')),
    agent_id text,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    PRIMARY KEY (resource, key, transaction_id)
);

CREATE TABLE IF NOT EXISTS agentcommit_leases (
    resource text PRIMARY KEY,
    owner text NOT NULL,
    token bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    ttl_seconds double precision NOT NULL
);
"""

#: Advisory lock key used to serialise commit validation/apply.
_COMMIT_LOCK = 0x4147434F4D4D4954  # "AGCOMMIT" truncated into int64


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class PostgresBackend(StorageBackend):
    """Durable backend for multi-process agent fleets."""

    name = "postgres"
    supports_mvcc = True
    supports_locking = True
    supports_leases = True
    supports_idempotency = True
    durable = True

    def __init__(
        self,
        pool: Any,
        *,
        event_bus: EventBus | None = None,
        transaction_log: TransactionLog | None = None,
        lock_timeout: float = 5.0,
        serialization_window: int = 512,
        idempotency_retention_days: float | None = 30.0,
    ) -> None:
        self._pool: Any = pool
        self._events = event_bus if event_bus is not None else EventBus()
        self._log = transaction_log if transaction_log is not None else TransactionLog()
        self._lock_timeout = lock_timeout
        self._idempotency_retention_days = idempotency_retention_days
        self._audit = SerializationGraphAudit(window=serialization_window)
        self._closed = False

    # -- construction --------------------------------------------------------

    @classmethod
    async def connect(
        cls,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        event_bus: EventBus | None = None,
        transaction_log: TransactionLog | None = None,
        lock_timeout: float = 5.0,
        serialization_window: int = 512,
        idempotency_retention_days: float | None = 30.0,
        schema: str | None = None,
    ) -> Self:
        """Open a pool and create the AgentCommit schema if needed."""

        try:
            pool = await asyncpg.create_pool(
                dsn,
                min_size=min_size,
                max_size=max_size,
                server_settings={"application_name": "agentcommit"},
            )
        except Exception as exc:  # pragma: no cover - requires a live server
            raise StorageError(f"could not connect to PostgreSQL: {exc}") from exc
        assert pool is not None
        backend = cls(
            pool,
            event_bus=event_bus,
            transaction_log=transaction_log,
            lock_timeout=lock_timeout,
            serialization_window=serialization_window,
            idempotency_retention_days=idempotency_retention_days,
        )
        try:
            if schema is not None:
                await pool.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                await pool.execute(f'SET search_path TO "{schema}", public')
            await pool.execute(_SCHEMA)
        except Exception as exc:  # pragma: no cover - requires a live server
            await pool.close()
            raise StorageError(f"could not initialise AgentCommit schema: {exc}") from exc
        return backend

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

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
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", _COMMIT_LOCK)
            inserted = await conn.fetchval(
                """
                INSERT INTO agentcommit_resources (name, metadata)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (name) DO NOTHING
                RETURNING name
                """,
                name,
                _json(dict(metadata or {})),
            )
            if inserted is None:
                if metadata:
                    await conn.execute(
                        """
                        UPDATE agentcommit_resources
                        SET metadata = metadata || $2::jsonb
                        WHERE name = $1
                        """,
                        name,
                        _json(dict(metadata)),
                    )
                return
            if not normalized:
                return
            seq = await conn.fetchval(
                "UPDATE agentcommit_meta SET seq = seq + 1 WHERE id = 1 RETURNING seq"
            )
            for key, value in normalized.items():
                payload = _json(value)
                await conn.execute(
                    """
                    INSERT INTO agentcommit_values (resource, key, value, version)
                    VALUES ($1, $2, $3::jsonb, $4)
                    """,
                    name,
                    key,
                    payload,
                    seq,
                )
                await conn.execute(
                    """
                    INSERT INTO agentcommit_history (resource, key, version, value)
                    VALUES ($1, $2, $3, $4::jsonb)
                    """,
                    name,
                    key,
                    seq,
                    payload,
                )
            await conn.execute(
                "UPDATE agentcommit_resources SET version = $2, last_commit_at = now() WHERE name = $1",
                name,
                seq,
            )

    async def list_resources(self) -> Sequence[str]:
        self._ensure_open()
        rows = await self._pool.fetch("SELECT name FROM agentcommit_resources ORDER BY name")
        return tuple(row["name"] for row in rows)

    async def resource_info(self, resource: str) -> ResourceInfo:
        self._ensure_open()
        row = await self._pool.fetchrow(
            "SELECT metadata, version FROM agentcommit_resources WHERE name = $1", resource
        )
        if row is None:
            raise ResourceNotFoundError(
                f"resource {resource!r} is not registered", resource=resource
            )
        keys = await self._pool.fetchval(
            "SELECT count(*) FROM agentcommit_values WHERE resource = $1 AND NOT deleted", resource
        )
        return ResourceInfo(
            name=resource,
            keys=int(keys or 0),
            version=int(row["version"]),
            metadata=_loads_mapping(row["metadata"]),
        )

    async def snapshot(self, resource: str) -> ResourceSnapshot:
        self._ensure_open()
        await self._require(resource)
        rows, seq = await asyncio.gather(
            self._pool.fetch(
                "SELECT key, value, version FROM agentcommit_values "
                "WHERE resource = $1 AND NOT deleted",
                resource,
            ),
            self._current_seq(),
        )
        values = {
            row["key"]: StoredValue(value=_loads(row["value"]), version=int(row["version"]))
            for row in rows
        }
        return ResourceSnapshot(
            resource=resource,
            taken_at=seq,
            values=values,
            fence=await self.current_fence(resource),
        )

    async def read_latest(self, resource: str, key: str) -> StoredValue:
        self._ensure_open()
        row = await self._pool.fetchrow(
            "SELECT value, version, deleted FROM agentcommit_values WHERE resource = $1 AND key = $2",
            resource,
            key,
        )
        if row is None or row["deleted"]:
            raise KeyNotFoundError(resource, key)
        return StoredValue(value=_loads(row["value"]), version=int(row["version"]))

    async def versions(self, resource: str) -> Mapping[str, Version]:
        self._ensure_open()
        rows = await self._pool.fetch(
            "SELECT key, version FROM agentcommit_values WHERE resource = $1 AND NOT deleted",
            resource,
        )
        return {row["key"]: int(row["version"]) for row in rows}

    async def history(self, resource: str, key: str, *, limit: int = 50) -> list[StoredValue]:
        self._ensure_open()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = await self._pool.fetch(
            """
            SELECT value, version FROM agentcommit_history
            WHERE resource = $1 AND key = $2 AND NOT deleted
            ORDER BY version DESC LIMIT $3
            """,
            resource,
            key,
            limit,
        )
        return [
            StoredValue(value=_loads(row["value"]), version=int(row["version"])) for row in rows
        ]

    # -- commit --------------------------------------------------------------

    async def commit(self, plan: CommitPlan) -> CommitResult:
        self._ensure_open()
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock($1)", _COMMIT_LOCK)
                for resource in plan.resources:
                    exists = await conn.fetchval(
                        "SELECT 1 FROM agentcommit_resources WHERE name = $1", resource
                    )
                    if exists is None:
                        raise ResourceNotFoundError(
                            f"resource {resource!r} is not registered", resource=resource
                        )
                replay = await self._check_idempotency(conn, plan)
                if replay is not None:
                    return replay
                return await self._commit_locked(conn, plan)
        except ConflictError as exc:
            self._record_conflict(plan, exc)
            raise
        except (ResourceNotFoundError, IdempotencyMismatchError):
            raise
        except Exception as exc:  # pragma: no cover - requires a live server
            raise StorageError(f"postgres commit failed: {exc}") from exc

    async def _commit_locked(self, conn: Any, plan: CommitPlan) -> CommitResult:
        started = _utcnow()
        if plan.read_only:
            seq = await self._current_seq_conn(conn)
            result = CommitResult(
                transaction_id=plan.transaction_id,
                committed=True,
                committed_at=started,
                commit_version=seq,
                read_set=plan.reads,
                write_set=plan.writes,
            )
            self._log.append(
                TransactionRecord.from_plan(
                    plan,
                    status=TransactionStatus.READ_ONLY,
                    commit_version=seq,
                    finished_at=started,
                )
            )
            await self._store_idempotency(conn, plan, result)
            return result

        for resource in {write.resource for write in plan.writes}:
            token = await conn.fetchval(
                "SELECT token FROM agentcommit_leases WHERE resource = $1", resource
            )
            current = int(token or 0)
            if plan.fence is not None and plan.fence < current:
                raise LeaseLostError(
                    resource=resource,
                    token=plan.fence,
                    current_token=current,
                    message=f"fencing token {plan.fence} is older than highest token {current}",
                )

        latest: dict[tuple[str, str], Version] = {}
        for record in plan.reads:
            version = await conn.fetchval(
                "SELECT version FROM agentcommit_values WHERE resource = $1 AND key = $2",
                record.resource,
                record.key,
            )
            latest[(record.resource, record.key)] = int(version or 0)
        for write in plan.writes:
            version = await conn.fetchval(
                "SELECT version FROM agentcommit_values WHERE resource = $1 AND key = $2",
                write.resource,
                write.key,
            )
            latest[(write.resource, write.key)] = int(version or 0)

        outcome = occ.validate(plan, latest)
        if outcome.conflict is not None:
            raise outcome.conflict.to_exception()

        next_seq = int(
            await conn.fetchval(
                "UPDATE agentcommit_meta SET seq = seq + 1 WHERE id = 1 RETURNING seq"
            )
        )
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

        for write in plan.writes:
            payload = None if write.is_delete else _json(write.value)
            await conn.execute(
                """
                INSERT INTO agentcommit_values (resource, key, value, version, deleted)
                VALUES ($1, $2, $3::jsonb, $4, $5)
                ON CONFLICT (resource, key) DO UPDATE
                SET value = EXCLUDED.value,
                    version = EXCLUDED.version,
                    deleted = EXCLUDED.deleted
                """,
                write.resource,
                write.key,
                payload,
                next_seq,
                write.is_delete,
            )
            await conn.execute(
                """
                INSERT INTO agentcommit_history (resource, key, version, value, deleted)
                VALUES ($1, $2, $3, $4::jsonb, $5)
                """,
                write.resource,
                write.key,
                next_seq,
                payload,
                write.is_delete,
            )
            await conn.execute(
                """
                UPDATE agentcommit_resources
                SET version = $2, last_commit_at = now(),
                    metadata = metadata || $3::jsonb
                WHERE name = $1
                """,
                write.resource,
                next_seq,
                _json({"last_commit_at": started.isoformat()}),
            )

        finished = _utcnow()
        result = CommitResult(
            transaction_id=plan.transaction_id,
            committed=True,
            committed_at=finished,
            commit_version=next_seq,
            read_set=plan.reads,
            write_set=plan.writes,
        )
        await self._store_idempotency(conn, plan, result)
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
                    "backend": self.name,
                },
            )
        )
        return result

    async def _check_idempotency(self, conn: Any, plan: CommitPlan) -> CommitResult | None:
        if plan.idempotency_key is None:
            return None
        row = await conn.fetchrow(
            """
            SELECT request_hash, commit_version, committed_at
            FROM agentcommit_idempotency WHERE idempotency_key = $1
            FOR UPDATE
            """,
            plan.idempotency_key,
        )
        if row is None:
            return None
        if row["request_hash"] != plan.request_hash:
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
        return CommitResult(
            transaction_id=plan.transaction_id,
            committed=True,
            committed_at=row["committed_at"],
            commit_version=(None if row["commit_version"] is None else int(row["commit_version"])),
            read_set=plan.reads,
            write_set=plan.writes,
            replayed=True,
        )

    async def _store_idempotency(self, conn: Any, plan: CommitPlan, result: CommitResult) -> None:
        if plan.idempotency_key is None:
            return
        await conn.execute(
            """
            INSERT INTO agentcommit_idempotency
                (idempotency_key, request_hash, transaction_id, commit_version, committed_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            plan.idempotency_key,
            plan.request_hash or "",
            plan.transaction_id,
            result.commit_version,
            result.committed_at,
        )

    def _record_conflict(self, plan: CommitPlan, error: ConflictError) -> None:
        self._log.append(
            TransactionRecord.from_plan(
                plan,
                status=TransactionStatus.ABORTED,
                finished_at=_utcnow(),
                conflict_class=error.conflict_class,
                abort_reason=str(error),
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
        await self._require(resource)
        deadline = time.monotonic() + timeout
        while True:
            acquired = await self._try_lock(transaction_id, resource, key, mode, agent_id)
            if acquired:
                return
            if time.monotonic() >= deadline:
                raise LockTimeoutError(
                    resource=resource,
                    key=key,
                    transaction_id=transaction_id,
                    message=f"timed out after {timeout:.2f}s waiting for a {mode} lock",
                )
            await asyncio.sleep(0.05)

    async def _try_lock(
        self,
        transaction_id: str,
        resource: str,
        key: str,
        mode: LockMode,
        agent_id: str | None,
    ) -> bool:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"{resource}:{key}")
            await conn.execute(
                "DELETE FROM agentcommit_locks WHERE resource = $1 AND key = $2 "
                "AND expires_at IS NOT NULL AND expires_at <= now()",
                resource,
                key,
            )
            holders = await conn.fetch(
                "SELECT transaction_id, mode FROM agentcommit_locks "
                "WHERE resource = $1 AND key = $2 AND transaction_id <> $3",
                resource,
                key,
                transaction_id,
            )
            if mode == "exclusive":
                blocked = bool(holders)
            else:
                blocked = any(row["mode"] == "exclusive" for row in holders)
            if blocked:
                return False
            await conn.execute(
                """
                INSERT INTO agentcommit_locks
                    (resource, key, transaction_id, mode, agent_id, expires_at)
                VALUES ($1, $2, $3, $4, $5, now() + interval '5 minutes')
                ON CONFLICT (resource, key, transaction_id) DO UPDATE
                SET mode = EXCLUDED.mode, expires_at = EXCLUDED.expires_at
                """,
                resource,
                key,
                transaction_id,
                mode,
                agent_id,
            )
            return True

    async def release_locks(self, transaction_id: str) -> None:
        await self._pool.execute(
            "DELETE FROM agentcommit_locks WHERE transaction_id = $1", transaction_id
        )

    # -- leases --------------------------------------------------------------

    async def acquire_lease(self, resource: str, owner: str, ttl_seconds: float) -> Lease:
        self._ensure_open()
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        await self._require(resource)
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"lease:{resource}")
            row = await conn.fetchrow(
                "SELECT owner, token, expires_at, ttl_seconds FROM agentcommit_leases "
                "WHERE resource = $1 FOR UPDATE",
                resource,
            )
            now = _utcnow()
            if row is not None and row["expires_at"] > now:
                if row["owner"] != owner:
                    raise LeaseNotAvailableError(resource, owner, str(row["owner"]))
                token = int(row["token"])
            else:
                token = int(row["token"]) + 1 if row is not None else 1
            expires_at = await conn.fetchval(
                """
                INSERT INTO agentcommit_leases (resource, owner, token, expires_at, ttl_seconds)
                VALUES ($1, $2, $3, now() + ($4 * interval '1 second'), $4)
                ON CONFLICT (resource) DO UPDATE
                SET owner = EXCLUDED.owner,
                    token = EXCLUDED.token,
                    expires_at = EXCLUDED.expires_at,
                    ttl_seconds = EXCLUDED.ttl_seconds
                RETURNING expires_at
                """,
                resource,
                owner,
                token,
                ttl_seconds,
            )
        self._events.emit(
            Event(
                type=EventType.LEASE_ACQUIRED,
                agent_id=owner,
                resource=resource,
                data={"token": token, "ttl_seconds": ttl_seconds},
            )
        )
        return Lease(resource, owner, token, expires_at, ttl_seconds)

    async def renew_lease(self, lease: Lease, ttl_seconds: float | None = None) -> Lease:
        self._ensure_open()
        ttl = ttl_seconds if ttl_seconds is not None else lease.ttl_seconds
        if ttl <= 0:
            raise ValueError("ttl_seconds must be positive")
        row = await self._pool.fetchrow(
            """
            UPDATE agentcommit_leases
            SET expires_at = now() + ($4 * interval '1 second'), ttl_seconds = $4
            WHERE resource = $1 AND owner = $2 AND token = $3 AND expires_at > now()
            RETURNING token, expires_at
            """,
            lease.resource,
            lease.owner,
            lease.token,
            ttl,
        )
        if row is None:
            raise LeaseLostError(
                resource=lease.resource,
                token=lease.token,
                message=f"cannot renew expired or superseded lease {lease.token}",
            )
        self._events.emit(
            Event(
                type=EventType.LEASE_RENEWED,
                agent_id=lease.owner,
                resource=lease.resource,
                data={"token": lease.token, "ttl_seconds": ttl},
            )
        )
        return Lease(
            lease.resource,
            lease.owner,
            lease.token,
            row["expires_at"],
            ttl,
        )

    async def release_lease(self, lease: Lease) -> bool:
        self._ensure_open()
        row = await self._pool.fetchrow(
            """
            UPDATE agentcommit_leases
            SET expires_at = to_timestamp(0)
            WHERE resource = $1 AND owner = $2 AND token = $3
            RETURNING token
            """,
            lease.resource,
            lease.owner,
            lease.token,
        )
        if row is None:
            return False
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
        token = await self._pool.fetchval(
            "SELECT token FROM agentcommit_leases WHERE resource = $1", resource
        )
        return int(token or 0)

    # -- observability / lifecycle ------------------------------------------

    @property
    def log(self) -> TransactionLog:
        return self._log

    @property
    def event_bus(self) -> EventBus:
        return self._events

    async def sequence(self) -> Version:
        return await self._current_seq()

    async def health_check(self) -> bool:
        if self._closed:
            return False
        try:
            return bool(await self._pool.fetchval("SELECT 1") == 1)
        except Exception:  # pragma: no cover - server down
            return False

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._pool.close()

    async def clear(self) -> None:
        """Delete all AgentCommit rows. Test and playground helper."""

        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "TRUNCATE agentcommit_history, agentcommit_values, agentcommit_idempotency, "
                "agentcommit_locks, agentcommit_leases, agentcommit_resources"
            )
            await conn.execute("UPDATE agentcommit_meta SET seq = 0 WHERE id = 1")
        self._log.clear()

    # -- internals -----------------------------------------------------------

    async def _current_seq(self) -> Version:
        return await self._current_seq_conn(self._pool)

    async def _current_seq_conn(self, conn: Any) -> Version:
        seq = await conn.fetchval("SELECT seq FROM agentcommit_meta WHERE id = 1")
        return int(seq or 0)

    async def _require(self, resource: str) -> None:
        exists = await self._pool.fetchval(
            "SELECT 1 FROM agentcommit_resources WHERE name = $1", resource
        )
        if exists is None:
            raise ResourceNotFoundError(
                f"resource {resource!r} is not registered", resource=resource
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise StorageError("postgres backend is closed")


def _loads(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _loads_mapping(value: Any) -> Mapping[str, Any]:
    loaded = _loads(value)
    return loaded if isinstance(loaded, Mapping) else {}


def _seconds(ttl_seconds: float) -> Any:
    from datetime import timedelta

    return timedelta(seconds=ttl_seconds)


def _validate_resource_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("resource name must be a non-empty string")
    if "\x00" in name:
        raise ValueError("resource name must not contain NUL characters")
