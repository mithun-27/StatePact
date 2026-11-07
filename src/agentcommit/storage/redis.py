"""Redis storage backend.

Design
------
* A single Lua script validates fencing tokens, read/write versions, idempotency
  and applies every write atomically. Redis executes scripts atomically, so the
  commit point is well defined across processes.
* State layout (all keys share a configurable prefix):

  ``{p}:seq``                 global commit version
  ``{p}:res``                 set of registered resource names
  ``{p}:meta:{resource}``     hash: ``version``, ``last_commit_at``, ``metadata``
  ``{p}:data:{resource}``     hash: key -> ``{"v": value, "ver": n, "d": bool}``
  ``{p}:hist:{resource}:{key}`` list of the same entries, newest first
  ``{p}:idem:{key}``          idempotency fingerprint + commit version
  ``{p}:fence:{resource}``    highest fencing token ever issued
  ``{p}:lease:{resource}``    hash: owner, token, expires_at, ttl
  ``{p}:lock:{resource}:{key}`` set of ``transaction_id|mode`` lock holders

* Pessimistic locks are lease-like sets with a TTL so a crashed process cannot
  block a key forever. Acquisition is a Lua compare-and-set; waiters poll and
  surface a :class:`LockTimeoutError`.
* ``serializable`` uses the same full read validation as ``optimistic``. The
  in-memory serialization-graph audit is process-local and is not run here;
  full backward validation is what provides serializability for tracked keys.

Requires ``pip install agentcommit[redis]``.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Self

from agentcommit.concurrency.serializability import SerializationGraphAudit
from agentcommit.core.exceptions import (
    ConflictError,
    IdempotencyMismatchError,
    KeyNotFoundError,
    LeaseLostError,
    LeaseNotAvailableError,
    LockTimeoutError,
    ReadWriteConflict,
    ResourceNotFoundError,
    StorageError,
    WriteWriteConflict,
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
    from redis.asyncio import Redis
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "RedisBackend requires redis; install with `pip install agentcommit[redis]`"
    ) from exc


_COMMIT_SCRIPT = """
local prefix = ARGV[3]
local plan = cjson.decode(ARGV[1])
local now = ARGV[2]
local history_limit = tonumber(ARGV[4])
local idem_ttl = tonumber(ARGV[5])
local seq_key = prefix .. ':seq'
local idem_key = nil

local function current_version(resource, key)
  local raw = redis.call('HGET', prefix .. ':data:' .. resource, key)
  if not raw then return 0 end
  local entry = cjson.decode(raw)
  return entry.ver or 0
end

local function store_idempotency(version)
  if idem_key then
    redis.call('SET', idem_key,
      cjson.encode({hash = plan.request_hash, version = version, at = now}),
      'EX', idem_ttl)
  end
end

if plan.idempotency_key ~= nil and plan.idempotency_key ~= cjson.null then
  idem_key = prefix .. ':idem:' .. plan.idempotency_key
  local raw = redis.call('GET', idem_key)
  if raw then
    local stored = cjson.decode(raw)
    if stored.hash ~= plan.request_hash then
      return cjson.encode({error = 'idempotency_mismatch'})
    end
    return cjson.encode({ok = true, replay = true, version = stored.version, at = stored.at})
  end
end

if #plan.writes == 0 then
  local seq = tonumber(redis.call('GET', seq_key) or '0')
  store_idempotency(seq)
  return cjson.encode({ok = true, read_only = true, version = seq})
end

if plan.fence ~= nil and plan.fence ~= cjson.null then
  for _, w in ipairs(plan.writes) do
    local fence = tonumber(redis.call('GET', prefix .. ':fence:' .. w.resource) or '0')
    if plan.fence < fence then
      return cjson.encode({error = 'lease_lost', resource = w.resource,
                           token = plan.fence, current = fence})
    end
  end
end

if plan.isolation ~= 'none' then
  if plan.isolation ~= 'snapshot' then
    for _, r in ipairs(plan.reads) do
      local actual = current_version(r.resource, r.key)
      if actual ~= r.version then
        return cjson.encode({error = 'read_write', resource = r.resource, key = r.key,
                             expected = r.version, actual = actual})
      end
    end
  end
  for _, w in ipairs(plan.writes) do
    local actual = current_version(w.resource, w.key)
    if actual ~= w.expected_version then
      return cjson.encode({error = 'write_write', resource = w.resource, key = w.key,
                           expected = w.expected_version, actual = actual})
    end
  end
end

local seq = redis.call('INCR', seq_key)
for _, w in ipairs(plan.writes) do
  local entry
  if w.op == 'delete' then
    entry = cjson.encode({ver = seq, d = true})
  else
    entry = cjson.encode({v = w.value, ver = seq, d = false})
  end
  redis.call('HSET', prefix .. ':data:' .. w.resource, w.key, entry)
  local history_key = prefix .. ':hist:' .. w.resource .. ':' .. w.key
  redis.call('LPUSH', history_key, entry)
  redis.call('LTRIM', history_key, 0, history_limit - 1)
  redis.call('HSET', prefix .. ':meta:' .. w.resource,
    'version', seq, 'last_commit_at', now)
end

store_idempotency(seq)
return cjson.encode({ok = true, version = seq})
"""

_ENSURE_SCRIPT = """
local resource = ARGV[1]
local meta_key = ARGV[2]
local data_key = ARGV[3]
local hist_prefix = ARGV[4]
local seq_key = ARGV[5]
local res_key = ARGV[6]
local metadata = ARGV[7]
local initial = cjson.decode(ARGV[8])
local now = ARGV[9]

if redis.call('EXISTS', meta_key) == 1 then
  if metadata ~= '' then
    local existing = cjson.decode(redis.call('HGET', meta_key, 'metadata') or '{}')
    for k, v in pairs(cjson.decode(metadata)) do existing[k] = v end
    redis.call('HSET', meta_key, 'metadata', cjson.encode(existing))
  end
  return 0
end

redis.call('SADD', res_key, resource)
redis.call('HSET', meta_key, 'metadata', metadata, 'version', 0, 'created_at', now)

local keys = {}
for key, _ in pairs(initial) do keys[key] = true end
local n = 0
for _ in pairs(keys) do n = n + 1 end
if n > 0 then
  local seq = redis.call('INCR', seq_key)
  for key, value in pairs(initial) do
    local entry = cjson.encode({v = value, ver = seq, d = false})
    redis.call('HSET', data_key, key, entry)
    redis.call('LPUSH', hist_prefix .. ':' .. key, entry)
  end
  redis.call('HSET', meta_key, 'version', seq)
end
return 1
"""

_ACQUIRE_LOCK_SCRIPT = """
local key = KEYS[1]
local txid = ARGV[1]
local mode = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local holders = redis.call('SMEMBERS', key)
for _, holder in ipairs(holders) do
  local sep = string.find(holder, '|', 1, true)
  local holder_tx = sep and string.sub(holder, 1, sep - 1) or holder
  local holder_mode = sep and string.sub(holder, sep + 1) or 'shared'
  if holder_tx ~= txid then
    if mode == 'exclusive' or holder_mode == 'exclusive' then
      return 0
    end
  end
end
for _, holder in ipairs(holders) do
  local sep = string.find(holder, '|', 1, true)
  local holder_tx = sep and string.sub(holder, 1, sep - 1) or holder
  if holder_tx == txid then
    redis.call('SREM', key, holder)
  end
end
redis.call('SADD', key, txid .. '|' .. mode)
redis.call('PEXPIRE', key, ttl_ms)
return 1
"""

_RELEASE_LOCK_SCRIPT = """
redis.call('SREM', KEYS[1], ARGV[1])
if redis.call('SCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
end
return 1
"""

_ACQUIRE_LEASE_SCRIPT = """
local now = tonumber(ARGV[1])
local owner = ARGV[2]
local ttl = tonumber(ARGV[3])
local data = redis.call('HMGET', KEYS[1], 'owner', 'token', 'expires_at')
local token
if data[1] and tonumber(data[3]) > now then
  if data[1] ~= owner then
    return cjson.encode({error = 'held', holder = data[1]})
  end
  token = tonumber(data[2])
else
  token = tonumber(redis.call('GET', KEYS[2]) or '0') + 1
  redis.call('SET', KEYS[2], token)
end
local expires_at = now + ttl
redis.call('HSET', KEYS[1], 'owner', owner, 'token', token,
  'expires_at', expires_at, 'ttl', ttl)
return cjson.encode({ok = true, token = token, expires_at = expires_at})
"""

_RENEW_LEASE_SCRIPT = """
local now = tonumber(ARGV[1])
local owner = ARGV[2]
local token = ARGV[3]
local ttl = tonumber(ARGV[4])
local data = redis.call('HMGET', KEYS[1], 'owner', 'token', 'expires_at')
if not data[1] or data[1] ~= owner or tonumber(data[2]) ~= token
   or tonumber(data[3]) <= now then
  return 0
end
local expires_at = now + ttl
redis.call('HSET', KEYS[1], 'expires_at', expires_at, 'ttl', ttl)
return expires_at
"""

_RELEASE_LEASE_SCRIPT = """
local data = redis.call('HMGET', KEYS[1], 'owner', 'token')
if data[1] == ARGV[1] and tonumber(data[2]) == tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], 'expires_at', 0)
  return 1
end
return 0
"""


def _utcnow() -> datetime:
    return datetime.now(UTC)


class RedisBackend(StorageBackend):
    """Atomic, multi-process backend built on Redis Lua scripts."""

    name = "redis"
    supports_mvcc = True
    supports_locking = True
    supports_leases = True
    supports_idempotency = True
    durable = True

    def __init__(
        self,
        client: Any,
        *,
        prefix: str = "agentcommit",
        event_bus: EventBus | None = None,
        transaction_log: TransactionLog | None = None,
        history_limit: int = 512,
        idempotency_ttl_seconds: int = 30 * 24 * 3600,
        lock_ttl_seconds: float = 60.0,
    ) -> None:
        self._redis: Any = client
        self._prefix = prefix
        self._events = event_bus if event_bus is not None else EventBus()
        self._log = transaction_log if transaction_log is not None else TransactionLog()
        self._history_limit = history_limit
        self._idempotency_ttl = idempotency_ttl_seconds
        self._lock_ttl = lock_ttl_seconds
        self._audit = SerializationGraphAudit(window=512)
        self._closed = False

    @classmethod
    async def connect(
        cls,
        url: str = "redis://localhost:6379/0",
        *,
        prefix: str = "agentcommit",
        event_bus: EventBus | None = None,
        transaction_log: TransactionLog | None = None,
        history_limit: int = 512,
        idempotency_ttl_seconds: int = 30 * 24 * 3600,
        lock_ttl_seconds: float = 60.0,
    ) -> Self:
        try:
            client = Redis.from_url(url, decode_responses=True)
            await client.ping()
        except Exception as exc:  # pragma: no cover - requires a live server
            raise StorageError(f"could not connect to Redis: {exc}") from exc
        return cls(
            client,
            prefix=prefix,
            event_bus=event_bus,
            transaction_log=transaction_log,
            history_limit=history_limit,
            idempotency_ttl_seconds=idempotency_ttl_seconds,
            lock_ttl_seconds=lock_ttl_seconds,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    # -- key helpers ---------------------------------------------------------

    def _k(self, *parts: str) -> str:
        return ":".join((self._prefix, *parts))

    def _meta(self, resource: str) -> str:
        return self._k("meta", resource)

    def _data(self, resource: str) -> str:
        return self._k("data", resource)

    def _hist(self, resource: str, key: str) -> str:
        return self._k("hist", resource, key)

    def _lease(self, resource: str) -> str:
        return self._k("lease", resource)

    def _fence(self, resource: str) -> str:
        return self._k("fence", resource)

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
        await self._eval(
            _ENSURE_SCRIPT,
            keys=[],
            args=[
                name,
                self._meta(name),
                self._data(name),
                self._k("hist", name),
                self._k("seq"),
                self._k("res"),
                json.dumps(dict(metadata or {})) if metadata else "",
                json.dumps(normalized),
                _utcnow().isoformat(),
            ],
        )

    async def list_resources(self) -> Sequence[str]:
        self._ensure_open()
        members = await self._redis.smembers(self._k("res"))
        return tuple(sorted(str(member) for member in members))

    async def resource_info(self, resource: str) -> ResourceInfo:
        self._ensure_open()
        meta = await self._redis.hgetall(self._meta(resource))
        if not meta:
            raise ResourceNotFoundError(
                f"resource {resource!r} is not registered", resource=resource
            )
        versions = await self.versions(resource)
        metadata = json.loads(meta.get("metadata") or "{}")
        return ResourceInfo(
            name=resource,
            keys=len(versions),
            version=int(meta.get("version") or 0),
            metadata=metadata if isinstance(metadata, Mapping) else {},
        )

    async def snapshot(self, resource: str) -> ResourceSnapshot:
        self._ensure_open()
        await self._require(resource)
        pipe = self._redis.pipeline(transaction=True)
        pipe.hgetall(self._data(resource))
        pipe.get(self._k("seq"))
        raw, seq = await pipe.execute()
        values: dict[str, StoredValue] = {}
        for key, blob in raw.items():
            entry = json.loads(blob)
            if not entry.get("d"):
                values[key] = StoredValue(value=entry.get("v"), version=int(entry["ver"]))
        return ResourceSnapshot(
            resource=resource,
            taken_at=int(seq or 0),
            values=values,
            fence=await self.current_fence(resource),
        )

    async def read_latest(self, resource: str, key: str) -> StoredValue:
        self._ensure_open()
        blob = await self._redis.hget(self._data(resource), key)
        if blob is None:
            raise KeyNotFoundError(resource, key)
        entry = json.loads(blob)
        if entry.get("d"):
            raise KeyNotFoundError(resource, key)
        return StoredValue(value=entry.get("v"), version=int(entry["ver"]))

    async def versions(self, resource: str) -> Mapping[str, Version]:
        self._ensure_open()
        raw = await self._redis.hgetall(self._data(resource))
        result: dict[str, Version] = {}
        for key, blob in raw.items():
            entry = json.loads(blob)
            if not entry.get("d"):
                result[key] = int(entry["ver"])
        return result

    async def history(self, resource: str, key: str, *, limit: int = 50) -> list[StoredValue]:
        self._ensure_open()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        raw = await self._redis.lrange(self._hist(resource, key), 0, limit - 1)
        result: list[StoredValue] = []
        for blob in raw:
            entry = json.loads(blob)
            if not entry.get("d"):
                result.append(StoredValue(value=entry.get("v"), version=int(entry["ver"])))
        return result

    # -- commit --------------------------------------------------------------

    async def commit(self, plan: CommitPlan) -> CommitResult:
        self._ensure_open()
        payload = {
            "transaction_id": plan.transaction_id,
            "agent_id": plan.agent_id,
            "isolation": plan.isolation,
            "reads": [
                {"resource": read.resource, "key": read.key, "version": read.version}
                for read in plan.reads
            ],
            "writes": [
                {
                    "resource": write.resource,
                    "key": write.key,
                    "value": write.value,
                    "expected_version": write.expected_version,
                    "op": write.op,
                }
                for write in plan.writes
            ],
            "fence": plan.fence,
            "idempotency_key": plan.idempotency_key,
            "request_hash": plan.request_hash,
        }
        for resource in plan.resources:
            await self._require(resource)
        raw = await self._eval(
            _COMMIT_SCRIPT,
            keys=[],
            args=[
                json.dumps(payload),
                _utcnow().isoformat(),
                self._prefix,
                self._history_limit,
                self._idempotency_ttl,
            ],
        )
        response = json.loads(raw)
        error = response.get("error")
        if error is not None:
            if error == "idempotency_mismatch":
                self._log.append(
                    TransactionRecord.from_plan(
                        plan,
                        status=TransactionStatus.FAILED,
                        abort_reason="idempotency key reused with a different payload",
                    )
                )
                raise IdempotencyMismatchError(
                    f"idempotency key {plan.idempotency_key!r} was already used "
                    "with a different payload",
                    idempotency_key=plan.idempotency_key,
                )
            conflict: ConflictError
            if error == "read_write":
                conflict = ReadWriteConflict(
                    transaction_id=plan.transaction_id,
                    resource=response.get("resource"),
                    key=response.get("key"),
                    expected_version=response.get("expected"),
                    actual_version=response.get("actual"),
                )
            elif error == "write_write":
                conflict = WriteWriteConflict(
                    transaction_id=plan.transaction_id,
                    resource=response.get("resource"),
                    key=response.get("key"),
                    expected_version=response.get("expected"),
                    actual_version=response.get("actual"),
                )
            elif error == "lease_lost":
                conflict = LeaseLostError(
                    transaction_id=plan.transaction_id,
                    resource=response.get("resource"),
                    token=response.get("token"),
                    current_token=response.get("current"),
                )
            else:  # pragma: no cover - script contract
                raise StorageError(f"unexpected Redis commit response: {response!r}")
            self._record_conflict(plan, conflict)
            raise conflict

        version = int(response["version"]) if response.get("version") is not None else None
        if response.get("replay"):
            committed_at = _parse_at(response.get("at"))
            return CommitResult(
                transaction_id=plan.transaction_id,
                committed=True,
                committed_at=committed_at,
                commit_version=version,
                read_set=plan.reads,
                write_set=plan.writes,
                replayed=True,
            )

        finished = _utcnow()
        result = CommitResult(
            transaction_id=plan.transaction_id,
            committed=True,
            committed_at=finished,
            commit_version=version,
            read_set=plan.reads,
            write_set=plan.writes,
        )
        status = TransactionStatus.READ_ONLY if plan.read_only else TransactionStatus.COMMITTED
        self._log.append(
            TransactionRecord.from_plan(
                plan,
                status=status,
                commit_version=version,
                finished_at=finished,
            )
        )
        if not plan.read_only:
            self._events.emit(
                Event(
                    type=EventType.COMMIT_APPLIED,
                    transaction_id=plan.transaction_id,
                    agent_id=plan.agent_id,
                    message=plan.describe(),
                    data={
                        "commit_version": version,
                        "writes": len(plan.writes),
                        "reads": len(plan.reads),
                        "isolation": plan.isolation,
                        "backend": self.name,
                    },
                )
            )
        return result

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
        lock_key = self._k("lock", resource, key)
        while True:
            acquired = await self._eval(
                _ACQUIRE_LOCK_SCRIPT,
                keys=[lock_key],
                args=[transaction_id, mode, int(self._lock_ttl * 1000)],
            )
            if int(acquired) == 1:
                await self._remember_lock(transaction_id, resource, key, mode)
                return
            if time.monotonic() >= deadline:
                raise LockTimeoutError(
                    resource=resource,
                    key=key,
                    transaction_id=transaction_id,
                    message=f"timed out after {timeout:.2f}s waiting for a {mode} lock",
                )
            await asyncio.sleep(0.05)

    async def release_locks(self, transaction_id: str) -> None:
        # A transaction may hold locks on many keys; track them in a set so
        # release is O(held) instead of scanning the keyspace.
        members_key = self._k("held_locks", transaction_id)
        members = await self._redis.smembers(members_key)
        for member in members:
            resource, key, mode = str(member).split("\x00", 2)
            await self._eval(
                _RELEASE_LOCK_SCRIPT,
                keys=[self._k("lock", resource, key)],
                args=[f"{transaction_id}|{mode}"],
            )
        await self._redis.delete(members_key)

    async def _remember_lock(self, transaction_id: str, resource: str, key: str, mode: str) -> None:
        await self._redis.sadd(
            self._k("held_locks", transaction_id), f"{resource}\x00{key}\x00{mode}"
        )
        await self._redis.expire(self._k("held_locks", transaction_id), int(self._lock_ttl) + 1)

    # -- leases --------------------------------------------------------------

    async def acquire_lease(self, resource: str, owner: str, ttl_seconds: float) -> Lease:
        self._ensure_open()
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        await self._require(resource)
        now_ms = int(time.time() * 1000)
        raw = await self._eval(
            _ACQUIRE_LEASE_SCRIPT,
            keys=[self._lease(resource), self._fence(resource)],
            args=[now_ms, owner, int(ttl_seconds * 1000)],
        )
        response = json.loads(raw)
        if response.get("error") == "held":
            raise LeaseNotAvailableError(resource, owner, str(response.get("holder")))
        expires_at = datetime.fromtimestamp(int(response["expires_at"]) / 1000, tz=UTC)
        self._events.emit(
            Event(
                type=EventType.LEASE_ACQUIRED,
                agent_id=owner,
                resource=resource,
                data={"token": response["token"], "ttl_seconds": ttl_seconds},
            )
        )
        return Lease(resource, owner, int(response["token"]), expires_at, ttl_seconds)

    async def renew_lease(self, lease: Lease, ttl_seconds: float | None = None) -> Lease:
        self._ensure_open()
        ttl = ttl_seconds if ttl_seconds is not None else lease.ttl_seconds
        if ttl <= 0:
            raise ValueError("ttl_seconds must be positive")
        now_ms = int(time.time() * 1000)
        raw = await self._eval(
            _RENEW_LEASE_SCRIPT,
            keys=[self._lease(lease.resource)],
            args=[now_ms, lease.owner, lease.token, int(ttl * 1000)],
        )
        if int(raw) == 0:
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
            datetime.fromtimestamp(int(raw) / 1000, tz=UTC),
            ttl,
        )

    async def release_lease(self, lease: Lease) -> bool:
        self._ensure_open()
        raw = await self._eval(
            _RELEASE_LEASE_SCRIPT,
            keys=[self._lease(lease.resource)],
            args=[lease.owner, lease.token],
        )
        if int(raw) == 0:
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
        token = await self._redis.get(self._fence(resource))
        return int(token or 0)

    # -- observability / lifecycle ------------------------------------------

    @property
    def log(self) -> TransactionLog:
        return self._log

    @property
    def event_bus(self) -> EventBus:
        return self._events

    async def sequence(self) -> Version:
        value = await self._redis.get(self._k("seq"))
        return int(value or 0)

    async def health_check(self) -> bool:
        if self._closed:
            return False
        try:
            return bool(await self._redis.ping())
        except Exception:  # pragma: no cover - server down
            return False

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._redis.aclose()

    async def clear(self) -> None:
        """Delete every key under this backend's prefix. Test/demo helper."""

        pattern = f"{self._prefix}:*"
        batch: list[str] = []
        async for key in self._redis.scan_iter(match=pattern, count=500):
            batch.append(str(key))
            if len(batch) >= 500:
                await self._redis.delete(*batch)
                batch.clear()
        if batch:
            await self._redis.delete(*batch)

    # -- internals -----------------------------------------------------------

    async def _eval(self, script: str, *, keys: Sequence[str], args: Sequence[Any]) -> Any:
        try:
            return await self._redis.eval(script, len(keys), *keys, *args)
        except Exception as exc:  # pragma: no cover - requires a live server
            raise StorageError(f"Redis operation failed: {exc}") from exc

    async def _require(self, resource: str) -> None:
        exists = await self._redis.exists(self._meta(resource))
        if not exists:
            raise ResourceNotFoundError(
                f"resource {resource!r} is not registered", resource=resource
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise StorageError("redis backend is closed")


def _parse_at(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:  # pragma: no cover - defensive
            pass
    return _utcnow()


def _validate_resource_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("resource name must be a non-empty string")
    if "\x00" in name:
        raise ValueError("resource name must not contain NUL characters")
