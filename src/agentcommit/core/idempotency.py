"""Idempotency for commits and external side effects.

Two different problems, two different tools:

1. **Commit deduplication** lives in the storage backend. A commit carrying an
   ``idempotency_key`` is applied once; a replay returns the original result and
   never re-applies writes. Reusing a key with a different write set raises
   :class:`~agentcommit.core.exceptions.IdempotencyMismatchError`.

2. **External side effects** (charge a card, send an email, call a third-party
   API) cannot participate in a database transaction. AgentCommit provides an
   effect journal with a strict state machine. It gives *at-least-once* execution
   with *at-most-once completion* when the action itself is idempotent or accepts
   an idempotency key. Exactly-once delivery to an arbitrary external system is
   impossible without that system's cooperation, and this library does not claim
   otherwise.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from agentcommit.core.exceptions import EffectFailedError, EffectInProgressError


class EffectState(StrEnum):
    PENDING = "pending"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class EffectRecord:
    key: str
    state: EffectState
    attempts: int = 0
    result: Any = None
    error: str | None = None
    created_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)
    updated_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "state": self.state.value,
            "attempts": self.attempts,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class EffectStore:
    """Interface for a durable effect journal (Memory implementation included)."""

    async def get(self, key: str) -> EffectRecord | None:  # pragma: no cover - interface
        raise NotImplementedError

    async def create(self, record: EffectRecord) -> bool:  # pragma: no cover - interface
        """Insert if absent. Returns False when the key already exists."""
        raise NotImplementedError

    async def compare_and_set(
        self, key: str, expected: EffectState, record: EffectRecord
    ) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    async def put(self, record: EffectRecord) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class MemoryEffectStore(EffectStore):
    """Process-local effect journal. Use a database-backed store in production."""

    def __init__(self) -> None:
        self._records: dict[str, EffectRecord] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> EffectRecord | None:
        async with self._lock:
            return self._records.get(key)

    async def create(self, record: EffectRecord) -> bool:
        async with self._lock:
            if record.key in self._records:
                return False
            self._records[record.key] = record
            return True

    async def compare_and_set(self, key: str, expected: EffectState, record: EffectRecord) -> bool:
        async with self._lock:
            current = self._records.get(key)
            if current is None or current.state is not expected:
                return False
            self._records[key] = record
            return True

    async def put(self, record: EffectRecord) -> None:
        async with self._lock:
            self._records[record.key] = record

    async def all(self) -> Mapping[str, EffectRecord]:
        async with self._lock:
            return dict(self._records)


class IdempotentExecutor:
    """Execute external actions with a durable at-most-once completion journal."""

    def __init__(self, store: EffectStore, *, clock: Callable[[], datetime] | None = None) -> None:
        self.store = store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._local_locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, key: str) -> asyncio.Lock:
        lock = self._local_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._local_locks[key] = lock
        return lock

    async def run(
        self,
        key: str,
        action: Callable[[], Awaitable[Any]],
        *,
        retry_failed: bool = False,
    ) -> Any:
        """Run ``action`` once for ``key``, replaying a confirmed result.

        A confirmed result is never re-executed. A failed attempt raises
        :class:`EffectFailedError` unless ``retry_failed=True``.
        """

        async with self._lock_for(key):
            record = await self.store.get(key)
            if record is not None:
                if record.state is EffectState.SUCCEEDED:
                    return record.result
                if record.state is EffectState.EXECUTING:
                    raise EffectInProgressError(f"effect {key!r} is already executing", key=key)
                if record.state is EffectState.FAILED and not retry_failed:
                    raise EffectFailedError(
                        f"effect {key!r} previously failed: {record.error}", key=key
                    )
                if record.state is EffectState.PENDING and not retry_failed:
                    raise EffectFailedError(
                        f"effect {key!r} was reserved but never executed", key=key
                    )

            now = self._clock()
            claimed = EffectRecord(
                key=key,
                state=EffectState.EXECUTING,
                attempts=(record.attempts if record else 0) + 1,
                created_at=record.created_at if record else now,
                updated_at=now,
            )
            if record is None:
                await self.store.create(claimed)
            else:
                await self.store.put(claimed)
            try:
                result = await action()
            except BaseException as exc:
                await self.store.put(
                    replace(
                        claimed,
                        state=EffectState.FAILED,
                        error=f"{type(exc).__name__}: {exc}",
                        updated_at=self._clock(),
                    )
                )
                raise
            await self.store.put(
                replace(
                    claimed,
                    state=EffectState.SUCCEEDED,
                    result=result,
                    updated_at=self._clock(),
                )
            )
            return result

    async def reserve(self, key: str, *, payload: Any = None) -> bool:
        """Two-phase part 1: durably record intent before committing local state."""

        now = self._clock()
        return await self.store.create(
            EffectRecord(
                key=key,
                state=EffectState.PENDING,
                result=None,
                created_at=now,
                updated_at=now,
            )
        )

    async def execute_reserved(self, key: str, action: Callable[[], Awaitable[Any]]) -> Any:
        """Two-phase part 2: execute a previously reserved effect."""

        async with self._lock_for(key):
            record = await self.store.get(key)
            if record is None:
                raise EffectFailedError(f"effect {key!r} was not reserved", key=key)
            if record.state is EffectState.SUCCEEDED:
                return record.result
            if record.state is not EffectState.PENDING:
                raise EffectFailedError(
                    f"effect {key!r} is {record.state.value}, expected pending", key=key
                )
            now = self._clock()
            executing = replace(
                record, state=EffectState.EXECUTING, attempts=record.attempts + 1, updated_at=now
            )
            if not await self.store.compare_and_set(key, EffectState.PENDING, executing):
                raise EffectInProgressError(f"effect {key!r} was claimed concurrently", key=key)
            try:
                result = await action()
            except BaseException as exc:
                await self.store.put(
                    replace(
                        executing,
                        state=EffectState.FAILED,
                        error=f"{type(exc).__name__}: {exc}",
                        updated_at=self._clock(),
                    )
                )
                raise
            await self.store.put(
                replace(
                    executing, state=EffectState.SUCCEEDED, result=result, updated_at=self._clock()
                )
            )
            return result


async def run_idempotent(
    store: EffectStore,
    key: str,
    action: Callable[[], Awaitable[Any]],
    *,
    retry_failed: bool = False,
) -> Any:
    """One-shot helper around :class:`IdempotentExecutor`."""

    return await IdempotentExecutor(store).run(key, action, retry_failed=retry_failed)
