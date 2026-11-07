"""Two-phase locking (strict 2PL) with deadlock detection.

Locks are key-level (``resource``, ``key``) and held until the session commits or
aborts. Reads take shared locks, writes take exclusive locks, and a shared lock is
upgraded in place when the same transaction writes a key it already read.

Deadlocks are detected with a wait-for graph at the moment a transaction starts
waiting: the transaction that closes the cycle is aborted and its locks are
released. This is a *detection* strategy, not prevention, and it is documented as
such.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from agentcommit.coordination.deadlock import DeadlockDetector
from agentcommit.coordination.wait_for_graph import WaitForGraph
from agentcommit.core.exceptions import DeadlockError, LockTimeoutError
from agentcommit.core.interfaces import LockMode
from agentcommit.observability.events import Event, EventBus, EventType

LockKey = tuple[str, str]


@dataclass(slots=True)
class _Waiter:
    transaction_id: str
    mode: LockMode
    future: asyncio.Future[None]
    since: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(slots=True)
class _Lock:
    readers: set[str] = field(default_factory=set)
    writer: str | None = None
    waiters: list[_Waiter] = field(default_factory=list)

    @property
    def holders(self) -> set[str]:
        holders = set(self.readers)
        if self.writer is not None:
            holders.add(self.writer)
        return holders

    @property
    def empty(self) -> bool:
        return self.writer is None and not self.readers and not self.waiters


@dataclass
class LockStats:
    acquisitions: int = 0
    waits: int = 0
    upgrades: int = 0
    timeouts: int = 0
    deadlocks: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "acquisitions": self.acquisitions,
            "waits": self.waits,
            "upgrades": self.upgrades,
            "timeouts": self.timeouts,
            "deadlocks": self.deadlocks,
        }


class LockManager:
    """Async key-level lock manager used by the in-memory pessimistic backend."""

    def __init__(
        self,
        *,
        default_timeout: float = 5.0,
        events: EventBus | None = None,
        detector: DeadlockDetector | None = None,
    ) -> None:
        if default_timeout <= 0:
            raise ValueError("default_timeout must be positive")
        self.default_timeout = default_timeout
        self._locks: dict[LockKey, _Lock] = {}
        self._held: dict[str, set[LockKey]] = {}
        self._stats = LockStats()
        self._events = events
        self._detector = detector or DeadlockDetector("requester")

    # -- public API ----------------------------------------------------------

    async def acquire(
        self,
        transaction_id: str,
        resource: str,
        key: str,
        mode: LockMode,
        timeout: float | None = None,
        *,
        agent_id: str | None = None,
    ) -> None:
        lock_key: LockKey = (resource, key)
        lock = self._locks.setdefault(lock_key, _Lock())
        effective_timeout = self.default_timeout if timeout is None else timeout
        if effective_timeout <= 0:
            raise ValueError("lock timeout must be positive")

        already_shared = transaction_id in lock.readers
        if lock.writer == transaction_id and mode == "exclusive":
            return
        if mode == "shared" and (already_shared or lock.writer == transaction_id):
            return

        if self._can_grant(lock, transaction_id, mode):
            self._grant(lock_key, lock, transaction_id, mode)
            self._emit(
                EventType.LOCK_ACQUIRED,
                transaction_id,
                agent_id,
                resource,
                key,
                {"mode": mode, "waited": False},
            )
            return

        self._stats.waits += 1
        waiter = _Waiter(
            transaction_id=transaction_id,
            mode=mode,
            future=asyncio.get_running_loop().create_future(),
        )
        lock.waiters.append(waiter)
        self._emit(
            EventType.LOCK_WAIT,
            transaction_id,
            agent_id,
            resource,
            key,
            {"mode": mode, "holders": sorted(lock.holders)},
        )

        graph = self._graph()
        cycle = graph.find_cycle(transaction_id)
        if cycle is not None:
            self._remove_waiter(lock_key, lock, waiter)
            self._stats.deadlocks += 1
            released = self.release_all(transaction_id)
            self._emit(
                EventType.SESSION_ABORTED,
                transaction_id,
                agent_id,
                resource,
                key,
                {"reason": "deadlock", "cycle": cycle, "released_locks": released},
            )
            raise DeadlockError(
                transaction_id=transaction_id,
                resource=resource,
                key=key,
                cycle=tuple(cycle),
                message=f"deadlock cycle detected: {' -> '.join(cycle)}",
            )

        try:
            await asyncio.wait_for(waiter.future, timeout=effective_timeout)
        except TimeoutError as exc:
            self._stats.timeouts += 1
            self._remove_waiter(lock_key, lock, waiter)
            raise LockTimeoutError(
                transaction_id=transaction_id,
                resource=resource,
                key=key,
                timeout_seconds=effective_timeout,
            ) from exc
        except asyncio.CancelledError:
            self._remove_waiter(lock_key, lock, waiter)
            raise

        self._emit(
            EventType.LOCK_ACQUIRED,
            transaction_id,
            agent_id,
            resource,
            key,
            {"mode": mode, "waited": True},
        )

    def release_all(self, transaction_id: str) -> int:
        """Release every lock held by ``transaction_id``. Returns the count."""

        keys = self._held.pop(transaction_id, set())
        for lock_key in keys:
            lock = self._locks.get(lock_key)
            if lock is None:  # pragma: no cover - defensive
                continue
            lock.readers.discard(transaction_id)
            if lock.writer == transaction_id:
                lock.writer = None
            self._wake(lock_key, lock)
            if lock.empty:
                self._locks.pop(lock_key, None)
        return len(keys)

    def held_by(self, transaction_id: str) -> set[LockKey]:
        return set(self._held.get(transaction_id, set()))

    def is_held(self, transaction_id: str, resource: str, key: str) -> bool:
        lock = self._locks.get((resource, key))
        if lock is None:
            return False
        return lock.writer == transaction_id or transaction_id in lock.readers

    def graph(self) -> WaitForGraph:
        return self._graph()

    def snapshot(self) -> dict[str, Any]:
        return {
            "locks": {
                f"{resource}.{key}": {
                    "writer": lock.writer,
                    "readers": sorted(lock.readers),
                    "waiters": [
                        {"transaction_id": waiter.transaction_id, "mode": waiter.mode}
                        for waiter in lock.waiters
                    ],
                }
                for (resource, key), lock in sorted(self._locks.items())
            },
            "wait_for": self._graph().to_dict(),
            "stats": self._stats.to_dict(),
        }

    @property
    def stats(self) -> LockStats:
        return self._stats

    # -- internals -----------------------------------------------------------

    @staticmethod
    def _can_grant(lock: _Lock, transaction_id: str, mode: LockMode) -> bool:
        if mode == "shared":
            return lock.writer is None or lock.writer == transaction_id
        return (lock.writer is None or lock.writer == transaction_id) and lock.readers <= {
            transaction_id
        }

    def _grant(self, lock_key: LockKey, lock: _Lock, transaction_id: str, mode: LockMode) -> None:
        if mode == "shared":
            lock.readers.add(transaction_id)
        else:
            lock.readers.discard(transaction_id)
            lock.writer = transaction_id
            if lock_key in self._held.get(transaction_id, set()):
                self._stats.upgrades += 1
        self._held.setdefault(transaction_id, set()).add(lock_key)
        self._stats.acquisitions += 1

    def _remove_waiter(self, lock_key: LockKey, lock: _Lock, waiter: _Waiter) -> None:
        with suppress(ValueError):  # pragma: no cover - already granted
            lock.waiters.remove(waiter)
        if lock.empty:
            self._locks.pop(lock_key, None)

    def _wake(self, lock_key: LockKey, lock: _Lock) -> None:
        index = 0
        while index < len(lock.waiters):
            waiter = lock.waiters[index]
            if waiter.future.done():
                lock.waiters.pop(index)
                continue
            if self._can_grant(lock, waiter.transaction_id, waiter.mode):
                lock.waiters.pop(index)
                self._grant(lock_key, lock, waiter.transaction_id, waiter.mode)
                if not waiter.future.done():
                    waiter.future.set_result(None)
                index = 0
                continue
            if waiter.mode == "exclusive":
                # Do not let later shared waiters starve an exclusive waiter.
                break
            index += 1

    def _graph(self) -> WaitForGraph:
        graph = WaitForGraph()
        for lock in self._locks.values():
            blockers = lock.holders
            for waiter in lock.waiters:
                graph.add_edges(waiter.transaction_id, blockers - {waiter.transaction_id})
        return graph

    def _emit(
        self,
        event_type: EventType,
        transaction_id: str | None,
        agent_id: str | None,
        resource: str | None,
        key: str | None,
        data: dict[str, Any],
    ) -> None:
        if self._events is None:
            return
        self._events.emit(
            Event(
                type=event_type,
                transaction_id=transaction_id,
                agent_id=agent_id,
                resource=resource,
                key=key,
                data=data,
            )
        )
