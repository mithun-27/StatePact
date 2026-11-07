"""Transaction manager: the composition root for sessions, retries and metrics."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar

from statepact.coordination.retry import RetryOutcome, RetryPolicy, run_with_retry
from statepact.core.interfaces import IsolationMode, Lease, StorageBackend
from statepact.core.resource import SharedResource
from statepact.core.session import AgentSession
from statepact.observability.events import Event, EventBus, EventType
from statepact.observability.metrics import Metrics
from statepact.observability.transaction_logs import TransactionLog, TransactionRecord
from statepact.storage.memory import MemoryBackend

T = TypeVar("T")

Work = Callable[[AgentSession], Awaitable[T]]
Replan = Callable[[BaseException, int], Awaitable[None] | None]


class TransactionManager:
    """Owns a backend and provides session/retry helpers.

    One manager maps to one backend. Sharing a manager between agents is the
    intended deployment: the manager is stateless apart from observability.
    """

    def __init__(
        self,
        backend: StorageBackend | None = None,
        *,
        transaction_log: TransactionLog | None = None,
        metrics: Metrics | None = None,
        event_bus: EventBus | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._backend: StorageBackend
        if backend is None:
            bus = event_bus if event_bus is not None else EventBus()
            self.event_bus = bus
            self._backend = MemoryBackend(event_bus=bus, transaction_log=transaction_log)
        else:
            self._backend = backend
            backend_bus = getattr(backend, "event_bus", None)
            if event_bus is not None:
                self.event_bus = event_bus
            elif backend_bus is not None:
                self.event_bus = backend_bus
            else:
                self.event_bus = EventBus()
        backend_log = getattr(self._backend, "log", None)
        if transaction_log is not None:
            self.log = transaction_log
        elif backend_log is not None:
            self.log = backend_log
        else:
            self.log = TransactionLog()
        self.metrics = metrics or Metrics()
        self.retry_policy = retry_policy or RetryPolicy()

    # -- accessors -----------------------------------------------------------

    @property
    def backend(self) -> StorageBackend:
        return self._backend

    @property
    def backend_name(self) -> str:
        return self._backend.name

    def resource(
        self,
        name: str,
        initial_state: Mapping[str, Any] | None = None,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SharedResource:
        """Create a resource bound to this manager."""

        return SharedResource(name, initial_state, metadata=metadata, manager=self)

    def session(
        self,
        *,
        agent_id: str,
        resources: Sequence[SharedResource] = (),
        isolation: IsolationMode = "optimistic",
        idempotency_key: str | None = None,
        idempotency_fingerprint: str | None = None,
        fence: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        lock_timeout: float | None = None,
        retry_attempt: int = 1,
    ) -> AgentSession:
        return AgentSession(
            agent_id=agent_id,
            resources=resources,
            isolation=isolation,
            manager=self,
            idempotency_key=idempotency_key,
            idempotency_fingerprint=idempotency_fingerprint,
            fence=fence,
            metadata=metadata,
            lock_timeout=lock_timeout,
            retry_attempt=retry_attempt,
        )

    async def ensure_resources(self, *resources: SharedResource) -> None:
        for resource in resources:
            await resource.ensure(self)

    # -- retry orchestration -------------------------------------------------

    async def run(
        self,
        agent_id: str,
        resources: Sequence[SharedResource],
        work: Work[T],
        *,
        isolation: IsolationMode = "optimistic",
        retry_policy: RetryPolicy | None = None,
        replan: Replan | None = None,
        idempotency_key: str | None = None,
        idempotency_fingerprint: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        fence: int | None = None,
        lock_timeout: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> T:
        """Run ``work`` in a fresh session per attempt, retrying conflicts.

        Each retry takes a **new snapshot** and re-executes the whole body. Staged
        writes from a failed attempt are never reused. If ``idempotency_key`` is
        supplied, the backend deduplicates commits across attempts.
        """

        outcome = await self.run_outcome(
            agent_id,
            resources,
            work,
            isolation=isolation,
            retry_policy=retry_policy,
            replan=replan,
            idempotency_key=idempotency_key,
            idempotency_fingerprint=idempotency_fingerprint,
            metadata=metadata,
            fence=fence,
            lock_timeout=lock_timeout,
            sleep=sleep,
            rng=rng,
        )
        return outcome.value

    async def run_outcome(
        self,
        agent_id: str,
        resources: Sequence[SharedResource],
        work: Work[T],
        *,
        isolation: IsolationMode = "optimistic",
        retry_policy: RetryPolicy | None = None,
        replan: Replan | None = None,
        idempotency_key: str | None = None,
        idempotency_fingerprint: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        fence: int | None = None,
        lock_timeout: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> RetryOutcome[T]:
        policy = retry_policy or self.retry_policy

        async def attempt(number: int) -> T:
            attempt_metadata = dict(metadata or {})
            attempt_metadata["attempt"] = number
            async with self.session(
                agent_id=agent_id,
                resources=resources,
                isolation=isolation,
                idempotency_key=idempotency_key,
                idempotency_fingerprint=idempotency_fingerprint,
                fence=fence,
                metadata=attempt_metadata,
                lock_timeout=lock_timeout,
                retry_attempt=number,
            ) as session:
                value = await work(session)
                if session.state.value == "active":
                    await session.commit()
                return value

        async def on_retry(error: BaseException, number: int, delay: float) -> None:
            self.metrics.increment("statepact.retry.attempts")
            self.metrics.observe("statepact.retry.delay_ms", delay * 1000.0)
            self.event_bus.emit(
                Event(
                    type=EventType.RETRY,
                    agent_id=agent_id,
                    message=str(error),
                    data={
                        "attempt": number,
                        "delay_seconds": delay,
                        "error": type(error).__name__,
                        "conflict_class": getattr(error, "conflict_class", None),
                    },
                )
            )

        return await run_with_retry(
            attempt,
            policy,
            on_retry=on_retry,
            replan=replan,
            sleep=sleep,
            rng=rng,
        )

    # -- observability / lifecycle ------------------------------------------

    def transactions(self, limit: int = 100) -> list[TransactionRecord]:
        return self.log.recent(limit)

    def stats(self) -> dict[str, Any]:
        stats_fn = getattr(self._backend, "stats", None)
        base = dict(stats_fn()) if callable(stats_fn) else {}
        base.update({"backend": self._backend.name, "metrics": self.metrics.snapshot()})
        return base

    @property
    def lock_stats(self) -> Any:
        return getattr(self._backend, "lock_stats", None)

    async def lease(self, resource: str, owner: str, ttl_seconds: float = 30.0) -> Lease:
        # A lease is a coordination primitive over a named resource; make sure the
        # resource exists so `manager.lease("jobs", ...)` works before any session.
        await self._backend.ensure_resource(resource, {})
        return await self._backend.acquire_lease(resource, owner, ttl_seconds)

    async def health_check(self) -> bool:
        return await self._backend.health_check()

    async def close(self) -> None:
        await self._backend.close()
        await self.event_bus.aclose()

    def __repr__(self) -> str:
        return f"TransactionManager(backend={self._backend.name!r})"


_default_manager: TransactionManager | None = None


def default_manager() -> TransactionManager:
    """Process-wide lazy manager used when no manager is supplied explicitly."""

    global _default_manager
    if _default_manager is None:
        _default_manager = TransactionManager()
    return _default_manager


def set_default_manager(manager: TransactionManager) -> None:
    global _default_manager
    _default_manager = manager


def reset_default_manager() -> None:
    global _default_manager
    _default_manager = None
