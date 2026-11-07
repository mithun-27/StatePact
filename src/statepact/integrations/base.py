"""Framework-neutral transaction helpers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from statepact.coordination.retry import RetryPolicy
from statepact.core.interfaces import CommitResult, IsolationMode
from statepact.core.manager import Replan, TransactionManager, Work
from statepact.core.resource import SharedResource


@dataclass(frozen=True, slots=True)
class TransactionRequest:
    """Portable description of one agent operation.

    Frameworks construct this from their own context (thread id, graph run id,
    tool call id) and hand it to :func:`run_transaction`.
    """

    agent_id: str
    resources: Sequence[SharedResource]
    isolation: IsolationMode = "optimistic"
    retry_policy: RetryPolicy | None = None
    idempotency_key: str | None = None
    idempotency_fingerprint: str | None = None
    fence: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    lock_timeout: float | None = None


async def run_transaction[T](
    manager: TransactionManager,
    request: TransactionRequest,
    work: Work[T],
    *,
    replan: Replan | None = None,
) -> T:
    """Execute ``work`` with retries under the request's transaction policy."""

    return await manager.run(
        request.agent_id,
        request.resources,
        work,
        isolation=request.isolation,
        retry_policy=request.retry_policy,
        replan=replan,
        idempotency_key=request.idempotency_key,
        idempotency_fingerprint=request.idempotency_fingerprint,
        metadata=request.metadata,
        fence=request.fence,
        lock_timeout=request.lock_timeout,
    )


def run_sync[T](coroutine: Awaitable[T]) -> T:
    """Run an awaitable from synchronous framework code.

    Raises a clear error instead of silently nesting event loops. For async
    frameworks use the ``async`` methods directly.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coroutine)
        finally:
            loop.close()
    close = getattr(coroutine, "close", None)
    if close is not None:
        close()
    raise RuntimeError(
        "run_sync() cannot be used inside a running event loop; "
        "use the async API (e.g. `await tool.arun(...)`) instead"
    )


def transactional[T](
    manager: TransactionManager,
    *,
    agent_id: str,
    resources: Sequence[SharedResource],
    isolation: IsolationMode = "optimistic",
    retry_policy: RetryPolicy | None = None,
    idempotency_key: str | None = None,
    idempotency_fingerprint: str | None = None,
    fence: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> Callable[[Work[T]], Work[T]]:
    """Decorate an async ``work(session)`` function with retry semantics."""

    request = TransactionRequest(
        agent_id=agent_id,
        resources=resources,
        isolation=isolation,
        retry_policy=retry_policy,
        idempotency_key=idempotency_key,
        idempotency_fingerprint=idempotency_fingerprint,
        fence=fence,
        metadata=dict(metadata or {}),
    )

    def decorator(work: Work[T]) -> Work[T]:
        async def runner(_: Any) -> T:
            return await run_transaction(manager, request, work)

        return runner

    return decorator


async def read_modify_write[T](
    manager: TransactionManager,
    *,
    agent_id: str,
    resource: SharedResource,
    key: str,
    update: Callable[[Any], T] | Callable[[Any], Awaitable[T]],
    isolation: IsolationMode = "optimistic",
    retry_policy: RetryPolicy | None = None,
    idempotency_key: str | None = None,
    missing: Any = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[T, CommitResult]:
    """Atomically read ``key``, apply ``update`` and commit.

    Returns ``(new_value, commit_result)``. ``update`` may be sync or async. This
    is the canonical operation agent frameworks need: a tool that mutates shared
    state without losing concurrent updates.
    """

    async def work_with_result(session: Any) -> tuple[T, CommitResult]:
        current = await session.get(key, default=missing)
        proposed = update(current)
        if inspect.isawaitable(proposed):
            proposed = await cast(Awaitable[T], proposed)
        await session.write(key, proposed)
        result = await session.commit()
        return proposed, result

    return await manager.run(
        agent_id,
        [resource],
        work_with_result,
        isolation=isolation,
        retry_policy=retry_policy,
        idempotency_key=idempotency_key,
        metadata=metadata,
    )
