"""AutoGen adapter.

AutoGen agents expose async message handlers. Wrap the handler (or the part of
it that touches shared state) with :class:`AutoGenTransactionMiddleware`:

.. code-block:: python

    middleware = AutoGenTransactionMiddleware(
        manager,
        agent_id="planner",
        resources=[blackboard],
        idempotency_prefix="run-42",
    )

    @middleware.transactional
    async def handle(message, session):
        plan = await session.get("plan", [])
        plan.append(message.content)
        await session.write("plan", plan)
        return plan

    # AutoGen calls ``handle(message)``; the middleware supplies the session.

The decorator retries conflicts with a fresh snapshot and uses an idempotency
key derived from the middleware prefix when provided, so a redelivered message
does not apply twice.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from statepact.coordination.retry import RetryPolicy
from statepact.core.interfaces import IsolationMode
from statepact.core.manager import TransactionManager
from statepact.core.resource import SharedResource
from statepact.integrations.base import TransactionRequest, run_transaction

MessageHandler = Callable[..., Awaitable[Any]]


class AutoGenTransactionMiddleware:
    """Run AutoGen handlers inside a transactional session."""

    def __init__(
        self,
        manager: TransactionManager,
        *,
        agent_id: str,
        resources: Sequence[SharedResource],
        isolation: IsolationMode = "optimistic",
        retry_policy: RetryPolicy | None = None,
        idempotency_prefix: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._manager = manager
        self._agent_id = agent_id
        self._resources = tuple(resources)
        self._isolation = isolation
        self._retry_policy = retry_policy
        self._idempotency_prefix = idempotency_prefix
        self._metadata = dict(metadata or {})

    def _request(self, idempotency_key: str | None) -> TransactionRequest:
        return TransactionRequest(
            agent_id=self._agent_id,
            resources=self._resources,
            isolation=self._isolation,
            retry_policy=self._retry_policy,
            idempotency_key=idempotency_key,
            metadata=dict(self._metadata),
        )

    async def run[T](self, work: Any, *, idempotency_key: str | None = None) -> T:
        """Run ``work(session)`` transactionally."""

        key = self._derive_key(idempotency_key)
        return await run_transaction(self._manager, self._request(key), work)

    def transactional[T](
        self, function: Callable[..., Awaitable[T]]
    ) -> Callable[..., Awaitable[T]]:
        """Decorate ``handler(message, session, *args)`` for AutoGen."""

        async def wrapped(message: Any, *args: Any, **kwargs: Any) -> T:
            key = self._message_key(message)

            async def work(session: Any) -> T:
                return await function(message, session, *args, **kwargs)

            return await run_transaction(self._manager, self._request(key), work)

        wrapped.__name__ = getattr(function, "__name__", "transactional_handler")
        wrapped.__doc__ = function.__doc__
        return wrapped

    def _derive_key(self, key: str | None) -> str | None:
        if key is None or self._idempotency_prefix is None:
            return key
        return f"{self._idempotency_prefix}:{key}"

    def _message_key(self, message: Any) -> str | None:
        if self._idempotency_prefix is None:
            return None
        payload = getattr(message, "id", None) or repr(message)
        digest = hashlib.sha256(str(payload).encode("utf-8")).hexdigest()[:24]
        return f"{self._idempotency_prefix}:{digest}"


def transactional_handler(
    manager: TransactionManager,
    *,
    agent_id: str,
    resources: Sequence[SharedResource],
    **kwargs: Any,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Functional form of :meth:`AutoGenTransactionMiddleware.transactional`."""

    return AutoGenTransactionMiddleware(
        manager, agent_id=agent_id, resources=resources, **kwargs
    ).transactional
