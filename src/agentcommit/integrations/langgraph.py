"""LangGraph adapter.

LangGraph nodes are ordinary callables that receive graph state and return a
state update. Wrap a node with :meth:`LangGraphCoordinator.node` when the node
touches shared agent state:

.. code-block:: python

    coordinator = LangGraphCoordinator(
        manager, agent_id="researcher", resources=[inventory]
    )

    @coordinator.node
    async def reserve(state, session):
        stock = await session.read("sku-1")
        if stock == 0:
            return {"status": "sold_out"}
        await session.write("sku-1", stock - 1)
        return {"status": "reserved"}

    graph.add_node("reserve", reserve)

The node's return mapping is merged into the graph state after the commit. A
conflict causes the whole node to be retried by ``TransactionManager`` with a
fresh snapshot, which is the correct behaviour for a read-modify-write step.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from functools import wraps
from typing import Any

from agentcommit.coordination.retry import RetryPolicy
from agentcommit.core.interfaces import CommitResult, IsolationMode
from agentcommit.core.manager import TransactionManager
from agentcommit.core.resource import SharedResource
from agentcommit.integrations.base import TransactionRequest, read_modify_write, run_transaction

NodeFunction = Callable[[Mapping[str, Any], Any], Mapping[str, Any] | Awaitable[Mapping[str, Any]]]


class LangGraphCoordinator:
    """Connect LangGraph nodes to one :class:`TransactionManager`."""

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
        self._request = TransactionRequest(
            agent_id=agent_id,
            resources=tuple(resources),
            isolation=isolation,
            retry_policy=retry_policy,
            metadata=dict(metadata or {}),
        )
        self._idempotency_prefix = idempotency_prefix

    @property
    def request(self) -> TransactionRequest:
        return self._request

    def node(
        self, function: NodeFunction
    ) -> Callable[[Mapping[str, Any]], Awaitable[Mapping[str, Any]]]:
        """Wrap an async or sync ``(state, session) -> patch`` node."""

        @wraps(function)
        async def wrapped(state: Mapping[str, Any]) -> Mapping[str, Any]:
            async def work(session: Any) -> Mapping[str, Any]:
                patch = function(state, session)
                if inspect.isawaitable(patch):
                    patch = await patch
                return dict(patch or {})

            patch = await run_transaction(self._manager, self._request, work)
            return {**state, **patch}

        return wrapped

    async def update(
        self,
        *,
        resource: SharedResource,
        key: str,
        update: Callable[[Any], Any],
        idempotency_key: str | None = None,
        missing: Any = None,
    ) -> tuple[Any, CommitResult]:
        """Convenience read-modify-write for tool nodes."""

        effective_key = idempotency_key
        if effective_key is not None and self._idempotency_prefix is not None:
            effective_key = f"{self._idempotency_prefix}:{effective_key}"
        return await read_modify_write(
            self._manager,
            agent_id=self._request.agent_id,
            resource=resource,
            key=key,
            update=update,
            isolation=self._request.isolation,
            retry_policy=self._request.retry_policy,
            idempotency_key=effective_key,
            missing=missing,
            metadata=dict(self._request.metadata),
        )
