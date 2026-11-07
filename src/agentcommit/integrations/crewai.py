"""CrewAI adapter.

CrewAI tools are synchronous callables with ``name``, ``description`` and a
``run`` method. :class:`TransactionTool` is such a callable: it performs one
atomic read-modify-write against shared state and returns a JSON string that a
CrewAI agent can consume.

.. code-block:: python

    from crewai import Agent
    from agentcommit.integrations.crewai import transaction_tool

    reserve = transaction_tool(
        manager,
        name="reserve_stock",
        description="Reserve one unit of SKU-1",
        agent_id="fulfilment",
        resource=inventory,
        key="sku-1",
        update=lambda stock: stock - 1,
    )
    agent = Agent(role="fulfilment", tools=[reserve], ...)

Use ``async_run`` from async code; ``run`` is for CrewAI's synchronous worker.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from agentcommit.coordination.retry import RetryPolicy
from agentcommit.core.interfaces import CommitResult, IsolationMode
from agentcommit.core.manager import TransactionManager
from agentcommit.core.resource import SharedResource
from agentcommit.integrations.base import read_modify_write, run_sync


class TransactionTool:
    """A CrewAI-compatible tool wrapping one atomic read-modify-write."""

    def __init__(
        self,
        manager: TransactionManager,
        *,
        name: str,
        description: str,
        agent_id: str,
        resource: SharedResource,
        key: str,
        update: Callable[[Any], Any] | Callable[[Any], Awaitable[Any]],
        isolation: IsolationMode = "optimistic",
        retry_policy: RetryPolicy | None = None,
        idempotency_key: str | None = None,
        missing: Any = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not name:
            raise ValueError("tool name must be non-empty")
        self.name = name
        self.description = description
        self._manager = manager
        self._agent_id = agent_id
        self._resource = resource
        self._key = key
        self._update = update
        self._isolation = isolation
        self._retry_policy = retry_policy
        self._idempotency_key = idempotency_key
        self._missing = missing
        self._metadata = dict(metadata or {})

    def _args(self) -> dict[str, Any]:
        return {
            "agent_id": self._agent_id,
            "resource": self._resource,
            "key": self._key,
            "update": self._update,
            "isolation": self._isolation,
            "retry_policy": self._retry_policy,
            "idempotency_key": self._idempotency_key,
            "missing": self._missing,
            "metadata": self._metadata,
        }

    async def async_run(self, **_: Any) -> str:
        value, result = await read_modify_write(self._manager, **self._args())
        return _encode(self.name, value, result)

    def run(self, *_: Any, **__: Any) -> str:
        """Synchronous CrewAI entry point."""

        return run_sync(self.async_run())

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        return self.run(*args, **kwargs)


def transaction_tool(manager: TransactionManager, **kwargs: Any) -> TransactionTool:
    """Factory used by examples and tests."""

    return TransactionTool(manager, **kwargs)


def _encode(name: str, value: Any, result: CommitResult) -> str:
    payload = {
        "tool": name,
        "ok": result.committed,
        "value": value,
        "commit_version": result.commit_version,
        "replayed": result.replayed,
    }
    return json.dumps(payload, default=str)


def build_tools(
    manager: TransactionManager,
    *,
    agent_id: str,
    resources: Sequence[SharedResource],
    specs: Sequence[Mapping[str, Any]],
    isolation: IsolationMode = "optimistic",
    retry_policy: RetryPolicy | None = None,
) -> list[TransactionTool]:
    """Build several tools that all share one transaction policy."""

    tools: list[TransactionTool] = []
    for spec in specs:
        tools.append(
            TransactionTool(
                manager,
                agent_id=agent_id,
                isolation=isolation,
                retry_policy=retry_policy,
                **spec,
            )
        )
    return tools
