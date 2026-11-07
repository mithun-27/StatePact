from __future__ import annotations

import asyncio

import pytest

from statepact import RetryPolicy, TransactionManager
from statepact.integrations.autogen import AutoGenTransactionMiddleware
from statepact.integrations.base import read_modify_write, run_sync, transactional
from statepact.integrations.crewai import transaction_tool
from statepact.integrations.langgraph import LangGraphCoordinator


async def test_read_modify_write_retries_without_losing_updates(
    manager: TransactionManager,
) -> None:
    resource = manager.resource("counter", {"k": 0})

    async def bump() -> None:
        await read_modify_write(
            manager,
            agent_id="a",
            resource=resource,
            key="k",
            update=lambda value: value + 1,
            retry_policy=RetryPolicy(max_attempts=200, base_delay=0.0, jitter=0.0),
        )

    await asyncio.gather(*(bump() for _ in range(20)))
    assert await resource.read("k") == 20


async def test_transactional_decorator(manager: TransactionManager) -> None:
    resource = manager.resource("decorated", {"k": 1})

    @transactional(manager, agent_id="a", resources=[resource])
    async def work(session):
        value = await session.read("k")
        await session.write("k", value + 1)
        return value + 1

    assert await work(None) == 2
    assert await resource.read("k") == 2


async def test_langgraph_node_merges_patch_and_commits(manager: TransactionManager) -> None:
    resource = manager.resource("graph", {"k": 0})
    coordinator = LangGraphCoordinator(manager, agent_id="node", resources=[resource])

    @coordinator.node
    async def increment(state, session):
        value = await session.read("k")
        await session.write("k", value + 1)
        return {"last": value + 1}

    state = await increment({"messages": []})
    assert state["messages"] == []
    assert state["last"] == 1
    assert await resource.read("k") == 1


async def test_langgraph_update_returns_commit_result(manager: TransactionManager) -> None:
    resource = manager.resource("graph-update", {"k": 5})
    coordinator = LangGraphCoordinator(manager, agent_id="tool", resources=[resource])
    value, result = await coordinator.update(
        resource=resource,
        key="k",
        update=lambda current: current * 2,
        idempotency_key="op",
        missing=0,
    )
    assert value == 10
    assert result.committed is True
    assert result.commit_version is not None


async def test_crewai_tool_async(manager: TransactionManager) -> None:
    resource = manager.resource("crew", {"k": 3})
    tool = transaction_tool(
        manager,
        name="decrement",
        description="decrement",
        agent_id="crew-agent",
        resource=resource,
        key="k",
        update=lambda value: value - 1,
    )
    assert tool.name == "decrement"
    payload = await tool.async_run()
    assert '"ok": true' in payload
    assert await resource.read("k") == 2


async def test_autogen_middleware_handler(manager: TransactionManager) -> None:
    resource = manager.resource("autogen", {"messages": []})
    middleware = AutoGenTransactionMiddleware(
        manager,
        agent_id="assistant",
        resources=[resource],
        idempotency_prefix="run-1",
    )

    @middleware.transactional
    async def handle(message, session):
        messages = await session.get("messages", default=[])
        messages.append(message)
        await session.write("messages", messages)
        return len(messages)

    assert await handle("hello") == 1
    assert await handle("world") == 2
    assert await resource.read("messages") == ["hello", "world"]


async def test_run_sync_rejects_nested_loop() -> None:
    async def noop() -> None:
        return None

    with pytest.raises(RuntimeError):
        run_sync(noop())


def test_run_sync_outside_loop() -> None:
    async def add() -> int:
        return 1 + 1

    assert run_sync(add()) == 2
