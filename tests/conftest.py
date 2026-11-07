from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from statepact import AgentSession, SharedResource, TransactionManager


@pytest.fixture
async def manager() -> AsyncIterator[TransactionManager]:
    instance = TransactionManager()
    try:
        yield instance
    finally:
        await instance.close()


def make_inventory(
    manager: TransactionManager, stock: int = 1, key: str = "sku-1"
) -> SharedResource:
    return manager.resource("inventory", {key: stock})


def reserve_script(session: AgentSession, key: str = "sku-1", amount: int = 1) -> Any:
    """Read-modify-write body used by several concurrency tests."""

    return session.read(key)
