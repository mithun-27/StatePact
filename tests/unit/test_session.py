from __future__ import annotations

import pytest

from statepact import (
    AgentSession,
    KeyNotFoundError,
    SharedResource,
    TransactionManager,
    TransactionStateError,
)
from statepact.core.exceptions import ValidationError
from statepact.core.serde import normalize_value


async def test_basic_read_write_commit(manager: TransactionManager) -> None:
    inventory = manager.resource("inventory", {"sku-1": 5})
    async with manager.session(agent_id="a1", resources=[inventory]) as session:
        assert await session.read("sku-1") == 5
        await session.write("sku-1", 4)
        result = await session.commit()
    assert result.committed is True
    assert result.commit_version is not None
    assert await inventory.read("sku-1") == 4


async def test_default_manager_is_used_when_omitted() -> None:
    from statepact import reset_default_manager

    reset_default_manager()
    inventory = SharedResource("auto-inventory", {"sku": 2})
    async with AgentSession(agent_id="a", resources=[inventory]) as session:
        stock = await session.read("sku")
        await session.write("sku", stock - 1)
        await session.commit()
    assert await inventory.read("sku") == 1
    reset_default_manager()


async def test_read_your_writes(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        assert await session.read("k") == 2
        await session.write("k", 3)
        assert await session.read("k") == 3
        await session.commit()
    assert await resource.read("k") == 3


async def test_session_exit_without_commit_rolls_back(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 99)
        assert session.state.value == "active"
    assert await resource.read("k") == 1


async def test_exception_inside_session_rolls_back(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    with pytest.raises(RuntimeError):
        async with manager.session(agent_id="a", resources=[resource]) as session:
            await session.write("k", 99)
            raise RuntimeError("agent crashed")
    assert await resource.read("k") == 1


async def test_operations_after_commit_are_rejected(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        await session.commit()
        with pytest.raises(TransactionStateError):
            await session.read("k")
        with pytest.raises(TransactionStateError):
            await session.commit()


async def test_commit_is_idempotent_within_session(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        first = await session.commit()
        with pytest.raises(TransactionStateError):
            await session.commit()
    assert first.committed
    assert await resource.read("k") == 2


async def test_read_missing_key_raises_but_get_tracks_absence(manager: TransactionManager) -> None:
    resource = manager.resource("r", {})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        with pytest.raises(KeyNotFoundError):
            await session.read("missing")
        assert await session.get("missing") is None
        assert await session.version("missing") == 0
        await session.write("missing", "inserted", expected_version=0)
        await session.commit()
    assert await resource.read("missing") == "inserted"


async def test_multi_resource_requires_explicit_resource(manager: TransactionManager) -> None:
    left = manager.resource("left", {"k": 1})
    right = manager.resource("right", {"k": 2})
    async with manager.session(agent_id="a", resources=[left, right]) as session:
        with pytest.raises(Exception, match="multiple resources"):
            await session.read("k")
        assert await session.read("k", resource="left") == 1
        assert await session.read("k", resource=right) == 2


async def test_ambiguous_resource_is_reported(manager: TransactionManager) -> None:
    left = manager.resource("left", {"k": 1})
    right = manager.resource("right", {"k": 2})
    async with manager.session(agent_id="a", resources=[left, right]) as session:
        from statepact import AmbiguousResourceError

        with pytest.raises(AmbiguousResourceError):
            await session.read("k")


async def test_resource_must_belong_to_session(manager: TransactionManager) -> None:
    left = manager.resource("left", {"k": 1})
    other = manager.resource("other", {"k": 2})
    async with manager.session(agent_id="a", resources=[left]) as session:
        with pytest.raises(Exception, match="not part of this session"):
            await session.read("k", resource=other)


async def test_values_must_be_json_serialisable(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        with pytest.raises(ValidationError):
            await session.write("k", {1, 2, 3})
        with pytest.raises(ValidationError):
            await session.write("k", float("nan"))


def test_normalize_value_round_trips_json() -> None:
    assert normalize_value((1, 2)) == [1, 2]
    assert normalize_value({"a": (1, 2)}) == {"a": [1, 2]}


async def test_delete_and_tombstone(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.delete("k")
        assert await session.get("k") is None
        await session.commit()
    with pytest.raises(KeyNotFoundError):
        await resource.read("k")
    # A later snapshot must not resurrect the key.
    async with manager.session(agent_id="b", resources=[resource]) as session:
        assert await session.get("k") is None


async def test_read_only_commit_does_not_consume_version(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.read("k")
        result = await session.commit()
    assert result.read_only
    assert manager.backend.sequence == 1


async def test_session_metadata_and_ids(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(
        agent_id="planner", resources=[resource], metadata={"task": "demo"}
    ) as session:
        assert session.transaction_id.startswith("tx_")
        await session.read("k")
        result = await session.commit()
    assert result.transaction_id == session.transaction_id
    records = manager.transactions()
    assert records[-1].agent_id == "planner"
    assert records[-1].metadata["task"] == "demo"
