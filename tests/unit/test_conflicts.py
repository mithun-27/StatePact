from __future__ import annotations

import pytest

from agentcommit import (
    ConflictError,
    ReadWriteConflict,
    TransactionManager,
    WriteWriteConflict,
)


async def test_occ_detects_lost_update(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        assert await s1.read("k") == 1
        assert await s2.read("k") == 1
        await s1.write("k", 2)
        await s2.write("k", 3)
        await s1.commit()
        with pytest.raises(ReadWriteConflict):
            await s2.commit()
    assert await resource.read("k") == 2


async def test_occ_blind_write_conflict(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        await s1.write("k", 2)
        await s2.write("k", 3)
        await s1.commit()
        with pytest.raises(WriteWriteConflict):
            await s2.commit()
    assert await resource.read("k") == 2


async def test_disjoint_keys_do_not_conflict(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"a": 0, "b": 0})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        await s1.write("a", 1)
        await s2.write("b", 2)
        await s1.commit()
        await s2.commit()
    assert await resource.read("a") == 1
    assert await resource.read("b") == 2


async def test_no_control_mode_loses_updates(manager: TransactionManager) -> None:
    """The baseline demonstrates the bug AgentCommit exists to prevent."""

    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource], isolation="none")
    s2 = manager.session(agent_id="a2", resources=[resource], isolation="none")
    async with s1, s2:
        assert await s1.read("k") == 1
        assert await s2.read("k") == 1
        await s1.write("k", 2)
        await s2.write("k", 3)
        await s1.commit()
        await s2.commit()
    assert await resource.read("k") == 3


async def test_snapshot_isolation_allows_write_skew(manager: TransactionManager) -> None:
    """Snapshot isolation prevents lost updates but not write skew, by definition."""

    resource = manager.resource("doctors", {"alice": True, "bob": True})
    s1 = manager.session(agent_id="a1", resources=[resource], isolation="snapshot")
    s2 = manager.session(agent_id="a2", resources=[resource], isolation="snapshot")
    async with s1, s2:
        assert await s1.read("alice") is True
        assert await s2.read("bob") is True
        if await s1.read("bob"):
            await s1.write("alice", False)
        if await s2.read("alice"):
            await s2.write("bob", False)
        await s1.commit()
        await s2.commit()
    # Write skew: both doctors went off call.
    assert await resource.read("alice") is False
    assert await resource.read("bob") is False


async def test_optimistic_rejects_write_skew(manager: TransactionManager) -> None:
    resource = manager.resource("doctors", {"alice": True, "bob": True})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        assert await s1.read("alice") is True
        assert await s2.read("bob") is True
        if await s1.read("bob"):
            await s1.write("alice", False)
        if await s2.read("alice"):
            await s2.write("bob", False)
        await s1.commit()
        with pytest.raises(ConflictError):
            await s2.commit()
    assert await resource.read("alice") is False
    assert await resource.read("bob") is True


async def test_conflict_reports_resource_and_versions(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        await s1.read("k")
        await s2.read("k")
        await s1.write("k", 2)
        await s1.commit()
        await s2.write("k", 3)
        with pytest.raises(ConflictError) as info:
            await s2.commit()
    error = info.value
    assert error.resource == "r"
    assert error.key == "k"
    assert error.retryable is True
    assert error.conflict_class in {"read_write", "write_write"}
    assert error.expected_version == 1
    assert error.actual_version == 2


async def test_snapshot_read_is_repeatable(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1:
        assert await s1.read("k") == 1
        async with s2:
            await s2.write("k", 2)
            await s2.commit()
        # s1 keeps its snapshot even though k changed.
        assert await s1.read("k") == 1
        await s1.commit()


async def test_isolation_none_reads_are_live(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource], isolation="none")
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1:
        assert await s1.read("k") == 1
        async with s2:
            await s2.write("k", 2)
            await s2.commit()
        assert await s1.read("k") == 2


async def test_expected_version_is_honoured(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        await s1.read("k")
        await s1.write("k", 2)
        await s2.write("k", 3, expected_version=1)
        await s1.commit()
        with pytest.raises(WriteWriteConflict):
            await s2.commit()


async def test_conflict_leaves_state_untouched(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"a": 1, "b": 2})
    s1 = manager.session(agent_id="a1", resources=[resource])
    s2 = manager.session(agent_id="a2", resources=[resource])
    async with s1, s2:
        await s1.read("a")
        await s2.read("a")
        await s2.write("a", 99)
        await s2.write("b", 99)
        await s1.write("a", 10)
        await s1.commit()
        with pytest.raises(ConflictError):
            await s2.commit()
    assert await resource.read("a") == 10
    assert await resource.read("b") == 2
