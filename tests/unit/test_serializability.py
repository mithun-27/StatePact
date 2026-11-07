from __future__ import annotations

from datetime import UTC, datetime, timedelta

from statepact import ConflictError, TransactionManager
from statepact.concurrency.serializability import (
    SerializationGraphAudit,
    TransactionFootprint,
)
from statepact.core.interfaces import ReadRecord, WriteRecord


def footprint(
    tx: str,
    *,
    commit_version: int,
    started: datetime,
    committed: datetime,
    reads: list[tuple[str, str, int]],
    writes: list[tuple[str, str, int]],
) -> TransactionFootprint:
    return TransactionFootprint(
        transaction_id=tx,
        started_at=started,
        committed_at=committed,
        commit_version=commit_version,
        reads=tuple(ReadRecord(r, k, v) for r, k, v in reads),
        writes=tuple(WriteRecord(r, k, None, v) for r, k, v in writes),
    )


def test_audit_detects_cycle_between_overlapping_transactions() -> None:
    now = datetime.now(UTC)
    audit = SerializationGraphAudit(window=10)
    audit.record(
        footprint(
            "other",
            commit_version=2,
            started=now,
            committed=now + timedelta(milliseconds=1),
            reads=[("r", "k", 1)],
            writes=[("r", "k", 1)],
        )
    )
    candidate = footprint(
        "candidate",
        commit_version=3,
        started=now,
        committed=now + timedelta(milliseconds=2),
        reads=[("r", "k", 1)],
        writes=[("r", "k", 1)],
    )
    cycle = audit.check(candidate)
    assert cycle is not None
    assert cycle[0] == "candidate"
    assert audit.stats()["cycles"] == 1


def test_audit_accepts_non_overlapping_transaction() -> None:
    now = datetime.now(UTC)
    audit = SerializationGraphAudit(window=10)
    audit.record(
        footprint(
            "old",
            commit_version=2,
            started=now - timedelta(seconds=10),
            committed=now - timedelta(seconds=9),
            reads=[("r", "k", 1)],
            writes=[("r", "k", 1)],
        )
    )
    candidate = footprint(
        "new",
        commit_version=3,
        started=now,
        committed=now,
        reads=[("r", "k", 2)],
        writes=[("r", "k", 2)],
    )
    assert audit.check(candidate) is None


async def test_serializable_mode_commits_consistent_workload(manager: TransactionManager) -> None:
    from statepact import RetryPolicy

    resource = manager.resource("r", {"k": 0})

    async def increment(index: int) -> None:
        async def work(session) -> None:
            value = await session.read("k")
            await session.write("k", value + 1)

        await manager.run(
            f"a{index}",
            [resource],
            work,
            isolation="serializable",
            retry_policy=RetryPolicy(max_attempts=50, base_delay=0.0, jitter=0.0),
        )

    from statepact.testing import run_concurrently

    await run_concurrently(10, increment)
    assert await resource.read("k") == 10


async def test_serializable_mode_rejects_write_skew(manager: TransactionManager) -> None:
    import pytest

    resource = manager.resource("doctors", {"alice": True, "bob": True})
    s1 = manager.session(agent_id="a1", resources=[resource], isolation="serializable")
    s2 = manager.session(agent_id="a2", resources=[resource], isolation="serializable")
    async with s1, s2:
        assert await s1.read("alice")
        assert await s2.read("bob")
        if await s1.read("bob"):
            await s1.write("alice", False)
        if await s2.read("alice"):
            await s2.write("bob", False)
        await s1.commit()
        with pytest.raises(ConflictError) as info:
            await s2.commit()
    assert info.value.conflict_class in {"read_write", "serialization"}
    assert await resource.read("alice") is False
    assert await resource.read("bob") is True
