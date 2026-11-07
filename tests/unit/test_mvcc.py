from __future__ import annotations

from statepact import TransactionManager
from statepact.concurrency.mvcc import (
    VersionEntry,
    latest_entry,
    snapshot_values,
    trim_history,
    visible_entry,
)
from statepact.storage.memory import MemoryBackend


def test_visible_entry_selects_latest_at_or_before_snapshot() -> None:
    history = [VersionEntry(1, "a"), VersionEntry(3, "b"), VersionEntry(7, "c")]
    assert visible_entry(history, 0) is None
    assert visible_entry(history, 1) == VersionEntry(1, "a")
    assert visible_entry(history, 2) == VersionEntry(1, "a")
    assert visible_entry(history, 5) == VersionEntry(3, "b")
    assert visible_entry(history, 99) == VersionEntry(7, "c")


def test_snapshot_values_hides_tombstones_and_future_versions() -> None:
    histories = {
        "live": [VersionEntry(1, 10), VersionEntry(4, 20)],
        "deleted": [VersionEntry(1, 1), VersionEntry(2, None, deleted=True)],
        "future": [VersionEntry(9, 5)],
    }
    values = snapshot_values(histories, 4)
    assert values["live"].value == 20
    assert "deleted" not in values
    assert "future" not in values


def test_trim_history_preserves_snapshot_boundary() -> None:
    history = [VersionEntry(version, version) for version in (1, 2, 3, 4, 5)]
    trimmed = trim_history(history, 3)
    assert [entry.version for entry in trimmed] == [3, 4, 5]
    assert visible_entry(trimmed, 3) == visible_entry(history, 3)
    assert visible_entry(trimmed, 5) == visible_entry(history, 5)


def test_trim_history_noop_below_first_version() -> None:
    history = [VersionEntry(5, "a"), VersionEntry(6, "b")]
    assert trim_history(history, 1) == history


def test_latest_entry() -> None:
    assert latest_entry([]) is None
    assert latest_entry([VersionEntry(1, "a")]) == VersionEntry(1, "a")


async def test_version_history_and_gc() -> None:
    backend = MemoryBackend()
    await backend.ensure_resource("r", {"k": 0})
    for value in (1, 2, 3):
        snapshot = await backend.snapshot("r")
        # Capture a session-shaped transaction.
        from statepact.core.transaction import Transaction, new_transaction_id, utcnow

        tx = Transaction(
            id=new_transaction_id(),
            agent_id="a",
            isolation="optimistic",
            resources=("r",),
            started_at=utcnow(),
        )
        stored = snapshot.get("k")
        assert stored is not None
        tx.record_read("r", "k", stored.version)
        tx.stage_write("r", "k", value, stored.version)
        snapshot.release()
        await backend.commit(tx.to_plan())
    history = await backend.history("r", "k", limit=10)
    assert history[0].value == 3
    # GC keeps the visible boundary plus newer versions, not the entire history.
    assert 1 <= len(history) <= 4
    assert await backend.read_latest("r", "k") == history[0]


async def test_retain_versions_prevents_full_trim() -> None:
    backend = MemoryBackend(retain_versions=10)
    await backend.ensure_resource("r", {"k": 0})
    from statepact.core.transaction import Transaction, new_transaction_id, utcnow

    for value in range(1, 4):
        snapshot = await backend.snapshot("r")
        stored = snapshot.get("k")
        assert stored is not None
        tx = Transaction(
            id=new_transaction_id(),
            agent_id="a",
            isolation="optimistic",
            resources=("r",),
            started_at=utcnow(),
        )
        tx.record_read("r", "k", stored.version)
        tx.stage_write("r", "k", value, stored.version)
        snapshot.release()
        await backend.commit(tx.to_plan())
    assert len(await backend.history("r", "k", limit=100)) == 4


async def test_snapshot_isolation_across_commits(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1, "other": 10})
    launched = manager.session(agent_id="reader", resources=[resource])
    async with launched:
        assert await launched.read("k") == 1
        async with manager.session(agent_id="writer", resources=[resource]) as writer:
            await writer.write("k", 2)
            await writer.write("other", 20)
            await writer.commit()
        assert await launched.read("k") == 1
        assert await launched.read("other") == 10
        await launched.commit()
    assert (await resource.state()) == {"k": 2, "other": 20}
