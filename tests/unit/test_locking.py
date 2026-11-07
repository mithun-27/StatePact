from __future__ import annotations

import asyncio

import pytest

from statepact import DeadlockError, LockTimeoutError
from statepact.concurrency.locking import LockManager
from statepact.coordination.wait_for_graph import WaitForGraph


async def test_shared_locks_coexist() -> None:
    locks = LockManager(default_timeout=1.0)
    await locks.acquire("t1", "r", "k", "shared")
    await locks.acquire("t2", "r", "k", "shared")
    assert locks.is_held("t1", "r", "k")
    assert locks.is_held("t2", "r", "k")
    locks.release_all("t1")
    assert not locks.is_held("t1", "r", "k")
    assert locks.is_held("t2", "r", "k")


async def test_exclusive_lock_waits_for_reader() -> None:
    locks = LockManager(default_timeout=1.0)
    await locks.acquire("reader", "r", "k", "shared")
    started = asyncio.Event()
    finished = asyncio.Event()

    async def writer() -> None:
        started.set()
        await locks.acquire("writer", "r", "k", "exclusive")
        finished.set()

    task = asyncio.create_task(writer())
    await started.wait()
    await asyncio.sleep(0.01)
    assert not finished.is_set()
    locks.release_all("reader")
    await asyncio.wait_for(task, timeout=1.0)
    assert finished.is_set()
    assert locks.is_held("writer", "r", "k")


async def test_lock_upgrade() -> None:
    locks = LockManager(default_timeout=1.0)
    await locks.acquire("t1", "r", "k", "shared")
    await locks.acquire("t1", "r", "k", "exclusive")
    assert locks.is_held("t1", "r", "k")
    assert locks.stats.upgrades == 1


async def test_timeout_releases_waiter() -> None:
    locks = LockManager(default_timeout=0.02)
    await locks.acquire("holder", "r", "k", "exclusive")
    with pytest.raises(LockTimeoutError):
        await locks.acquire("waiter", "r", "k", "exclusive")
    assert locks.snapshot()["locks"]["r.k"]["waiters"] == []
    locks.release_all("holder")
    await locks.acquire("waiter", "r", "k", "exclusive")
    assert locks.is_held("waiter", "r", "k")


async def test_deadlock_detection_aborts_requester_and_releases_locks() -> None:
    locks = LockManager(default_timeout=2.0)
    await locks.acquire("t1", "r", "a", "exclusive")
    await locks.acquire("t2", "r", "b", "exclusive")
    first = asyncio.create_task(locks.acquire("t1", "r", "b", "exclusive"))
    await asyncio.sleep(0.01)
    with pytest.raises(DeadlockError) as info:
        await locks.acquire("t2", "r", "a", "exclusive")
    assert "cycle" in info.value.details
    assert locks.stats.deadlocks == 1
    assert not locks.is_held("t2", "r", "b")
    await asyncio.wait_for(first, timeout=1.0)
    assert locks.is_held("t1", "r", "a")
    assert locks.is_held("t1", "r", "b")


async def test_release_all_is_safe_for_unknown_transaction() -> None:
    locks = LockManager()
    assert locks.release_all("ghost") == 0


async def test_graph_edges_from_waiters() -> None:
    locks = LockManager(default_timeout=1.0)
    await locks.acquire("holder", "r", "k", "exclusive")
    waiter = asyncio.create_task(locks.acquire("waiter", "r", "k", "exclusive"))
    await asyncio.sleep(0.01)
    snapshot = locks.snapshot()
    assert snapshot["wait_for"] == {"waiter": ["holder"]}
    locks.release_all("holder")
    await waiter


def test_wait_for_graph_cycle_detection() -> None:
    graph = WaitForGraph.from_blockers({"a": {"b"}, "b": {"c"}, "c": {"a"}})
    cycle = graph.find_cycle("a")
    assert cycle is not None
    assert cycle[0] == "a"
    assert cycle[-1] == "a"
    assert graph.any_cycle() is not None


def test_wait_for_graph_handles_self_edges_and_empty() -> None:
    graph = WaitForGraph()
    graph.add_edge("a", "a")
    assert graph.edges == {}
    assert graph.find_cycle("a") is None
    assert graph.any_cycle() is None
