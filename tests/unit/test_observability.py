from __future__ import annotations

import asyncio

from agentcommit import EventType, TransactionManager
from agentcommit.observability.events import Event, EventBus
from agentcommit.observability.metrics import Metrics, summarize_latencies
from agentcommit.observability.transaction_logs import TransactionLog


async def test_event_bus_history_and_unsubscribe() -> None:
    bus = EventBus(history_size=10)
    seen: list[str] = []

    def hook(event: Event) -> None:
        seen.append(event.type.value)

    unsubscribe = bus.subscribe(hook)
    bus.emit(Event(type=EventType.SESSION_STARTED))
    bus.emit(Event(type=EventType.SESSION_COMMITTED))
    unsubscribe()
    bus.emit(Event(type=EventType.SESSION_ABORTED))
    assert seen == ["session_started", "session_committed"]
    assert len(bus) == 3
    assert bus.recent(1)[0].type is EventType.SESSION_ABORTED


async def test_async_hooks_are_scheduled_and_flushed() -> None:
    bus = EventBus()
    seen: list[str] = []

    async def hook(event: Event) -> None:
        await asyncio.sleep(0)
        seen.append(event.type.value)

    bus.subscribe(hook)
    bus.emit(Event(type=EventType.RETRY))
    await bus.aclose()
    assert seen == ["retry"]


async def test_broken_hook_does_not_break_commit(manager: TransactionManager) -> None:
    def broken(event: Event) -> None:
        raise RuntimeError("bad subscriber")

    manager.event_bus.subscribe(broken)
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        await session.commit()
    assert await resource.read("k") == 2


def test_metrics_snapshot_and_percentiles() -> None:
    metrics = Metrics()
    metrics.increment("commits")
    metrics.increment("commits", 2)
    metrics.gauge("stock", 4)
    for value in range(1, 101):
        metrics.observe("latency_ms", float(value))
    snapshot = metrics.snapshot()
    assert snapshot["counters"]["commits"] == 3
    assert snapshot["gauges"]["stock"] == 4
    assert metrics.percentile("latency_ms", 0.50) == 50.5
    assert snapshot["histograms"]["latency_ms"]["p95"] == 95.05


def test_summarize_latencies_empty() -> None:
    assert summarize_latencies([])["count"] == 0.0


def test_transaction_log_filters(manager: TransactionManager) -> None:
    # Covered end-to-end by the session tests, exercised here for the query API.
    from datetime import UTC, datetime

    from agentcommit.observability.transaction_logs import (
        TransactionRecord,
        TransactionStatus,
    )

    log = TransactionLog(max_records=3)
    for index in range(4):
        log.append(
            TransactionRecord(
                transaction_id=f"tx{index}",
                agent_id="agent",
                isolation="optimistic",
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                status=TransactionStatus.COMMITTED,
                conflict_class="read_write" if index == 3 else None,
            )
        )
    assert len(log) == 3
    assert len(log.committed()) == 3
    assert len(log.conflicts()) == 1
    assert log.for_agent("agent", limit=1)[0].transaction_id == "tx3"


async def test_session_metrics_and_events(manager: TransactionManager) -> None:
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        await session.commit()
    snapshot = manager.metrics.snapshot()
    assert snapshot["counters"]["agentcommit.tx.committed"] == 1.0
    assert any(event.type is EventType.SESSION_COMMITTED for event in manager.event_bus.history)
    record = manager.transactions()[-1]
    assert record.committed
    assert record.duration_ms >= 0.0
