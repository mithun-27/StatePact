from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from statepact import ConfigurationError, TransactionManager
from statepact.observability.events import Event, EventBus, EventType
from statepact.observability.otel import OTelBridge, attach_otel


@dataclass
class FakeSpan:
    name: str
    attributes: dict[str, object]
    events: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    status: object | None = None
    ended: bool = False

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value

    def add_event(
        self, name: str, attributes: dict[str, object] | None = None, **_: object
    ) -> None:
        self.events.append((name, attributes or {}))

    def set_status(self, status: object) -> None:
        self.status = status

    def end(self, **_: object) -> None:
        self.ended = True


class FakeTracer:
    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    def start_span(
        self, name: str, attributes: dict[str, object] | None = None, **_: object
    ) -> FakeSpan:
        span = FakeSpan(name, dict(attributes or {}))
        self.spans.append(span)
        return span


def _event(event_type: EventType, tx: str = "tx1", **kwargs: object) -> Event:
    return Event(type=event_type, transaction_id=tx, **kwargs)


def test_bridge_creates_and_ends_a_transaction_span() -> None:
    tracer = FakeTracer()
    bridge = OTelBridge(tracer)

    bridge(
        _event(
            EventType.SESSION_STARTED,
            agent_id="agent-1",
            data={"isolation": "optimistic", "attempt": 1},
        )
    )
    bridge(_event(EventType.CONFLICT, message="lost race", data={"conflict_class": "write_write"}))
    bridge(
        _event(
            EventType.SESSION_COMMITTED,
            message="committed",
            data={"commit_version": 7, "duration_ms": 12.5},
        )
    )

    assert len(tracer.spans) == 1
    span = tracer.spans[0]
    assert span.name == "statepact.transaction"
    assert span.attributes["statepact.agent_id"] == "agent-1"
    assert span.attributes["statepact.isolation"] == "optimistic"
    assert span.attributes["statepact.outcome"] == "committed"
    assert span.attributes["statepact.commit_version"] == 7
    assert span.events[0][0] == "conflict"
    assert span.events[0][1]["statepact.conflict_class"] == "write_write"
    assert span.ended


def test_bridge_marks_aborted_attempts() -> None:
    tracer = FakeTracer()
    bridge = OTelBridge(tracer)
    bridge(_event(EventType.SESSION_STARTED, agent_id="agent-1"))
    bridge(_event(EventType.SESSION_ABORTED, message="conflict", data={"duration_ms": 3.0}))

    span = tracer.spans[0]
    assert span.attributes["statepact.outcome"] == "conflict"
    assert span.ended


def test_bridge_ignores_events_without_a_span() -> None:
    tracer = FakeTracer()
    bridge = OTelBridge(tracer)
    bridge(_event(EventType.RETRY, tx="unknown"))
    assert tracer.spans == []


def test_bridge_bounds_leaked_spans() -> None:
    tracer = FakeTracer()
    bridge = OTelBridge(tracer, max_open_spans=2)
    for index in range(3):
        bridge(_event(EventType.SESSION_STARTED, tx=f"tx{index}"))

    assert len(tracer.spans) == 3
    assert tracer.spans[0].ended
    assert tracer.spans[0].attributes["statepact.outcome"] == "unknown"
    assert not tracer.spans[1].ended

    bridge.close()
    assert all(span.ended for span in tracer.spans)


def test_bridge_rejects_invalid_limit() -> None:
    with pytest.raises(ValueError):
        OTelBridge(FakeTracer(), max_open_spans=0)


async def test_attach_otel_detaches(manager: TransactionManager) -> None:
    tracer = FakeTracer()
    detach = attach_otel(manager.event_bus, tracer)
    resource = manager.resource("r", {"k": 1})
    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 2)
        await session.commit()

    assert len(tracer.spans) == 1
    assert tracer.spans[0].ended
    detach()

    async with manager.session(agent_id="a", resources=[resource]) as session:
        await session.write("k", 3)
        await session.commit()
    assert len(tracer.spans) == 1


def test_attach_otel_without_extra_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real_import = builtins.__import__

    def blocked(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("opentelemetry"):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(ConfigurationError, match="OpenTelemetry is not installed"):
        attach_otel(EventBus())


def test_span_timestamps_come_from_events() -> None:
    tracer = FakeTracer()
    bridge = OTelBridge(tracer)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    bridge(Event(type=EventType.SESSION_STARTED, transaction_id="tx", timestamp=start))
    bridge(
        Event(
            type=EventType.SESSION_COMMITTED,
            transaction_id="tx",
            timestamp=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        )
    )
    assert tracer.spans[0].ended
