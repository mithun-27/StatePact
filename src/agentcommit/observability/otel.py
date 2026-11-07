"""Optional OpenTelemetry bridge.

The core package has no OpenTelemetry dependency. Install the extra and attach a
bridge when you want transaction spans::

    pip install "agentcommit[otel]"

    from agentcommit.observability.otel import attach_otel

    detach = attach_otel(manager.event_bus)
    ...
    detach()

One span is created per session attempt. Conflicts, retries, lock waits and
lease events are recorded as span events on the attempt that produced them.
Leaked spans are bounded: if a session never emits a terminal event, the oldest
open span is ended (with ``agentcommit.outcome=unknown``) once ``max_open_spans``
is exceeded.

The bridge uses the OpenTelemetry API only. Configure an SDK/exporter in your
application; this module never installs a global provider.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from agentcommit.core.exceptions import ConfigurationError
from agentcommit.observability.events import Event, EventBus, EventType

_TERMINAL = {EventType.SESSION_COMMITTED, EventType.SESSION_ABORTED}
_INTERESTING = {
    EventType.CONFLICT,
    EventType.RETRY,
    EventType.LOCK_WAIT,
    EventType.LOCK_ACQUIRED,
    EventType.LOCK_RELEASED,
    EventType.LEASE_ACQUIRED,
    EventType.LEASE_RENEWED,
    EventType.LEASE_RELEASED,
    EventType.LEASE_LOST,
    EventType.COMMIT_APPLIED,
}


class OTelBridge:
    """Translate :class:`~agentcommit.observability.events.Event` objects to spans."""

    def __init__(self, tracer: Any, *, max_open_spans: int = 10_000) -> None:
        if max_open_spans <= 0:
            raise ValueError("max_open_spans must be positive")
        self._tracer = tracer
        self._max_open_spans = max_open_spans
        self._spans: OrderedDict[str, Any] = OrderedDict()

    def __call__(self, event: Event) -> None:
        self.handle(event)

    def handle(self, event: Event) -> None:
        transaction_id = event.transaction_id
        if event.type == EventType.SESSION_STARTED:
            if transaction_id is None:  # pragma: no cover - always set by sessions
                return
            self._end_oldest_if_full()
            attributes: dict[str, Any] = {"agentcommit.transaction_id": transaction_id}
            if event.agent_id:
                attributes["agentcommit.agent_id"] = event.agent_id
            isolation = event.data.get("isolation")
            if isolation:
                attributes["agentcommit.isolation"] = isolation
            attempt = event.data.get("attempt")
            if attempt is not None:
                attributes["agentcommit.attempt"] = attempt
            span = self._tracer.start_span(
                "agentcommit.transaction",
                attributes=attributes,
                start_time=event.timestamp,
            )
            self._spans[transaction_id] = span
            return

        span = self._spans.get(transaction_id) if transaction_id else None
        if span is None:
            return

        if event.type in _INTERESTING:
            span.add_event(
                event.type.value,
                attributes={
                    key: value
                    for key, value in {
                        "agentcommit.resource": event.resource,
                        "agentcommit.key": event.key,
                        "agentcommit.message": event.message or None,
                        **{
                            f"agentcommit.{name}": value
                            for name, value in event.data.items()
                            if isinstance(value, (str, int, float, bool))
                        },
                    }.items()
                    if value is not None
                },
                timestamp=event.timestamp,
            )
            return

        if event.type in _TERMINAL:
            self._finish(span, event)

    def _finish(self, span: Any, event: Event) -> None:
        outcome = "committed" if event.type == EventType.SESSION_COMMITTED else "aborted"
        if event.message and outcome == "aborted":
            outcome = event.message
        span.set_attribute("agentcommit.outcome", outcome)
        commit_version = event.data.get("commit_version")
        if commit_version is not None:
            span.set_attribute("agentcommit.commit_version", commit_version)
        duration_ms = event.data.get("duration_ms")
        if duration_ms is not None:
            span.set_attribute("agentcommit.duration_ms", duration_ms)
        status = self._status(outcome, event.message)
        if status is not None:
            span.set_status(status)
        span.end(end_time=event.timestamp)
        if event.transaction_id:
            self._spans.pop(event.transaction_id, None)

    @staticmethod
    def _status(outcome: str, message: str) -> Any:
        try:
            from opentelemetry.trace import Status, StatusCode
        except ImportError:  # pragma: no cover - fake tracers in unit tests
            return None

        if outcome == "committed":
            return Status(StatusCode.OK)
        # A conflict aborts one attempt, not the agent operation. Mark it as an
        # error so exporters surface retry pressure, with the reason attached.
        return Status(StatusCode.ERROR, message or outcome)

    def _end_oldest_if_full(self) -> None:
        while len(self._spans) >= self._max_open_spans:
            _, span = self._spans.popitem(last=False)
            span.set_attribute("agentcommit.outcome", "unknown")
            span.end()

    def close(self) -> None:
        """End every span that never received a terminal event."""

        while self._spans:
            _, span = self._spans.popitem(last=False)
            span.set_attribute("agentcommit.outcome", "unknown")
            span.end()


def attach_otel(
    event_bus: EventBus,
    tracer: Any | None = None,
    *,
    max_open_spans: int = 10_000,
) -> Callable[[], None]:
    """Subscribe an :class:`OTelBridge` to ``event_bus`` and return a detach callable.

    Raises :class:`ConfigurationError` when the ``otel`` extra is not installed.
    """

    if tracer is None:
        try:
            from opentelemetry import trace
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ConfigurationError(
                "OpenTelemetry is not installed; run `pip install agentcommit[otel]`"
            ) from exc
        tracer = trace.get_tracer("agentcommit")

    bridge = OTelBridge(tracer, max_open_spans=max_open_spans)
    unsubscribe = event_bus.subscribe(bridge)

    def detach() -> None:
        unsubscribe()
        bridge.close()

    return detach


__all__ = ["OTelBridge", "attach_otel"]
