"""Structured lifecycle events.

Events are intentionally decoupled from logging: the same stream feeds Python
logging, OpenTelemetry spans, the transaction log and the Gradio playground's
timeline. Handlers must never block the transaction path; async handlers are
scheduled on the running loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, MutableMapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger("statepact.events")

EventHook = Callable[["Event"], None] | Callable[["Event"], Awaitable[None]]


class EventType(StrEnum):
    SESSION_STARTED = "session_started"
    SESSION_COMMITTED = "session_committed"
    SESSION_ABORTED = "session_aborted"
    CONFLICT = "conflict"
    RETRY = "retry"
    LOCK_WAIT = "lock_wait"
    LOCK_ACQUIRED = "lock_acquired"
    LOCK_RELEASED = "lock_released"
    LEASE_ACQUIRED = "lease_acquired"
    LEASE_RENEWED = "lease_renewed"
    LEASE_RELEASED = "lease_released"
    LEASE_LOST = "lease_lost"
    COMMIT_APPLIED = "commit_applied"
    GC = "gc"


@dataclass(frozen=True, slots=True)
class Event:
    type: EventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    transaction_id: str | None = None
    agent_id: str | None = None
    resource: str | None = None
    key: str | None = None
    message: str = ""
    data: MutableMapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "timestamp": self.timestamp.isoformat(),
            "transaction_id": self.transaction_id,
            "agent_id": self.agent_id,
            "resource": self.resource,
            "key": self.key,
            "message": self.message,
            "data": dict(self.data),
        }


class EventBus:
    """Bounded in-process pub/sub with synchronous dispatch.

    A misbehaving subscriber is logged and skipped; it can never fail a commit.
    """

    def __init__(
        self, *, history_size: int = 10_000, logger_: logging.Logger | None = None
    ) -> None:
        self._hooks: list[EventHook] = []
        self._history: deque[Event] = deque(maxlen=history_size)
        self._log = logger_ or logger
        self._task_refs: set[asyncio.Task[None]] = set()

    def subscribe(self, hook: EventHook) -> Callable[[], None]:
        self._hooks.append(hook)

        def unsubscribe() -> None:
            with suppress(ValueError):  # pragma: no cover - double unsubscribe
                self._hooks.remove(hook)

        return unsubscribe

    def emit(self, event: Event) -> None:
        self._history.append(event)
        for hook in tuple(self._hooks):
            try:
                result = hook(event)
            except Exception:  # pragma: no cover - defensive
                self._log.exception("event hook failed for %s", event.type.value)
                continue
            if asyncio.iscoroutine(result):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:  # pragma: no cover - no loop, drop async hook
                    self._log.debug("dropping async event hook without a running loop")
                    continue
                task = loop.create_task(result)
                self._task_refs.add(task)
                task.add_done_callback(self._task_refs.discard)

    @property
    def history(self) -> tuple[Event, ...]:
        return tuple(self._history)

    def recent(self, limit: int = 100) -> list[Event]:
        if limit <= 0:
            return []
        return list(self._history)[-limit:]

    def clear(self) -> None:
        self._history.clear()

    def __len__(self) -> int:
        return len(self._history)

    async def aclose(self) -> None:
        """Await in-flight async hooks. Best effort, used by the demo."""

        if self._task_refs:
            await asyncio.gather(*tuple(self._task_refs), return_exceptions=True)
        self._task_refs.clear()
