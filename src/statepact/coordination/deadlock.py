"""Deadlock detection and victim selection.

StatePact offers two strategies:

``detect`` (implemented by the pessimistic backend)
    Allow waits, build a wait-for graph, and abort a victim when a cycle appears.
    This is what the in-memory 2PL backend uses.

``prevent`` (documented alternative)
    A caller can sort resource keys before locking to impose a global order. The
    public ``TransactionManager.lock_order_hint`` supports this, but sessions do
    not reorder arbitrary agent logic, so detection is the default.

Aborting the *requester* that closes the cycle is always correct and keeps the
lock manager simple. Victim selection by age is exposed for callers that need
more sophisticated policies.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from statepact.coordination.wait_for_graph import WaitForGraph


@dataclass(frozen=True, slots=True)
class Deadlock:
    cycle: tuple[str, ...]
    victim: str

    @property
    def length(self) -> int:
        return max(0, len(self.cycle) - 1)


class DeadlockDetector:
    """Detect cycles and pick a victim.

    ``youngest`` aborts the transaction that started most recently, which usually
    minimizes wasted work because the oldest transaction has done the most.
    """

    def __init__(self, strategy: str = "youngest") -> None:
        if strategy not in {"youngest", "oldest", "requester"}:
            raise ValueError(f"unknown deadlock strategy: {strategy!r}")
        self.strategy = strategy

    def detect(
        self,
        graph: WaitForGraph,
        *,
        requester: str,
        started_at: Mapping[str, datetime] | None = None,
    ) -> Deadlock | None:
        cycle = graph.find_cycle(requester)
        if cycle is None:
            return None
        victim = self._choose_victim(cycle, requester=requester, started_at=started_at or {})
        return Deadlock(cycle=tuple(cycle), victim=victim)

    def _choose_victim(
        self,
        cycle: Sequence[str],
        *,
        requester: str,
        started_at: Mapping[str, datetime],
    ) -> str:
        if self.strategy == "requester" or not started_at:
            return requester
        participants = [node for node in cycle[:-1] if node in started_at]
        if not participants:
            return requester
        key = started_at.__getitem__
        return (
            max(participants, key=key)
            if self.strategy == "youngest"
            else min(participants, key=key)
        )
