"""Serialization-graph audit.

Used by ``isolation="serializable"``. After a commit candidate has passed version
validation, the auditor builds a directed graph from version observations:

* ``T -> O`` when ``T`` read a version that predates a write by ``O`` (``T`` must
  be serialized before ``O``);
* ``O -> T`` when ``O`` read the base version that ``T`` writes from;
* ``O -> T`` for write-write conflicts ordered by commit sequence.

A cycle means the committed history has no serial equivalent. With full read-set
validation this should be unreachable for tracked keys; the audit exists to make
the property testable and to catch mixed-isolation workloads.

Phantoms (new keys appearing in a scanned range) are not tracked. That limitation
is explicit in the documentation.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from statepact.core.interfaces import ReadRecord, WriteRecord


@dataclass(frozen=True, slots=True)
class TransactionFootprint:
    """The read/write footprint of a committed transaction."""

    transaction_id: str
    started_at: datetime
    committed_at: datetime
    commit_version: int
    reads: tuple[ReadRecord, ...]
    writes: tuple[WriteRecord, ...]

    def read_versions(self) -> dict[tuple[str, str], int]:
        return {(read.resource, read.key): read.version for read in self.reads}

    def written_keys(self) -> set[tuple[str, str]]:
        return {(write.resource, write.key) for write in self.writes}


@dataclass
class _Graph:
    edges: dict[str, set[str]] = field(default_factory=dict)

    def add(self, source: str, target: str) -> None:
        if source == target:
            return
        self.edges.setdefault(source, set()).add(target)
        self.edges.setdefault(target, set())

    def find_cycle(self, start: str) -> list[str] | None:
        if start not in self.edges:
            return None
        queue: deque[str] = deque([start])
        parent: dict[str, str | None] = {start: None}
        while queue:
            node = queue.popleft()
            for neighbour in sorted(self.edges.get(node, ())):
                if neighbour == start:
                    path = [node]
                    cursor = node
                    while parent.get(cursor) is not None:
                        cursor = parent[cursor]  # type: ignore[assignment]
                        path.append(cursor)
                    path.reverse()
                    return [*path, start]
                if neighbour not in parent:
                    parent[neighbour] = node
                    queue.append(neighbour)
        return None

    def to_dict(self) -> dict[str, list[str]]:
        return {node: sorted(targets) for node, targets in sorted(self.edges.items())}


class SerializationGraphAudit:
    """Bound-window auditor for overlapping committed transactions."""

    def __init__(self, *, window: int = 512) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self._window = window
        self._recent: deque[TransactionFootprint] = deque(maxlen=window)
        self._checks = 0
        self._cycles = 0

    def overlapping(self, candidate: TransactionFootprint) -> list[TransactionFootprint]:
        """Committed transactions whose lifetime overlaps the candidate."""

        return [
            footprint
            for footprint in self._recent
            if footprint.committed_at >= candidate.started_at
        ]

    def build_graph(self, candidate: TransactionFootprint) -> _Graph:
        graph = _Graph()
        graph.edges.setdefault(candidate.transaction_id, set())
        candidate_reads = candidate.read_versions()
        candidate_writes = candidate.written_keys()

        for other in self.overlapping(candidate):
            other_reads = other.read_versions()
            other_write_keys = other.written_keys()

            # T read a version that O changed afterwards => T before O.
            for key, version in candidate_reads.items():
                for write in other.writes:
                    if (write.resource, write.key) == key and other.commit_version > version:
                        graph.add(candidate.transaction_id, other.transaction_id)

            # O read the version T writes from => O before T.
            for write in candidate.writes:
                key = (write.resource, write.key)
                observed = other_reads.get(key)
                if observed is not None and observed == write.expected_version:
                    graph.add(other.transaction_id, candidate.transaction_id)

            # Write-write ordering follows commit order; candidate is committing now.
            if candidate_writes & other_write_keys:
                graph.add(other.transaction_id, candidate.transaction_id)

        return graph

    def check(self, candidate: TransactionFootprint) -> list[str] | None:
        """Return the cycle (if any) and remember the candidate when accepted."""

        self._checks += 1
        graph = self.build_graph(candidate)
        cycle = graph.find_cycle(candidate.transaction_id)
        if cycle is not None:
            self._cycles += 1
            return cycle
        self.record(candidate)
        return None

    def record(self, footprint: TransactionFootprint) -> None:
        self._recent.append(footprint)

    def edges(self) -> dict[str, list[str]]:
        graph = _Graph()
        footprints: Sequence[TransactionFootprint] = tuple(self._recent)
        for candidate in footprints:
            subgraph = self.build_graph(candidate)
            for source, targets in subgraph.edges.items():
                for target in targets:
                    graph.add(source, target)
        return graph.to_dict()

    def stats(self) -> Mapping[str, int]:
        return {"checks": self._checks, "cycles": self._cycles, "window": len(self._recent)}
