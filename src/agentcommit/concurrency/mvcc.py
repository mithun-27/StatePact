"""Multi-version concurrency control primitives.

The in-memory backend keeps append-only per-key histories of
``VersionEntry(version, value, deleted)``. Versions are global commit sequence
numbers, so a single integer fully describes a snapshot.

These helpers are pure functions, which makes them straightforward to test
independently from the storage backend.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from agentcommit.core.interfaces import StoredValue
from agentcommit.core.version import Version


@dataclass(frozen=True, slots=True)
class VersionEntry:
    """One committed state of a key. ``deleted`` entries are tombstones."""

    version: Version
    value: Any | None
    deleted: bool = False

    def visible_value(self) -> StoredValue | None:
        if self.deleted:
            return None
        return StoredValue(value=self.value, version=self.version)


def visible_entry(
    history: Sequence[VersionEntry], snapshot_version: Version
) -> VersionEntry | None:
    """Return the newest entry with ``version <= snapshot_version``.

    Histories are append-only and sorted ascending, so this is a binary search.
    """

    if not history:
        return None
    versions = [entry.version for entry in history]
    index = bisect_right(versions, snapshot_version) - 1
    if index < 0:
        return None
    return history[index]


def snapshot_values(
    histories: Mapping[str, Sequence[VersionEntry]], snapshot_version: Version
) -> dict[str, StoredValue]:
    """Materialise all visible, non-deleted values at ``snapshot_version``."""

    values: dict[str, StoredValue] = {}
    for key, history in histories.items():
        entry = visible_entry(history, snapshot_version)
        if entry is None or entry.deleted:
            continue
        values[key] = StoredValue(value=entry.value, version=entry.version)
    return values


def trim_history(history: Sequence[VersionEntry], watermark: Version) -> list[VersionEntry]:
    """Garbage-collect history while preserving snapshot correctness.

    Keeps every version newer than ``watermark`` plus the newest version at or
    below it (the version a snapshot at ``watermark`` must still see).
    """

    if not history:
        return []
    versions = [entry.version for entry in history]
    boundary = bisect_right(versions, watermark) - 1
    if boundary <= 0:
        return list(history)
    return list(history[boundary:])


def latest_entry(history: Sequence[VersionEntry]) -> VersionEntry | None:
    return history[-1] if history else None


def apply_entry(history: list[VersionEntry], entry: VersionEntry) -> None:
    """Append a version, enforcing the monotonic invariant."""

    if history and entry.version <= history[-1].version:
        raise ValueError(
            f"versions must increase monotonically: {entry.version} <= {history[-1].version}"
        )
    history.append(entry)


def entry_for(version: Version, value: Any | None, *, deleted: bool = False) -> VersionEntry:
    return VersionEntry(version=version, value=None if deleted else value, deleted=deleted)
