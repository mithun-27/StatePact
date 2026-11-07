"""Conflict classification and detection.

The detector works on ``(resource, key) -> latest version`` maps supplied by the
backend *inside* its atomic commit section. Keeping it pure lets the same rules
be exercised by unit and property tests without a backend.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from agentcommit.core.exceptions import (
    ConflictClass,
    ConflictError,
    ReadWriteConflict,
    WriteWriteConflict,
)
from agentcommit.core.interfaces import CommitPlan, ReadRecord, WriteRecord
from agentcommit.core.version import Version


@dataclass(frozen=True, slots=True)
class Conflict:
    """A detected version conflict, before it is turned into an exception."""

    kind: ConflictClass
    transaction_id: str
    resource: str
    key: str
    expected_version: Version
    actual_version: Version
    description: str

    def to_exception(self) -> ConflictError:
        kwargs: dict[str, object] = {
            "transaction_id": self.transaction_id,
            "resource": self.resource,
            "key": self.key,
            "expected_version": self.expected_version,
            "actual_version": self.actual_version,
        }
        if self.kind == "read_write":
            return ReadWriteConflict(**kwargs)
        return WriteWriteConflict(**kwargs)


def check_read(record: ReadRecord, latest_version: Version, transaction_id: str) -> Conflict | None:
    if latest_version == record.version:
        return None
    return Conflict(
        kind="read_write",
        transaction_id=transaction_id,
        resource=record.resource,
        key=record.key,
        expected_version=record.version,
        actual_version=latest_version,
        description=(
            f"{record.resource}.{record.key} was read at version {record.version} "
            f"but is now at {latest_version}"
        ),
    )


def check_write(
    record: WriteRecord, latest_version: Version, transaction_id: str
) -> Conflict | None:
    if latest_version == record.expected_version:
        return None
    return Conflict(
        kind="write_write",
        transaction_id=transaction_id,
        resource=record.resource,
        key=record.key,
        expected_version=record.expected_version,
        actual_version=latest_version,
        description=(
            f"{record.resource}.{record.key} was staged on version "
            f"{record.expected_version} but is now at {latest_version}"
        ),
    )


def detect(
    plan: CommitPlan,
    latest: Mapping[tuple[str, str], Version],
    *,
    validate_reads: bool,
    validate_writes: bool,
) -> Conflict | None:
    """Return the first conflict in a stable (read order, then write order) scan."""

    if validate_reads:
        for read in plan.reads:
            actual = latest.get((read.resource, read.key), 0)
            conflict = check_read(read, actual, plan.transaction_id)
            if conflict is not None:
                return conflict
    if validate_writes:
        for write in plan.writes:
            actual = latest.get((write.resource, write.key), 0)
            conflict = check_write(write, actual, plan.transaction_id)
            if conflict is not None:
                return conflict
    return None


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    ok: bool
    conflict: Conflict | None = None

    def raise_if_conflict(self) -> None:
        if self.conflict is not None:
            raise self.conflict.to_exception()


def involved_keys(plan: CommitPlan) -> Sequence[tuple[str, str]]:
    keys = {(read.resource, read.key) for read in plan.reads}
    keys.update((write.resource, write.key) for write in plan.writes)
    return tuple(sorted(keys))
