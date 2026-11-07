"""Transaction state and commit-plan construction."""

from __future__ import annotations

import secrets
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from agentcommit.core.interfaces import (
    ABSENT,
    CommitPlan,
    CommitResult,
    IsolationMode,
    ReadRecord,
    ResourceSnapshot,
    WriteOp,
    WriteRecord,
)
from agentcommit.core.serde import stable_hash
from agentcommit.core.version import Version


class TransactionState(StrEnum):
    """Lifecycle states. Only ``ACTIVE`` transactions accept reads/writes."""

    ACTIVE = "active"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTED = "aborted"
    FAILED = "failed"


def new_transaction_id() -> str:
    """Time-ordered, URL-safe transaction id (millisecond prefix + random suffix)."""

    millis = int(time.time() * 1000)
    return f"tx_{millis:013x}{secrets.token_hex(6)}"


@dataclass(slots=True)
class Transaction:
    """Mutable bookkeeping for one transaction attempt.

    Instances are internal. Public code interacts with :class:`AgentSession`.
    """

    id: str
    agent_id: str
    isolation: IsolationMode
    resources: tuple[str, ...]
    started_at: datetime
    state: TransactionState = TransactionState.ACTIVE
    read_set: dict[tuple[str, str], Version] = field(default_factory=dict)
    write_set: dict[tuple[str, str], WriteRecord] = field(default_factory=dict)
    snapshots: dict[str, ResourceSnapshot] = field(default_factory=dict)
    fence: int | None = None
    idempotency_key: str | None = None
    #: Optional caller-supplied fingerprint describing the *logical* intent of an
    #: idempotent operation. Defaults to the write footprint (resource/key/op),
    #: which deliberately excludes values so a retried read-modify-write is
    #: recognised as the same operation instead of a payload mismatch.
    idempotency_fingerprint: str | None = None
    attempt: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
    abort_reason: str | None = None
    commit_result: CommitResult | None = None

    # -- read/write bookkeeping ------------------------------------------------

    def record_read(self, resource: str, key: str, version: Version) -> None:
        """Record the *first* version observed for a key.

        Repeat reads inside a transaction must see the same snapshot, so the
        original version is deliberately preserved.
        """

        self.read_set.setdefault((resource, key), version)

    def read_version(self, resource: str, key: str) -> Version | None:
        return self.read_set.get((resource, key))

    def stage_write(
        self,
        resource: str,
        key: str,
        value: Any,
        expected_version: Version,
        *,
        op: WriteOp = "put",
    ) -> None:
        self.write_set[(resource, key)] = WriteRecord(
            resource=resource,
            key=key,
            value=value,
            expected_version=expected_version,
            op=op,
        )

    def staged(self, resource: str, key: str) -> WriteRecord | None:
        return self.write_set.get((resource, key))

    def unstage(self, resource: str, key: str) -> None:
        self.write_set.pop((resource, key), None)

    @property
    def has_writes(self) -> bool:
        return bool(self.write_set)

    @property
    def is_active(self) -> bool:
        return self.state is TransactionState.ACTIVE

    # -- plan ----------------------------------------------------------------

    def base_version(self, resource: str, key: str) -> Version:
        snapshot = self.snapshots.get(resource)
        if snapshot is not None:
            return snapshot.version_of(key)
        return ABSENT

    def to_plan(self) -> CommitPlan:
        reads = tuple(
            ReadRecord(resource=resource, key=key, version=version)
            for (resource, key), version in sorted(self.read_set.items())
        )
        writes = tuple(self.write_set[key] for key in sorted(self.write_set))
        return CommitPlan(
            transaction_id=self.id,
            agent_id=self.agent_id,
            isolation=self.isolation,
            reads=reads,
            writes=writes,
            started_at=self.started_at,
            fence=self.fence,
            idempotency_key=self.idempotency_key,
            request_hash=self.request_hash(),
            metadata=dict(self.metadata),
        )

    def request_hash(self) -> str | None:
        """Stable fingerprint of the operation's logical intent.

        Used only to detect idempotency-key reuse with a *different* request.
        Values are deliberately excluded from the default fingerprint: an agent
        that retries a read-modify-write after an ambiguous transport failure
        will legitimately compute a different new value, but it is still the
        same operation and must replay rather than execute twice. Callers that
        need strict payload binding can pass ``idempotency_fingerprint``.
        """

        if self.idempotency_key is None:
            return None
        if self.idempotency_fingerprint is not None:
            payload: dict[str, Any] = {
                "agent_id": self.agent_id,
                "fingerprint": self.idempotency_fingerprint,
            }
        else:
            payload = {
                "agent_id": self.agent_id,
                "writes": [
                    {
                        "resource": record.resource,
                        "key": record.key,
                        "op": record.op,
                    }
                    for record in (self.write_set[key] for key in sorted(self.write_set))
                ],
            }
        return stable_hash(payload)

    # -- transitions ---------------------------------------------------------

    def mark_committing(self) -> None:
        self.state = TransactionState.COMMITTING

    def mark_committed(self, result: CommitResult) -> None:
        self.state = TransactionState.COMMITTED
        self.commit_result = result

    def mark_aborted(self, reason: str) -> None:
        if self.state is not TransactionState.COMMITTED:
            self.state = TransactionState.ABORTED
            self.abort_reason = reason

    def mark_failed(self, reason: str) -> None:
        if self.state is not TransactionState.COMMITTED:
            self.state = TransactionState.FAILED
            self.abort_reason = reason

    def to_record(self) -> dict[str, Any]:
        """Serialisable snapshot used by logs, the demo and tests."""

        return {
            "transaction_id": self.id,
            "agent_id": self.agent_id,
            "isolation": self.isolation,
            "state": self.state.value,
            "started_at": self.started_at.isoformat(),
            "read_set": [
                {"resource": r, "key": k, "version": v}
                for (r, k), v in sorted(self.read_set.items())
            ],
            "write_set": [
                {
                    "resource": record.resource,
                    "key": record.key,
                    "value": record.value,
                    "expected_version": record.expected_version,
                    "op": record.op,
                }
                for record in self.write_set.values()
            ],
            "abort_reason": self.abort_reason,
        }


def utcnow() -> datetime:
    return datetime.now(UTC)


def elapsed_seconds(started_at: datetime) -> float:
    return (datetime.now(UTC) - started_at).total_seconds()


def snapshot_summary(snapshots: Mapping[str, ResourceSnapshot]) -> dict[str, int]:
    return {name: snapshot.taken_at for name, snapshot in snapshots.items()}
