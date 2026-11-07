"""Durable-shape transaction records.

The same record type is written to the in-memory ring buffer, persisted by the
PostgreSQL backend and rendered by the playground. Records are immutable and
JSON-serialisable so they can be exported for benchmark analysis.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from statepact.core.interfaces import CommitPlan, CommitResult


class TransactionStatus(StrEnum):
    COMMITTED = "committed"
    READ_ONLY = "read_only"
    ABORTED = "aborted"
    FAILED = "failed"


class ReadEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource: str
    key: str
    version: int


class WriteEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource: str
    key: str
    value: Any = None
    expected_version: int
    op: str = "put"


class TransactionRecord(BaseModel):
    """A complete, self-contained account of one transaction attempt."""

    model_config = ConfigDict(extra="ignore")

    transaction_id: str
    agent_id: str
    isolation: str
    started_at: datetime
    finished_at: datetime
    status: TransactionStatus
    commit_version: int | None = None
    reads: list[ReadEntry] = Field(default_factory=list)
    writes: list[WriteEntry] = Field(default_factory=list)
    conflict_class: str | None = None
    abort_reason: str | None = None
    retry_attempt: int = 1
    duration_ms: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def committed(self) -> bool:
        return self.status in {TransactionStatus.COMMITTED, TransactionStatus.READ_ONLY}

    @classmethod
    def from_plan(
        cls,
        plan: CommitPlan,
        *,
        status: TransactionStatus,
        commit_version: int | None = None,
        finished_at: datetime | None = None,
        conflict_class: str | None = None,
        abort_reason: str | None = None,
        retry_attempt: int = 1,
    ) -> TransactionRecord:
        finished = finished_at or datetime.now(UTC)
        return cls(
            transaction_id=plan.transaction_id,
            agent_id=plan.agent_id,
            isolation=plan.isolation,
            started_at=plan.started_at,
            finished_at=finished,
            status=status,
            commit_version=commit_version,
            reads=[
                ReadEntry(resource=r.resource, key=r.key, version=r.version) for r in plan.reads
            ],
            writes=[
                WriteEntry(
                    resource=w.resource,
                    key=w.key,
                    value=w.value,
                    expected_version=w.expected_version,
                    op=w.op,
                )
                for w in plan.writes
            ],
            conflict_class=conflict_class,
            abort_reason=abort_reason,
            retry_attempt=retry_attempt,
            duration_ms=max(0.0, (finished - plan.started_at).total_seconds() * 1000.0),
            metadata=dict(plan.metadata),
        )

    @classmethod
    def from_result(
        cls,
        plan: CommitPlan,
        result: CommitResult,
        *,
        retry_attempt: int = 1,
    ) -> TransactionRecord:
        status = TransactionStatus.READ_ONLY if result.read_only else TransactionStatus.COMMITTED
        return cls.from_plan(
            plan,
            status=status,
            commit_version=result.commit_version,
            finished_at=result.committed_at,
            retry_attempt=retry_attempt,
        )


class TransactionLog:
    """Bounded, queryable in-memory transaction log.

    Backends may persist their own copy; this log is the observability view and
    is deliberately lossy under sustained load.
    """

    def __init__(self, *, max_records: int = 20_000) -> None:
        self._records: deque[TransactionRecord] = deque(maxlen=max_records)

    def append(self, record: TransactionRecord) -> None:
        self._records.append(record)

    def extend(self, records: Iterable[TransactionRecord]) -> None:
        self._records.extend(records)

    def recent(self, limit: int = 100) -> list[TransactionRecord]:
        if limit <= 0:
            return []
        return list(self._records)[-limit:]

    def committed(self, limit: int = 100) -> list[TransactionRecord]:
        return [record for record in self.recent(limit=max(limit * 4, 100)) if record.committed][
            -limit:
        ]

    def conflicts(self, limit: int = 100) -> list[TransactionRecord]:
        return [record for record in self._records if record.conflict_class is not None][-limit:]

    def for_agent(self, agent_id: str, limit: int = 100) -> list[TransactionRecord]:
        matches = [record for record in self._records if record.agent_id == agent_id]
        return matches[-limit:]

    def to_json(self, limit: int | None = None) -> str:
        records = list(self._records) if limit is None else self.recent(limit)
        return "[" + ",".join(record.model_dump_json() for record in records) + "]"

    def clear(self) -> None:
        self._records.clear()

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Iterable[TransactionRecord]:
        return iter(self._records)
