"""Optimistic concurrency control (backward validation).

Isolation semantics implemented here (and enforced by :mod:`agentcommit.storage.memory`):

``none``
    No validation at all. Used only as a benchmark baseline to demonstrate the
    failures AgentCommit prevents. Never use in production.

``optimistic``
    Snapshot reads at transaction start. At commit, **every read version** and
    **every write base version** is re-checked. This is backward OCC and is
    serializable with respect to tracked keys: a committed transaction can always
    be placed at its commit point in the serial order.

``snapshot``
    MVCC snapshot reads, but only **write base versions** are validated
    (first-committer-wins). This is snapshot isolation: no lost updates, but write
    skew remains possible. Documented in ``docs/limitations.md``.

``serializable``
    ``optimistic`` plus a serialization-graph audit over overlapping committed
    transactions. The audit is a safety net/diagnostic; with full read validation
    a cycle should be unreachable for tracked keys.

``pessimistic``
    Validation is still performed defensively, but correctness comes from the
    two-phase locks held until commit.
"""

from __future__ import annotations

from collections.abc import Mapping

from agentcommit.concurrency.conflict import ValidationOutcome, detect
from agentcommit.core.interfaces import CommitPlan, IsolationMode
from agentcommit.core.version import Version


def validate(
    plan: CommitPlan,
    latest: Mapping[tuple[str, str], Version],
) -> ValidationOutcome:
    isolation: IsolationMode = plan.isolation
    if isolation == "none":
        return ValidationOutcome(ok=True)
    if isolation == "snapshot":
        conflict = detect(plan, latest, validate_reads=False, validate_writes=True)
    elif isolation in {"optimistic", "serializable", "pessimistic"}:
        conflict = detect(plan, latest, validate_reads=True, validate_writes=True)
    else:  # pragma: no cover - guarded at construction
        raise ValueError(f"unknown isolation mode: {isolation!r}")
    return ValidationOutcome(ok=conflict is None, conflict=conflict)


def validate_reads(
    plan: CommitPlan, latest: Mapping[tuple[str, str], Version]
) -> ValidationOutcome:
    conflict = detect(plan, latest, validate_reads=True, validate_writes=False)
    return ValidationOutcome(ok=conflict is None, conflict=conflict)


def validate_writes(
    plan: CommitPlan, latest: Mapping[tuple[str, str], Version]
) -> ValidationOutcome:
    conflict = detect(plan, latest, validate_reads=False, validate_writes=True)
    return ValidationOutcome(ok=conflict is None, conflict=conflict)
