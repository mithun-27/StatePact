"""Compare-and-swap helper built on the transaction engine.

CAS is the smallest useful primitive for agents: "set this key to a new value only
if it is still at the version I observed". It is implemented as a one-operation
transaction so it inherits every backend's atomic validation; it is not a separate
code path that could silently diverge.
"""

from __future__ import annotations

from typing import Any

from agentcommit.core.exceptions import ConflictError
from agentcommit.core.interfaces import StorageBackend
from agentcommit.core.serde import normalize_value
from agentcommit.core.transaction import Transaction, new_transaction_id, utcnow
from agentcommit.core.version import UNVERSIONED, Version


async def compare_and_swap(
    backend: StorageBackend,
    resource: str,
    key: str,
    *,
    expected_version: Version,
    value: Any,
    agent_id: str = "cas",
) -> bool:
    """Atomically write ``value`` iff ``key`` is at ``expected_version``.

    ``expected_version=0`` means "only if the key does not exist" (insert).
    Returns ``True`` on success and ``False`` on a version conflict. Backend or
    configuration errors are never swallowed.
    """

    normalized = normalize_value(value)
    transaction = Transaction(
        id=new_transaction_id(),
        agent_id=agent_id,
        isolation="optimistic",
        resources=(resource,),
        started_at=utcnow(),
        fence=None,
    )
    transaction.stage_write(resource, key, normalized, expected_version)
    transaction.record_read(resource, key, expected_version)
    transaction.mark_committing()
    try:
        await backend.commit(transaction.to_plan())
    except ConflictError:
        transaction.mark_aborted("compare-and-swap conflict")
        return False
    return True


async def compare_and_delete(
    backend: StorageBackend,
    resource: str,
    key: str,
    *,
    expected_version: Version,
    agent_id: str = "cas",
) -> bool:
    """Delete ``key`` iff it is still at ``expected_version``."""

    transaction = Transaction(
        id=new_transaction_id(),
        agent_id=agent_id,
        isolation="optimistic",
        resources=(resource,),
        started_at=utcnow(),
        fence=None,
    )
    transaction.stage_write(resource, key, None, expected_version, op="delete")
    transaction.record_read(resource, key, expected_version)
    transaction.mark_committing()
    try:
        await backend.commit(transaction.to_plan())
    except ConflictError:
        transaction.mark_aborted("compare-and-delete conflict")
        return False
    return True


def is_absent(version: Version) -> bool:
    return version == UNVERSIONED
