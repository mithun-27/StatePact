"""Backend interface re-exported under the conventional ``storage.base`` path."""

from agentcommit.core.interfaces import (
    CommitPlan,
    CommitResult,
    Lease,
    LockMode,
    ResourceInfo,
    ResourceSnapshot,
    StorageBackend,
    StoredValue,
    WriteRecord,
)

BaseStorage = StorageBackend

__all__ = [
    "BaseStorage",
    "CommitPlan",
    "CommitResult",
    "Lease",
    "LockMode",
    "ResourceInfo",
    "ResourceSnapshot",
    "StorageBackend",
    "StoredValue",
    "WriteRecord",
]
